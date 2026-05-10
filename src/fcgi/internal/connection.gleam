import exception
import fcgi/internal/handler
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/dict.{type Dict}
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/result
import gleam/string

const file_chunk_size = 65_535

const max_params_size = 65_536

const init_recv_timeout_ms = 60_000

const fcgi_version = 1

const fcgi_stdout_type = 6

pub type Socket

pub type Handle

pub type SocketError {
  PathExists(path: String)
  Posix(reason: Atom)
}

pub type FileError {
  NotFound
  AccessDenied
  IsDirectory
  Unknown(reason: String)
}

@external(erlang, "fcgi_ffi", "accept")
pub fn accept(listen: Socket) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "socket_close")
pub fn close_socket(socket: Socket) -> Nil

@external(erlang, "fcgi_ffi", "controlling_process")
pub fn controlling_process(
  socket: Socket,
  pid: process.Pid,
) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "delete_path")
pub fn delete_path(path: String) -> Nil

@external(erlang, "fcgi_ffi", "listen")
pub fn listen(path: String) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "recv")
pub fn recv(
  socket: Socket,
  size: Int,
  timeout_ms: Int,
) -> Result(BitArray, SocketError)

@external(erlang, "fcgi_ffi", "send")
pub fn send(socket: Socket, data: BitArray) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "send")
fn send_tree(socket: Socket, data: BytesTree) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "close_file")
pub fn close_file(handle: Handle) -> Nil

@external(erlang, "fcgi_ffi", "open_and_size")
pub fn open_and_size(path: String) -> Result(#(Handle, Int), FileError)

@external(erlang, "fcgi_ffi", "sendfile")
fn sendfile(
  handle: Handle,
  socket: Socket,
  offset: Int,
  bytes: Int,
) -> Result(Int, FileError)

pub type ResponseData {
  Bytes(content: BytesTree)
  File(handle: Handle, offset: Int, length: Int)
  Stream(producer: fn(StreamSender) -> Nil)
}

pub type Spec {
  Spec(
    socket: Socket,
    max_body_size: Int,
    body_read_timeout_ms: Int,
    handler: fn(Request(Nil), BodyReader) -> Response(ResponseData),
  )
}

pub opaque type StreamSender {
  StreamSender(emit: fn(BitArray) -> Result(Nil, Nil))
}

pub type BodyReader =
  fn() -> Result(BodyRead, BodyReadError)

pub type BodyRead {
  BodyMore(data: BitArray, next: BodyReader)
  BodyEnded
}

pub type BodyReadError {
  ConnectionLost
  Timeout
  TooLarge
}

type BodyContext {
  BodyContext(
    socket: Socket,
    state: handler.State,
    pending: BitArray,
    finished: Bool,
    overflowed: Bool,
    max_body_size: Int,
    timeout_ms: Int,
    tracker: process.Subject(StateSnapshot),
  )
}

type StateSnapshot {
  StateSnapshot(state: handler.State, finished: Bool, overflowed: Bool)
}

pub fn send_chunk(sender: StreamSender, data: BitArray) -> Result(Nil, Nil) {
  let StreamSender(emit) = sender
  emit(data)
}

pub fn start(spec: Spec) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(self_subject) {
    process.send(self_subject, Nil)
    let selector = process.new_selector() |> process.select(self_subject)
    actor.initialised(spec)
    |> actor.selecting(selector)
    |> actor.returning(Nil)
    |> Ok
  })
  |> actor.on_message(fn(spec, _msg) {
    run_connection(spec)
    actor.stop()
  })
  |> actor.start
}

fn run_connection(spec: Spec) -> Nil {
  run_connection_loop(spec, handler.new(), <<>>)
  close_socket(spec.socket)
}

fn run_connection_loop(
  spec: Spec,
  state: handler.State,
  pending: BitArray,
) -> Nil {
  let tracker = process.new_subject()
  case feed_until_request_or_end(spec, state, pending) {
    FeedReachedEnd -> Nil
    FeedReachedRequest(fsm_state, request_id, params, body_queue, keep_conn) -> {
      let send_result =
        handle_one_request(
          spec,
          tracker,
          fsm_state,
          request_id,
          params,
          body_queue,
        )
      case send_result, keep_conn {
        Error(_), _ -> Nil
        Ok(_), False -> Nil
        Ok(_), True ->
          case finalize_request(spec, tracker) {
            Error(_) -> Nil
            Ok(next_state) -> run_connection_loop(spec, next_state, <<>>)
          }
      }
    }
  }
}

type FeedOutcome {
  FeedReachedRequest(
    fsm_state: handler.State,
    request_id: Int,
    params: BitArray,
    body_queue: List(handler.Event),
    keep_conn: Bool,
  )
  FeedReachedEnd
}

fn feed_until_request_or_end(
  spec: Spec,
  fsm_state: handler.State,
  pending: BitArray,
) -> FeedOutcome {
  let outcome =
    handler.feed(
      fsm_state,
      bytes: pending,
      max_body_size: spec.max_body_size,
      max_params_size:,
    )
  let _ = send_if_nonempty(spec.socket, outcome.outgoing)
  case extract_start(outcome.events) {
    option.Some(#(request_id, params, keep_conn, queue)) ->
      FeedReachedRequest(
        fsm_state: outcome.state,
        request_id:,
        params:,
        body_queue: queue,
        keep_conn:,
      )
    option.None ->
      case outcome.continuation {
        handler.CloseConnection -> FeedReachedEnd
        handler.WaitForMore ->
          case recv(spec.socket, 0, init_recv_timeout_ms) {
            Error(_) -> FeedReachedEnd
            Ok(<<>>) -> FeedReachedEnd
            Ok(more) -> feed_until_request_or_end(spec, outcome.state, more)
          }
      }
  }
}

fn extract_start(
  events: List(handler.Event),
) -> Option(#(Int, BitArray, Bool, List(handler.Event))) {
  case events {
    [handler.Start(request_id, params, keep_conn), ..rest] ->
      option.Some(#(request_id, params, keep_conn, rest))
    [_, ..rest] -> extract_start(rest)
    [] -> option.None
  }
}

fn handle_one_request(
  spec: Spec,
  tracker: process.Subject(StateSnapshot),
  fsm_state: handler.State,
  request_id: Int,
  params: BitArray,
  body_queue: List(handler.Event),
) -> Result(Nil, Nil) {
  use <- bool.lazy_guard(
    when: list.contains(body_queue, handler.BodyTooLarge),
    return: fn() { send_overloaded(spec.socket, request_id) },
  )
  case build_request(params) {
    Error(message) -> {
      let response = error_response(400, message)
      send_response(spec.socket, request_id, response)
    }
    Ok(req) -> {
      let reader = make_initial_reader(spec, tracker, fsm_state, body_queue)
      let response = run_user_handler(spec.handler, req, reader)
      send_response(spec.socket, request_id, response)
    }
  }
}

fn send_overloaded(socket: Socket, request_id: Int) -> Result(Nil, Nil) {
  send_if_nonempty(socket, handler.encode_overloaded_end(request_id))
  |> result.replace_error(Nil)
}

fn finalize_request(
  spec: Spec,
  tracker: process.Subject(StateSnapshot),
) -> Result(handler.State, Nil) {
  use initial <- result.try(
    process.receive(tracker, 0)
    |> result.replace_error(Nil),
  )
  let final = drain_tracker_loop(tracker, initial)
  drain_body_loop(spec, final)
}

fn drain_tracker_loop(
  tracker: process.Subject(StateSnapshot),
  latest: StateSnapshot,
) -> StateSnapshot {
  case process.receive(tracker, 0) {
    Error(_) -> latest
    Ok(snap) -> drain_tracker_loop(tracker, snap)
  }
}

fn drain_body_loop(
  spec: Spec,
  snap: StateSnapshot,
) -> Result(handler.State, Nil) {
  use <- bool.guard(when: snap.overflowed, return: Error(Nil))
  use <- bool.guard(when: snap.finished, return: Ok(snap.state))
  case recv(spec.socket, 0, spec.body_read_timeout_ms) {
    Error(_) -> Error(Nil)
    Ok(<<>>) -> Error(Nil)
    Ok(more) -> {
      let outcome =
        handler.feed(
          snap.state,
          bytes: more,
          max_body_size: spec.max_body_size,
          max_params_size:,
        )
      let _ = send_if_nonempty(spec.socket, outcome.outgoing)
      let #(_data, ended, overflowed) =
        collect_body_events(outcome.events, <<>>, False, False)
      let next =
        StateSnapshot(
          state: outcome.state,
          finished: snap.finished || ended,
          overflowed: snap.overflowed || overflowed,
        )
      drain_body_loop(spec, next)
    }
  }
}

fn run_user_handler(
  handler_fn: fn(Request(Nil), BodyReader) -> Response(ResponseData),
  req: Request(Nil),
  reader: BodyReader,
) -> Response(ResponseData) {
  case exception.rescue(fn() { handler_fn(req, reader) }) {
    Ok(resp) -> resp
    Error(_) -> error_response(500, "internal server error")
  }
}

fn make_initial_reader(
  spec: Spec,
  tracker: process.Subject(StateSnapshot),
  state: handler.State,
  body_queue: List(handler.Event),
) -> BodyReader {
  let #(initial_data, initial_done, initial_overflow) =
    collect_body_events(body_queue, <<>>, False, False)
  let ctx =
    BodyContext(
      socket: spec.socket,
      state:,
      pending: initial_data,
      finished: initial_done,
      overflowed: initial_overflow,
      max_body_size: spec.max_body_size,
      timeout_ms: spec.body_read_timeout_ms,
      tracker:,
    )
  put_snapshot(ctx)
  reader_for(ctx)
}

fn put_snapshot(ctx: BodyContext) -> Nil {
  process.send(
    ctx.tracker,
    StateSnapshot(
      state: ctx.state,
      finished: ctx.finished,
      overflowed: ctx.overflowed,
    ),
  )
}

fn reader_for(ctx: BodyContext) -> BodyReader {
  fn() { read_step(ctx) }
}

fn read_step(ctx: BodyContext) -> Result(BodyRead, BodyReadError) {
  use <- bool.guard(when: ctx.overflowed, return: Error(TooLarge))
  let pending_size = bit_array.byte_size(ctx.pending)
  case pending_size, ctx.finished {
    0, True -> Ok(BodyEnded)
    0, False -> pull_more(ctx)
    _, _ -> deliver_pending(ctx)
  }
}

fn deliver_pending(ctx: BodyContext) -> Result(BodyRead, BodyReadError) {
  let next_ctx = BodyContext(..ctx, pending: <<>>)
  put_snapshot(next_ctx)
  Ok(BodyMore(data: ctx.pending, next: reader_for(next_ctx)))
}

fn pull_more(ctx: BodyContext) -> Result(BodyRead, BodyReadError) {
  case recv(ctx.socket, 0, ctx.timeout_ms) {
    Error(error) -> Error(classify_recv_error(error))
    Ok(<<>>) -> Error(ConnectionLost)
    Ok(more) -> {
      let outcome =
        handler.feed(
          ctx.state,
          bytes: more,
          max_body_size: ctx.max_body_size,
          max_params_size:,
        )
      let _ = send_if_nonempty(ctx.socket, outcome.outgoing)
      let #(new_data, ended, overflowed) =
        collect_body_events(outcome.events, <<>>, False, False)
      let next_ctx =
        BodyContext(
          ..ctx,
          state: outcome.state,
          pending: <<ctx.pending:bits, new_data:bits>>,
          finished: ctx.finished || ended,
          overflowed: ctx.overflowed || overflowed,
        )
      put_snapshot(next_ctx)
      read_step(next_ctx)
    }
  }
}

fn classify_recv_error(error: SocketError) -> BodyReadError {
  case error {
    Posix(reason) ->
      case atom.to_string(reason) {
        "timeout" -> Timeout
        _ -> ConnectionLost
      }
    _ -> ConnectionLost
  }
}

fn collect_body_events(
  events: List(handler.Event),
  data: BitArray,
  ended: Bool,
  overflowed: Bool,
) -> #(BitArray, Bool, Bool) {
  case events {
    [] -> #(data, ended, overflowed)
    [handler.BodyChunk(chunk), ..rest] ->
      collect_body_events(rest, <<data:bits, chunk:bits>>, ended, overflowed)
    [handler.BodyEnd, ..rest] ->
      collect_body_events(rest, data, True, overflowed)
    [handler.BodyTooLarge, ..rest] ->
      collect_body_events(rest, data, True, True)
    [handler.Start(_, _, _), ..rest] ->
      collect_body_events(rest, data, ended, overflowed)
  }
}

fn send_response(
  socket: Socket,
  request_id: Int,
  response: Response(ResponseData),
) -> Result(Nil, Nil) {
  let header = handler.encode_response_header(request_id, response)
  let terminator = handler.encode_response_terminator(request_id)
  case response.body {
    Bytes(data) -> {
      let body = handler.encode_stdout_chunk(request_id, data)
      let combined =
        header
        |> bytes_tree.append_tree(body)
        |> bytes_tree.append_tree(terminator)
      send_if_nonempty(socket, combined)
      |> result.replace_error(Nil)
    }
    File(handle, offset, length) -> {
      use _ <- result.try(
        send_tree(socket, header)
        |> result.replace_error(Nil),
      )
      use _ <- result.try(send_file_body(
        socket,
        request_id,
        handle,
        offset,
        length,
      ))
      send_tree(socket, terminator)
      |> result.replace_error(Nil)
    }
    Stream(producer) -> {
      use _ <- result.try(
        send_tree(socket, header)
        |> result.replace_error(Nil),
      )
      let sender = stream_sender(socket, request_id)
      let _ = exception.rescue(fn() { producer(sender) })
      send_tree(socket, terminator)
      |> result.replace_error(Nil)
    }
  }
}

fn send_file_body(
  socket: Socket,
  request_id: Int,
  handle: Handle,
  offset: Int,
  length: Int,
) -> Result(Nil, Nil) {
  use <- exception.defer(fn() { close_file(handle) })
  send_via_sendfile(socket, request_id, handle, offset, length)
}

fn stream_sender(socket: Socket, request_id: Int) -> StreamSender {
  StreamSender(emit: fn(data) {
    send_if_nonempty(
      socket,
      handler.encode_stdout_chunk(request_id, bytes_tree.from_bit_array(data)),
    )
    |> result.replace_error(Nil)
  })
}

fn error_response(status: Int, message: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(Bytes(bytes_tree.from_string(message)))
}

fn send_via_sendfile(
  socket: Socket,
  request_id: Int,
  handle: Handle,
  offset: Int,
  remaining: Int,
) -> Result(Nil, Nil) {
  use <- bool.guard(when: remaining <= 0, return: Ok(Nil))

  let chunk_size = int.min(remaining, file_chunk_size)
  let padding_length = protocol.padding_for(chunk_size)
  let header = encode_stdout_header(request_id, chunk_size, padding_length)
  use _ <- result.try(
    send(socket, header)
    |> result.replace_error(Nil),
  )
  use _ <- result.try(drain_sendfile_loop(socket, handle, offset, chunk_size))
  use _ <- result.try(
    send_padding(socket, padding_length)
    |> result.replace_error(Nil),
  )
  send_via_sendfile(
    socket,
    request_id,
    handle,
    offset + chunk_size,
    remaining - chunk_size,
  )
}

fn drain_sendfile_loop(
  socket: Socket,
  handle: Handle,
  offset: Int,
  remaining: Int,
) -> Result(Nil, Nil) {
  use <- bool.guard(when: remaining <= 0, return: Ok(Nil))
  case sendfile(handle, socket, offset, remaining) {
    Error(_) -> Error(Nil)
    Ok(0) -> Error(Nil)
    Ok(sent) ->
      drain_sendfile_loop(socket, handle, offset + sent, remaining - sent)
  }
}

fn encode_stdout_header(
  request_id: Int,
  content_length: Int,
  padding_length: Int,
) -> BitArray {
  <<
    fcgi_version:size(8),
    fcgi_stdout_type:size(8),
    request_id:size(16),
    content_length:size(16),
    padding_length:size(8),
    0:size(8),
  >>
}

fn send_padding(
  socket: Socket,
  padding_length: Int,
) -> Result(Nil, SocketError) {
  use <- bool.guard(when: padding_length == 0, return: Ok(Nil))
  send(socket, <<0:size({ padding_length * 8 })>>)
}

fn build_request(params_bytes: BitArray) -> Result(Request(Nil), String) {
  use pairs <- result.try(
    protocol.parse_name_value_pairs(params_bytes)
    |> result.replace_error("malformed FastCGI parameters"),
  )
  let env = dict.from_list(pairs)
  use _ <- result.try(check_content_length(env))
  to_http_request(env, Nil)
  |> result.map_error(request_error_message)
}

pub type RequestError {
  MissingMethod
  InvalidMethod(method: String)
}

pub fn to_http_request(
  env: Dict(String, String),
  body: body,
) -> Result(Request(body), RequestError) {
  use method_str <- result.try(
    dict.get(env, "REQUEST_METHOD")
    |> result.replace_error(MissingMethod),
  )
  use method <- result.try(
    http.parse_method(method_str)
    |> result.replace_error(InvalidMethod(method_str)),
  )

  let scheme = case dict.get(env, "HTTPS") {
    Ok(value) -> https_scheme_from_value(value)
    Error(_) -> http.Http
  }

  let server_name = result.unwrap(dict.get(env, "SERVER_NAME"), "localhost")
  let server_port =
    dict.get(env, "SERVER_PORT")
    |> result.try(int.parse)
    |> option.from_result
  let #(host, port) = case dict.get(env, "HTTP_HOST") {
    Ok(raw) -> resolve_http_host(raw, server_name, server_port)
    Error(_) -> #(server_name, server_port)
  }
  let path = result.unwrap(dict.get(env, "PATH_INFO"), "/")
  let query =
    dict.get(env, "QUERY_STRING")
    |> option.from_result
  let headers =
    env
    |> dict.to_list
    |> list.filter_map(map_header)

  Ok(request.Request(
    method:,
    headers:,
    body:,
    scheme:,
    host:,
    port:,
    path:,
    query:,
  ))
}

fn https_scheme_from_value(value: String) -> http.Scheme {
  case string.lowercase(value) {
    "" | "off" -> http.Http
    _ -> http.Https
  }
}

fn resolve_http_host(
  raw: String,
  server_name: String,
  server_port: Option(Int),
) -> #(String, Option(Int)) {
  use <- bool.guard(when: raw == "", return: #(server_name, server_port))
  case string.starts_with(raw, "[") {
    True -> resolve_bracketed_host(raw, server_port)
    False -> resolve_unbracketed_host(raw, server_port)
  }
}

fn resolve_bracketed_host(
  raw: String,
  server_port: Option(Int),
) -> #(String, Option(Int)) {
  case string.split_once(string.drop_start(raw, 1), "]") {
    Ok(#(host, "")) if host != "" -> #(host, server_port)
    Ok(#(host, ":" <> port_str)) if host != "" -> {
      let port = int.parse(port_str) |> option.from_result
      #(host, option.or(port, server_port))
    }
    _ -> #(raw, server_port)
  }
}

fn resolve_unbracketed_host(
  raw: String,
  server_port: Option(Int),
) -> #(String, Option(Int)) {
  case string.split_once(raw, ":") {
    Ok(#(host, port_str)) if host != "" -> {
      let port = int.parse(port_str) |> option.from_result
      #(host, option.or(port, server_port))
    }
    _ -> #(raw, server_port)
  }
}

fn map_header(pair: #(String, String)) -> Result(#(String, String), Nil) {
  let #(key, value) = pair
  case key {
    "CONTENT_TYPE" -> Ok(#("content-type", value))
    "CONTENT_LENGTH" -> Ok(#("content-length", value))
    "HTTP_" <> name ->
      Ok(#(string.replace(string.lowercase(name), "_", "-"), value))
    _ -> Error(Nil)
  }
}

fn check_content_length(env: Dict(String, String)) -> Result(Nil, String) {
  case dict.get(env, "CONTENT_LENGTH") {
    Error(_) -> Ok(Nil)
    Ok("") -> Ok(Nil)
    Ok(raw) ->
      case int.parse(raw) {
        Error(_) -> Error("invalid CONTENT_LENGTH: " <> raw)
        Ok(declared) if declared < 0 -> Error("invalid CONTENT_LENGTH: " <> raw)
        Ok(_) -> Ok(Nil)
      }
  }
}

fn request_error_message(error: RequestError) -> String {
  case error {
    MissingMethod -> "missing REQUEST_METHOD parameter"
    InvalidMethod(method) -> "invalid REQUEST_METHOD: " <> method
  }
}

fn send_if_nonempty(
  socket: Socket,
  bytes: BytesTree,
) -> Result(Nil, SocketError) {
  case bytes_tree.byte_size(bytes) {
    0 -> Ok(Nil)
    _ -> send_tree(socket, bytes)
  }
}
