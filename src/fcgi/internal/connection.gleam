import exception
import fcgi/internal/handler
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/atom.{type Atom}
import gleam/erlang/process.{type Pid}
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/result
import gleam/string

const file_chunk_size: Int = 65_535

const max_params_size: Int = 262_144

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
pub fn controlling_process(socket: Socket, pid: Pid) -> Result(Nil, SocketError)

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

@external(erlang, "fcgi_ffi", "setopts_active_once")
pub fn setopts_active_once(socket: Socket) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "close")
pub fn close_file(handle: Handle) -> Nil

@external(erlang, "fcgi_ffi", "open")
pub fn open_file(path: String) -> Result(Handle, FileError)

@external(erlang, "fcgi_ffi", "pread")
fn pread(
  handle: Handle,
  offset offset: Int,
  size size: Int,
) -> Result(BitArray, FileError)

@external(erlang, "fcgi_ffi", "validate_file")
pub fn validate_file(path: String) -> Result(Nil, FileError)

pub type Connection {
  Connection(body: BitArray)
}

type Message {
  Tcp(data: BitArray)
  TcpClosed
  TcpError(reason: String)
}

pub type ResponseData {
  Bytes(content: BytesTree)
  File(path: String, offset: Int, limit: Option(Int))
  Stream(producer: fn(StreamSender) -> Nil)
}

pub type Spec {
  Spec(
    socket: Socket,
    max_body_size: Int,
    handler: fn(Request(Connection)) -> Response(ResponseData),
  )
}

pub opaque type StreamSender {
  StreamSender(emit: fn(BitArray) -> Result(Nil, Nil))
}

type PreparedResponseBody {
  PreparedBytes(data: BytesTree)
  PreparedStream(producer: fn(StreamSender) -> Nil)
}

type State {
  State(
    socket: Socket,
    handler_state: handler.State,
    max_body_size: Int,
    handler: fn(Request(Connection)) -> Response(ResponseData),
  )
}

pub fn send_chunk(sender: StreamSender, data: BitArray) -> Result(Nil, Nil) {
  let StreamSender(emit) = sender
  emit(data)
}

pub fn start(spec: Spec) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(_subject) {
    let initial_state =
      State(
        socket: spec.socket,
        handler_state: handler.new(),
        max_body_size: spec.max_body_size,
        handler: spec.handler,
      )
    let selector = build_selector()
    actor.initialised(initial_state)
    |> actor.selecting(selector)
    |> actor.returning(Nil)
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn build_selector() -> process.Selector(Message) {
  process.new_selector()
  |> process.select_record(atom.create("tcp"), 2, decode_tcp)
  |> process.select_record(atom.create("tcp_closed"), 1, fn(_) { TcpClosed })
  |> process.select_record(atom.create("tcp_error"), 2, decode_tcp_error)
}

fn decode_tcp(message: Dynamic) -> Message {
  case decode.run(message, decode.at([2], decode.bit_array)) {
    Ok(data) -> Tcp(data)
    Error(_) -> TcpError("malformed tcp message")
  }
}

fn decode_tcp_error(message: Dynamic) -> Message {
  TcpError(string.inspect(message))
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Tcp(data) -> handle_tcp(state, data)
    _ -> {
      close_socket(state.socket)
      actor.stop()
    }
  }
}

fn handle_tcp(state: State, data: BitArray) -> actor.Next(State, Message) {
  let outcome =
    handler.feed(
      state.handler_state,
      bytes: data,
      max_body_size: state.max_body_size,
      max_params_size:,
    )
  let _ = send_if_nonempty(state.socket, outcome.outgoing)
  drive_outcome(state, outcome)
}

fn drive_outcome(
  state: State,
  outcome: handler.Outcome,
) -> actor.Next(State, Message) {
  case outcome.action {
    handler.WaitForMore -> {
      let _ = setopts_active_once(state.socket)
      actor.continue(State(..state, handler_state: outcome.state))
    }
    handler.CloseConnection -> {
      close_socket(state.socket)
      actor.stop()
    }
    handler.ReadyForHandler(request_id, params_bytes, body, keep_conn) -> {
      run_handler(state, request_id, params_bytes, body)
      pump_after_handler_loop(
        State(..state, handler_state: outcome.state),
        keep_conn,
      )
    }
  }
}

fn pump_after_handler_loop(
  state: State,
  keep_conn: Bool,
) -> actor.Next(State, Message) {
  case keep_conn {
    False -> {
      close_socket(state.socket)
      actor.stop()
    }
    True -> {
      let outcome =
        handler.feed(
          state.handler_state,
          bytes: <<>>,
          max_body_size: state.max_body_size,
          max_params_size:,
        )
      let _ = send_if_nonempty(state.socket, outcome.outgoing)
      drive_outcome(state, outcome)
    }
  }
}

fn run_handler(
  state: State,
  request_id: Int,
  params_bytes: BitArray,
  body: BitArray,
) -> Nil {
  let response = case build_request(params_bytes, body) {
    Ok(req) -> safe_invoke_handler(state.handler, req)
    Error(message) -> error_response(400, message)
  }
  let #(final_response, prepared) = prepare_or_fallback(response)
  let _ =
    send_if_nonempty(
      state.socket,
      handler.encode_response_header(request_id, final_response),
    )
  send_prepared_body(state.socket, request_id, prepared)
  let _ =
    send_if_nonempty(
      state.socket,
      handler.encode_response_terminator(request_id),
    )
  Nil
}

fn safe_invoke_handler(
  handler_fn: fn(Request(Connection)) -> Response(ResponseData),
  req: Request(Connection),
) -> Response(ResponseData) {
  case exception.rescue(fn() { handler_fn(req) }) {
    Ok(resp) -> resp
    Error(_) -> error_response(500, "internal server error")
  }
}

fn prepare_or_fallback(
  response: Response(ResponseData),
) -> #(Response(ResponseData), PreparedResponseBody) {
  case prepare_response_body(response.body) {
    Ok(prepared) -> #(response, prepared)
    Error(_) -> {
      let message = "could not open response file"
      let fallback = error_response(500, message)
      #(fallback, PreparedBytes(bytes_tree.from_string(message)))
    }
  }
}

fn error_response(status: Int, message: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(Bytes(bytes_tree.from_string(message)))
}

fn prepare_response_body(
  body: ResponseData,
) -> Result(PreparedResponseBody, FileError) {
  case body {
    Bytes(tree) -> Ok(PreparedBytes(tree))
    File(path, offset, limit) ->
      open_file(path)
      |> result.map(fn(handle) {
        PreparedStream(fn(sender) {
          let StreamSender(emit) = sender
          stream_file(handle, offset:, remaining: limit, emit:)
          close_file(handle)
        })
      })
    Stream(producer) -> Ok(PreparedStream(producer))
  }
}

pub fn stream_file(
  handle: Handle,
  offset offset: Int,
  remaining remaining: Option(Int),
  emit emit: fn(BitArray) -> Result(Nil, Nil),
) -> Nil {
  let to_read = case remaining {
    option.None -> file_chunk_size
    option.Some(n) -> int.min(n, file_chunk_size)
  }
  use <- bool.guard(when: to_read <= 0, return: Nil)
  case pread(handle, offset:, size: to_read) {
    Error(_) -> Nil
    Ok(data) -> {
      let bytes_read = bit_array.byte_size(data)
      use <- bool.guard(when: bytes_read == 0, return: Nil)
      case emit(data) {
        Error(_) -> Nil
        Ok(_) if bytes_read < to_read -> Nil
        Ok(_) ->
          stream_file(
            handle,
            offset: offset + bytes_read,
            remaining: option.map(remaining, fn(n) { n - bytes_read }),
            emit:,
          )
      }
    }
  }
}

fn send_prepared_body(
  socket: Socket,
  request_id: Int,
  body: PreparedResponseBody,
) -> Nil {
  case body {
    PreparedBytes(data) -> {
      let _ =
        send_if_nonempty(
          socket,
          handler.encode_response_body_tree(request_id, data),
        )
      Nil
    }
    PreparedStream(producer) -> {
      let sender =
        StreamSender(emit: fn(data) {
          send_if_nonempty(
            socket,
            handler.encode_stdout_chunk(request_id, data),
          )
          |> result.replace_error(Nil)
        })
      let _ = exception.rescue(fn() { producer(sender) })
      Nil
    }
  }
}

fn build_request(
  params_bytes: BitArray,
  body: BitArray,
) -> Result(Request(Connection), String) {
  use pairs <- result.try(
    protocol.parse_name_value_pairs(params_bytes)
    |> result.replace_error("malformed FastCGI parameters"),
  )
  let env = dict.from_list(pairs)
  use _ <- result.try(check_content_length(env, body))
  to_http_request(env, Connection(body))
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

  Ok(request.Request(
    method:,
    headers: build_headers(env),
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

fn build_headers(env: Dict(String, String)) -> List(#(String, String)) {
  env
  |> dict.to_list
  |> list.filter_map(map_header)
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

fn check_content_length(
  env: dict.Dict(String, String),
  body: BitArray,
) -> Result(Nil, String) {
  case dict.get(env, "CONTENT_LENGTH") {
    Error(_) -> Ok(Nil)
    Ok("") -> Ok(Nil)
    Ok(raw) ->
      case int.parse(raw) {
        Error(_) -> Error("invalid CONTENT_LENGTH: " <> raw)
        Ok(declared) if declared < 0 -> Error("invalid CONTENT_LENGTH: " <> raw)
        Ok(declared) -> {
          let actual = bit_array.byte_size(body)
          case declared == actual {
            True -> Ok(Nil)
            False ->
              Error(
                "CONTENT_LENGTH "
                <> int.to_string(declared)
                <> " does not match received body of "
                <> int.to_string(actual)
                <> " bytes",
              )
          }
        }
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
    _ -> send(socket, bytes_tree.to_bit_array(bytes))
  }
}
