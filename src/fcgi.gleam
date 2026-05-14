//// FastCGI Responder server.

import exception
import fcgi/internal/body_reader
import fcgi/internal/handler
import fcgi/internal/protocol
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
import gleam/otp/factory_supervisor
import gleam/otp/static_supervisor.{type Supervisor}
import gleam/otp/supervision
import gleam/result
import gleam/string

const default_body_read_timeout_ms = 30_000

const default_max_body_size = 268_435_456

/// Streaming reader for the request body. The handler receives one in
/// `req.body`; call it to obtain the first `Read`, then advance via the
/// `consume` continuation returned in each `Chunk`.
pub type BodyReader =
  fn() -> Result(Read, ReadError)

/// What a `BodyReader` produced.
pub type Read {
  /// A chunk of body bytes plus a `consume` continuation that returns
  /// the next chunk when called.
  Chunk(data: BitArray, consume: BodyReader)
  /// The body has been fully delivered.
  End
}

/// Why a body read failed.
pub type ReadError {
  /// The connection from the upstream proxy was closed before the body
  /// was fully delivered.
  ClientDisconnected
  /// No chunk arrived within the configured `body_read_timeout`.
  ReadTimeout
  /// The body exceeded `max_body_size`.
  BodyTooLarge
}

/// Buffer the entire body into a `BytesTree`. The server's
/// `max_body_size` setting is the upper bound; the underlying reader
/// returns `BodyTooLarge` if the peer exceeds it.
///
/// The buffered length reflects what the upstream proxy actually sent,
/// not what `CONTENT_LENGTH` advertised. `End` signals that the upstream
/// proxy closed stdin, not that the consumed byte count matches the
/// `CONTENT_LENGTH` header.
pub fn read_all(read: BodyReader) -> Result(BytesTree, ReadError) {
  read_all_loop(read(), bytes_tree.new())
}

fn read_all_loop(
  read: Result(Read, ReadError),
  acc: BytesTree,
) -> Result(BytesTree, ReadError) {
  case read {
    Error(reason) -> Error(reason)
    Ok(End) -> Ok(acc)
    Ok(Chunk(data, consume)) ->
      read_all_loop(consume(), bytes_tree.append(acc, data))
  }
}

type Address {
  PathAddress(path: String)
  TcpAddress(host: String, port: Int)
}

/// Server configuration produced by `new`. Pass it to `start` to begin
/// listening.
pub opaque type Builder(address) {
  Builder(
    handler: Handler,
    address: Option(Address),
    max_body_size: Int,
    body_read_timeout_ms: Int,
  )
}

type Handler =
  fn(Request(BodyReader), Context) -> Response(ResponseData)

/// Phantom marker indicating a `Builder` has had a listen address set.
pub type HasAddress

/// Set how long the server waits for the next stdin or params record
/// before giving up. Must be `> 0`. Applies between successive socket
/// reads, including the wait for the first record after `accept`, not
/// to the request as a whole. Returns `Error(ReadTimeout)` to body
/// readers. Default: 30,000 ms.
pub fn body_read_timeout(
  builder: Builder(address),
  milliseconds: Int,
) -> Builder(address) {
  Builder(..builder, body_read_timeout_ms: milliseconds)
}

/// Set the Unix domain socket path the server listens on.
pub fn listen_unix(
  builder: Builder(address),
  path: String,
) -> Builder(HasAddress) {
  Builder(..builder, address: option.Some(PathAddress(path)))
}

/// Set the TCP `host` and `port` the server listens on. `host` must be a
/// numeric IP literal: either IPv4 (e.g. `"127.0.0.1"`, `"0.0.0.0"`) or
/// IPv6 (e.g. `"::1"`, `"::"`).
pub fn listen_tcp(
  builder: Builder(address),
  host host: String,
  port port: Int,
) -> Builder(HasAddress) {
  Builder(..builder, address: option.Some(TcpAddress(host, port)))
}

/// Set the maximum body bytes the server will deliver to the handler in
/// total across all body reads. Must be `>= 0`; `start` returns
/// `InvalidMaxBodySize(bytes)` for negative values.
///
/// When the peer sends more than this many bytes, the next body read
/// returns `Error(BodyTooLarge)`.
///
/// Default: 256 MiB.
pub fn max_body_size(
  builder: Builder(address),
  bytes: Int,
) -> Builder(address) {
  Builder(..builder, max_body_size: bytes)
}

/// Build a new FastCGI server with the given handler.
///
/// The handler is invoked once `Params` is fully received. The request
/// body is delivered incrementally via a `BodyReader` in `req.body`;
/// call it to obtain the first `Read` and thread `consume` to advance,
/// or pass it to `read_all` for the buffered case.
///
/// Default: 256 MiB max body, 30 s body read timeout.
pub fn new(handler: Handler) -> Builder(Nil) {
  Builder(
    handler:,
    address: option.None,
    max_body_size: default_max_body_size,
    body_read_timeout_ms: default_body_read_timeout_ms,
  )
}

/// Trusted CGI metadata supplied by the upstream proxy and handed to
/// every request handler alongside the `Request`.
pub type Context {
  Context(
    /// Client address as reported by the proxy (CGI `REMOTE_ADDR`).
    remote_addr: Option(String),
    /// Client port parsed as `Int`; absent if the proxy did not supply a
    /// numeric value (CGI `REMOTE_PORT`).
    remote_port: Option(Int),
    /// Reverse-DNS hostname of the client, when the proxy resolved one
    /// (CGI `REMOTE_HOST`).
    remote_host: Option(String),
    /// Authenticated identity, when the proxy set one (CGI `REMOTE_USER`).
    remote_user: Option(String),
    /// Authentication scheme, e.g. `"Basic"` (CGI `AUTH_TYPE`).
    auth_type: Option(String),
    /// Mount prefix assigned to the app by the proxy (CGI `SCRIPT_NAME`).
    script_name: Option(String),
    /// HTTP protocol version reported by the proxy, e.g. `"HTTP/1.1"`
    /// (CGI `SERVER_PROTOCOL`).
    server_protocol: Option(String),
    /// Identification string from the upstream proxy (CGI `SERVER_SOFTWARE`).
    server_software: Option(String),
    /// Any other CGI variables, keyed by their original uppercase name.
    /// Typical entries include `"DOCUMENT_ROOT"` and `"REQUEST_URI"`.
    extra: Dict(String, String),
  )
}

/// Why `send_file` could not produce a `File` body.
pub type FileError {
  /// No file exists at the given path.
  FileNotFound(path: String)
  /// The process lacks permission to read the file.
  FileAccessDenied(path: String)
  /// The path resolved to a directory, not a file.
  FileIsDirectory(path: String)
  /// Any other filesystem error, with the underlying reason as a human-readable string.
  FileOther(path: String, reason: String)
  /// `offset` is negative, or `limit` is `Some(n)` with `n < 0`.
  InvalidRange(offset: Int, limit: Option(Int))
}

@internal
pub type InternalResponseData {
  Bytes(content: BytesTree)
  File(handle: Handle, offset: Int, length: Int)
  Stream(producer: fn(StreamSender) -> Nil)
}

/// What the server should send back as a response body. Construct with
/// `bytes`, `send_file`, or `stream`.
pub type ResponseData =
  InternalResponseData

/// Handle passed to a `Stream` producer. Use `send_chunk` to emit body
/// bytes; each call writes one or more FastCGI `STDOUT` records on the
/// open connection.
pub opaque type StreamSender {
  StreamSender(socket: Socket, request_id: Int)
}

/// Emit a chunk of body bytes from inside a `Stream` producer.
///
/// Returns `Ok(Nil)` when the chunk is written, or `Error(Nil)` when the
/// underlying socket write fails (for example, the upstream proxy has
/// disconnected).
pub fn send_chunk(sender: StreamSender, data: BitArray) -> Result(Nil, Nil) {
  let StreamSender(socket:, request_id:) = sender
  send_if_nonempty(
    socket,
    handler.encode_stdout_chunk(request_id, bytes_tree.from_bit_array(data)),
  )
  |> result.replace_error(Nil)
}

/// Build an in-memory response body. The whole `BytesTree` is sent in
/// one or more `STDOUT` records.
pub fn bytes(content: BytesTree) -> ResponseData {
  Bytes(content:)
}

/// Open a file and return a response body that streams it via
/// `file:sendfile/5` when the response is sent.
///
/// Returns `FileError` if the file is missing, inaccessible, a
/// directory, or if `offset` or `limit` is negative.
pub fn send_file(
  path path: String,
  offset offset: Int,
  limit limit: Option(Int),
) -> Result(ResponseData, FileError) {
  use <- bool.guard(
    when: offset < 0 || option.unwrap(limit, 0) < 0,
    return: Error(InvalidRange(offset:, limit:)),
  )
  use #(handle, total_size) <- result.try(open_and_size(path))
  let max_length = option.unwrap(limit, total_size)
  let length = int.clamp(total_size - offset, min: 0, max: max_length)
  Ok(File(handle:, offset:, length:))
}

/// Build a streaming response body. The server calls `producer(sender)`
/// after sending the response headers; each call to `send_chunk(sender,
/// data)` writes one or more `STDOUT` records to the upstream proxy.
///
/// A panic raised by `producer` is caught; the response terminator is
/// still emitted so the upstream proxy sees a clean end-of-request.
pub fn stream(producer: fn(StreamSender) -> Nil) -> ResponseData {
  Stream(producer:)
}

type ServerHandler =
  fn(Request(Nil), Context, BodyReader) -> Response(ResponseData)

/// Why the listener could not start.
pub type StartError {
  /// Wraps a failure from the underlying listener, such as bind or
  /// listen failures, an unparseable TCP host, or a Unix-socket path
  /// that is already in use.
  ListenerError(reason: String)
  /// `max_body_size` was set to a negative value.
  InvalidMaxBodySize(bytes: Int)
  /// `body_read_timeout` was set to a negative value.
  InvalidBodyReadTimeout(milliseconds: Int)
}

/// Start the server.
pub fn start(
  builder: Builder(HasAddress),
) -> Result(actor.Started(Supervisor), StartError) {
  use <- bool.guard(
    when: builder.max_body_size < 0,
    return: Error(InvalidMaxBodySize(builder.max_body_size)),
  )
  use <- bool.guard(
    when: builder.body_read_timeout_ms <= 0,
    return: Error(InvalidBodyReadTimeout(builder.body_read_timeout_ms)),
  )
  let assert option.Some(address) = builder.address
  use socket <- result.try(listen_on_address(address))
  let factory_name = process.new_name(prefix: "fcgi_server_factory")
  let handler = fn(req: Request(Nil), ctx: Context, reader: BodyReader) {
    builder.handler(request.set_body(req, reader), ctx)
  }

  let supervisor =
    build_supervisor(
      address,
      socket,
      factory_name,
      builder.max_body_size,
      builder.body_read_timeout_ms,
      handler,
    )

  use started <- result.try(start_supervisor(supervisor, socket, address))

  controlling_process(socket, started.pid)
  |> result.replace(started)
  |> result.map_error(fn(error) {
    process.unlink(started.pid)
    process.send_abnormal_exit(started.pid, atom.create("shutdown"))
    cleanup_socket(socket, address)

    ListenerError(
      "controlling_process failed: " <> describe_transport_error(error),
    )
  })
}

/// Build a `supervision.ChildSpecification` so the server runs under an
/// OTP supervisor.
pub fn supervised(
  builder: Builder(HasAddress),
) -> supervision.ChildSpecification(Supervisor) {
  supervision.supervisor(fn() {
    start(builder)
    |> result.map_error(fn(error) {
      let reason = case error {
        ListenerError(reason) -> reason
        InvalidMaxBodySize(bytes) ->
          "max_body_size must be non-negative; got " <> int.to_string(bytes)
        InvalidBodyReadTimeout(milliseconds) ->
          "body_read_timeout must be positive; got "
          <> int.to_string(milliseconds)
      }

      actor.InitFailed(reason)
    })
  })
}

fn acceptor_supervised(
  socket: Socket,
  factory_name: process.Name(factory_supervisor.Message(Worker, Nil)),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: ServerHandler,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() {
    let factory = factory_supervisor.get_by_name(factory_name)
    let pid =
      process.spawn(fn() {
        accept_loop(
          socket,
          factory,
          max_body_size,
          body_read_timeout_ms,
          handler,
        )
      })
    Ok(actor.Started(pid:, data: Nil))
  })
  |> supervision.restart(supervision.Transient)
}

fn accept_loop(
  listen_socket: Socket,
  factory: factory_supervisor.Supervisor(Worker, Nil),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: ServerHandler,
) -> Nil {
  case accept(listen_socket) {
    Error(reason) -> {
      case atom.to_string(reason) {
        "emfile" | "enfile" -> {
          process.sleep(100)
        }
        _ -> Nil
      }
    }
    Ok(client) -> {
      let worker =
        Worker(socket: client, max_body_size:, body_read_timeout_ms:, handler:)
      case factory_supervisor.start_child(factory, worker) {
        Error(_) -> close_socket(client)
        Ok(started) ->
          case controlling_process(client, started.pid) {
            Error(_) -> {
              close_socket(client)
              process.send_exit(started.pid)
            }
            Ok(Nil) -> Nil
          }
      }
    }
  }

  accept_loop(
    listen_socket,
    factory,
    max_body_size,
    body_read_timeout_ms,
    handler,
  )
}

fn build_supervisor(
  address: Address,
  socket: Socket,
  factory_name: process.Name(factory_supervisor.Message(Worker, Nil)),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: ServerHandler,
) -> static_supervisor.Builder {
  let supervisor = static_supervisor.new(static_supervisor.RestForOne)
  let supervisor = case address {
    PathAddress(path) ->
      static_supervisor.add(supervisor, path_janitor_supervised(path))
    TcpAddress(_, _) -> supervisor
  }

  supervisor
  |> static_supervisor.add(connection_factory_supervised(factory_name))
  |> static_supervisor.add(acceptor_supervised(
    socket,
    factory_name,
    max_body_size,
    body_read_timeout_ms,
    handler,
  ))
}

fn cleanup_socket(socket: Socket, address: Address) -> Nil {
  close_socket(socket)
  case address {
    PathAddress(path) -> delete_path(path)
    TcpAddress(_, _) -> Nil
  }
}

fn connection_factory_supervised(
  name: process.Name(factory_supervisor.Message(Worker, Nil)),
) -> supervision.ChildSpecification(factory_supervisor.Supervisor(Worker, Nil)) {
  factory_supervisor.worker_child(start_connection)
  |> factory_supervisor.named(name)
  |> factory_supervisor.supervised
  |> supervision.restart(supervision.Transient)
}

fn describe_transport_error(error: SocketError) -> String {
  case error {
    Posix(reason) -> atom.to_string(reason)
    _ -> "unexpected transport error"
  }
}

fn listen_on_address(address: Address) -> Result(Socket, StartError) {
  case address {
    PathAddress(path) ->
      do_listen_unix(path)
      |> result.map_error(fn(error) {
        case error {
          PathExists(path) ->
            ListenerError("socket path already exists: " <> path)
          _ ->
            ListenerError("listen failed: " <> describe_transport_error(error))
        }
      })
    TcpAddress(host, port) ->
      do_listen_tcp(host, port)
      |> result.map_error(fn(error) {
        case error {
          InvalidHost(host) ->
            ListenerError("host must be a numeric IP literal: " <> host)
          _ ->
            ListenerError("listen failed: " <> describe_transport_error(error))
        }
      })
  }
}

fn path_janitor_supervised(
  path: String,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start_path_janitor(path) })
  |> supervision.restart(supervision.Transient)
}

fn start_path_janitor(path: String) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(_subject) {
    process.trap_exits(True)
    let selector =
      process.new_selector()
      |> process.select_trapped_exits(fn(_msg) { Nil })
    actor.initialised(path)
    |> actor.selecting(selector)
    |> actor.returning(Nil)
    |> Ok
  })
  |> actor.on_message(fn(path, _message) {
    delete_path(path)
    actor.stop()
  })
  |> actor.start
}

fn start_supervisor(
  builder: static_supervisor.Builder,
  socket: Socket,
  address: Address,
) -> Result(actor.Started(Supervisor), StartError) {
  case static_supervisor.start(builder) {
    Error(error) -> {
      cleanup_socket(socket, address)
      let reason = case error {
        actor.InitTimeout -> "supervisor init timeout"
        actor.InitFailed(reason) -> reason
        actor.InitExited(_) -> "supervisor init exited"
      }
      Error(ListenerError(reason))
    }
    Ok(started) -> Ok(started)
  }
}

type Env {
  Env(
    method: Option(String),
    https: Option(String),
    server_name: Option(String),
    server_port_raw: Option(String),
    http_host: Option(String),
    path: Option(String),
    query: Option(String),
    headers: List(#(String, String)),
    context: Context,
  )
}

type NextRequest {
  Ready(
    state: handler.State,
    request_id: Int,
    params: BitArray,
    body_queue: List(handler.Event),
    keep_conn: Bool,
  )
  Closed
}

@internal
pub type RequestError {
  MissingMethod
  InvalidMethod(method: String)
}

type Worker {
  Worker(
    socket: Socket,
    max_body_size: Int,
    body_read_timeout_ms: Int,
    handler: ServerHandler,
  )
}

fn start_connection(worker: Worker) -> actor.StartResult(Nil) {
  let pid =
    process.spawn(fn() {
      run_connection_loop(worker, handler.Idle(<<>>))
      close_socket(worker.socket)
    })
  Ok(actor.Started(pid:, data: Nil))
}

fn run_connection_loop(worker: Worker, state: handler.State) -> Nil {
  case await_next_request(worker, state, <<>>) {
    Closed -> Nil
    Ready(state:, request_id:, params:, body_queue:, keep_conn:) ->
      case serve_request(worker, state, request_id, params, body_queue, keep_conn) {
        Error(_) -> Nil
        Ok(next_state) -> run_connection_loop(worker, next_state)
      }
  }
}

fn serve_request(
  worker: Worker,
  state: handler.State,
  request_id: Int,
  params: BitArray,
  body_queue: List(handler.Event),
  keep_conn: Bool,
) -> Result(handler.State, Nil) {
  use tracker <- result.try(handle_request(
    worker,
    state,
    request_id,
    params,
    body_queue,
  ))
  use <- bool.guard(when: !keep_conn, return: Error(Nil))
  case tracker {
    option.None -> Ok(state)
    option.Some(tracker) ->
      body_reader.finalize(
        tracker:,
        max_body_size: worker.max_body_size,
        recv: fn() { adapt_recv(worker) },
        send: fn(bytes) {
          let _ = send_if_nonempty(worker.socket, bytes)
          Nil
        },
      )
  }
}

@internal
pub fn to_http_request(
  pairs: List(#(String, String)),
  body: body,
) -> Result(#(Request(body), Context), RequestError) {
  collect_env(pairs)
  |> env_to_request(body)
}

fn absorb_context_var(ctx: Context, key: String, value: String) -> Context {
  case key {
    "REMOTE_ADDR" -> Context(..ctx, remote_addr: option.Some(value))
    "REMOTE_PORT" ->
      Context(..ctx, remote_port: int.parse(value) |> option.from_result)
    "REMOTE_HOST" -> Context(..ctx, remote_host: option.Some(value))
    "REMOTE_USER" -> Context(..ctx, remote_user: option.Some(value))
    "AUTH_TYPE" -> Context(..ctx, auth_type: option.Some(value))
    "SCRIPT_NAME" -> Context(..ctx, script_name: option.Some(value))
    "SERVER_PROTOCOL" -> Context(..ctx, server_protocol: option.Some(value))
    "SERVER_SOFTWARE" -> Context(..ctx, server_software: option.Some(value))
    _ -> Context(..ctx, extra: dict.insert(ctx.extra, key, value))
  }
}

fn absorb_pair(env: Env, key: String, value: String) -> Env {
  case key {
    "REQUEST_METHOD" -> Env(..env, method: option.Some(value))
    "HTTPS" -> Env(..env, https: option.Some(value))
    "SERVER_NAME" -> Env(..env, server_name: option.Some(value))
    "SERVER_PORT" -> Env(..env, server_port_raw: option.Some(value))
    "HTTP_HOST" ->
      Env(..env, http_host: option.Some(value), headers: [
        #("host", value),
        ..env.headers
      ])
    "PATH_INFO" -> Env(..env, path: option.Some(value))
    "QUERY_STRING" -> Env(..env, query: option.Some(value))
    "CONTENT_TYPE" ->
      Env(..env, headers: [#("content-type", value), ..env.headers])
    "CONTENT_LENGTH" ->
      Env(..env, headers: [#("content-length", value), ..env.headers])
    "HTTP_" <> name ->
      Env(..env, headers: [
        #(string.replace(string.lowercase(name), "_", "-"), value),
        ..env.headers
      ])
    _ -> Env(..env, context: absorb_context_var(env.context, key, value))
  }
}

fn adapt_recv(worker: Worker) -> body_reader.RecvOutcome {
  case recv(worker.socket, 0, worker.body_read_timeout_ms) {
    Error(Posix(reason)) ->
      case atom.to_string(reason) {
        "timeout" -> body_reader.RecvTimeout
        _ -> body_reader.RecvClosed
      }
    Error(_) -> body_reader.RecvClosed
    Ok(<<>>) -> body_reader.RecvClosed
    Ok(more) -> body_reader.RecvData(more)
  }
}

fn adapt_step(step: body_reader.ReadStep) -> Result(Read, ReadError) {
  case step {
    body_reader.Done -> Ok(End)
    body_reader.More(data, consume) ->
      Ok(Chunk(data:, consume: fn() { adapt_step(consume()) }))
    body_reader.TooLarge -> Error(BodyTooLarge)
    body_reader.Disconnected -> Error(ClientDisconnected)
    body_reader.Timeout -> Error(ReadTimeout)
  }
}

fn await_next_request(
  worker: Worker,
  state: handler.State,
  pending: BitArray,
) -> NextRequest {
  let outcome =
    handler.step(state, bytes: pending, max_body_size: worker.max_body_size)
  let _ = send_if_nonempty(worker.socket, outcome.outgoing)
  case extract_start(outcome.events) {
    option.Some(#(request_id, params, keep_conn, queue)) ->
      Ready(
        state: outcome.state,
        request_id:,
        params:,
        body_queue: queue,
        keep_conn:,
      )
    option.None ->
      case outcome.continuation {
        handler.CloseConnection -> Closed
        handler.WaitForMore ->
          case recv(worker.socket, 0, worker.body_read_timeout_ms) {
            Error(_) -> Closed
            Ok(<<>>) -> Closed
            Ok(more) -> await_next_request(worker, outcome.state, more)
          }
      }
  }
}

fn collect_env(pairs: List(#(String, String))) -> Env {
  let empty =
    Env(
      method: option.None,
      https: option.None,
      server_name: option.None,
      server_port_raw: option.None,
      http_host: option.None,
      path: option.None,
      query: option.None,
      headers: [],
      context: Context(
        remote_addr: option.None,
        remote_port: option.None,
        remote_host: option.None,
        remote_user: option.None,
        auth_type: option.None,
        script_name: option.None,
        server_protocol: option.None,
        server_software: option.None,
        extra: dict.new(),
      ),
    )
  list.fold(pairs, empty, fn(env, pair) { absorb_pair(env, pair.0, pair.1) })
}

fn env_to_request(
  env: Env,
  body: body,
) -> Result(#(Request(body), Context), RequestError) {
  use method_str <- result.try(option.to_result(env.method, MissingMethod))
  use method <- result.try(
    http.parse_method(method_str)
    |> result.replace_error(InvalidMethod(method_str)),
  )
  let scheme = case env.https {
    option.Some(value) -> https_scheme_from_value(value)
    option.None -> http.Http
  }
  let server_name = option.unwrap(env.server_name, "localhost")
  let server_port =
    option.then(env.server_port_raw, fn(raw) {
      int.parse(raw)
      |> option.from_result
    })
  let #(host, port) = case env.http_host {
    option.Some(raw) -> resolve_http_host(raw, server_name, server_port)
    option.None -> #(server_name, server_port)
  }
  let path = option.unwrap(env.path, "/")
  let req =
    request.Request(
      method:,
      headers: env.headers,
      body:,
      scheme:,
      host:,
      port:,
      path:,
      query: env.query,
    )
  Ok(#(req, env.context))
}

fn error_response(status: Int, message: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(Bytes(bytes_tree.from_string(message)))
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

fn handle_request(
  worker: Worker,
  state: handler.State,
  request_id: Int,
  params: BitArray,
  events: List(handler.Event),
) -> Result(Option(body_reader.Tracker), Nil) {
  use <- bool.lazy_guard(
    when: list.contains(events, handler.BodyTooLarge),
    return: fn() {
      let _ =
        send_if_nonempty(
          worker.socket,
          handler.encode_overloaded_end(request_id),
        )
      Error(Nil)
    },
  )

  case parse_params(params) {
    Error(message) -> {
      let response = error_response(400, message)
      send_response(worker.socket, request_id, response)
      |> result.replace(option.None)
    }
    Ok(#(req, cgi)) -> {
      let #(reader, tracker) = make_reader(worker, state, events)
      let response = run_user_handler(worker.handler, req, cgi, reader)
      send_response(worker.socket, request_id, response)
      |> result.replace(tracker)
    }
  }
}

fn https_scheme_from_value(value: String) -> http.Scheme {
  case string.lowercase(value) {
    "" | "off" -> http.Http
    _ -> http.Https
  }
}

fn make_reader(
  worker: Worker,
  state: handler.State,
  events: List(handler.Event),
) -> #(BodyReader, Option(body_reader.Tracker)) {
  let #(step_fn, tracker) =
    body_reader.start(
      state:,
      events:,
      max_body_size: worker.max_body_size,
      recv: fn() { adapt_recv(worker) },
      send: fn(bytes) {
        let _ = send_if_nonempty(worker.socket, bytes)
        Nil
      },
    )
  #(fn() { adapt_step(step_fn()) }, tracker)
}

fn parse_params(params: BitArray) -> Result(#(Request(Nil), Context), String) {
  case protocol.parse_name_value_pairs(params) {
    Error(_) -> Error("malformed FastCGI parameters")
    Ok(pairs) ->
      to_http_request(pairs, Nil)
      |> result.map_error(request_error_message)
  }
}

fn request_error_message(error: RequestError) -> String {
  case error {
    MissingMethod -> "missing REQUEST_METHOD parameter"
    InvalidMethod(method) -> "invalid REQUEST_METHOD: " <> method
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

fn run_user_handler(
  handler_fn: ServerHandler,
  req: Request(Nil),
  ctx: Context,
  reader: BodyReader,
) -> Response(ResponseData) {
  case exception.rescue(fn() { handler_fn(req, ctx, reader) }) {
    Ok(resp) -> resp
    Error(_) -> error_response(500, "internal server error")
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

fn send_if_nonempty(
  socket: Socket,
  bytes: BytesTree,
) -> Result(Nil, SocketError) {
  case bytes_tree.byte_size(bytes) {
    0 -> Ok(Nil)
    _ -> send_tree(socket, bytes)
  }
}

fn send_padding(
  socket: Socket,
  padding_length: Int,
) -> Result(Nil, SocketError) {
  use <- bool.guard(when: padding_length == 0, return: Ok(Nil))
  send_bits(socket, <<0:size({ padding_length * 8 })>>)
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
      let combined = bytes_tree.concat([header, body, terminator])
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

      let sender = StreamSender(socket:, request_id:)
      let _ = exception.rescue(fn() { producer(sender) })

      send_tree(socket, terminator)
      |> result.replace_error(Nil)
    }
  }
}

fn send_via_sendfile(
  socket: Socket,
  request_id: Int,
  handle: Handle,
  offset: Int,
  remaining: Int,
) -> Result(Nil, Nil) {
  use <- bool.guard(when: remaining <= 0, return: Ok(Nil))

  let chunk_size = int.min(remaining, protocol.max_record_content_size)
  let #(header, padding_length) =
    protocol.encode_stdout_frame_header(request_id, chunk_size)
  use _ <- result.try(
    send_bits(socket, header)
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

@internal
pub type Handle

type Socket

type SocketError {
  PathExists(path: String)
  InvalidHost(host: String)
  Posix(reason: Atom)
}

@external(erlang, "gen_tcp", "accept")
fn accept(listen: Socket) -> Result(Socket, Atom)

@external(erlang, "fcgi_ffi", "listen_tcp")
fn do_listen_tcp(host: String, port: Int) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "listen_unix")
fn do_listen_unix(path: String) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "close_file")
fn close_file(handle: Handle) -> Nil

@external(erlang, "fcgi_ffi", "close_socket")
fn close_socket(socket: Socket) -> Nil

@external(erlang, "fcgi_ffi", "controlling_process")
fn controlling_process(
  socket: Socket,
  pid: process.Pid,
) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "delete_path")
fn delete_path(path: String) -> Nil

@external(erlang, "fcgi_ffi", "open_and_size")
fn open_and_size(path: String) -> Result(#(Handle, Int), FileError)

@external(erlang, "fcgi_ffi", "recv")
fn recv(
  socket: Socket,
  size: Int,
  timeout_ms: Int,
) -> Result(BitArray, SocketError)

@external(erlang, "fcgi_ffi", "send")
fn send_bits(socket: Socket, data: BitArray) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "send")
fn send_tree(socket: Socket, data: BytesTree) -> Result(Nil, SocketError)

@external(erlang, "fcgi_ffi", "sendfile")
fn sendfile(
  handle: Handle,
  socket: Socket,
  offset: Int,
  bytes: Int,
) -> Result(Int, Nil)
