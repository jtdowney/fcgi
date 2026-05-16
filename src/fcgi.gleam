//// FastCGI Responder server.

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
import gleam/otp/factory_supervisor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import logging

const default_body_read_timeout_ms = 30_000

const default_max_body_size = 268_435_456

const socket_owner_transfer_timeout_ms = 5000

/// Listen address set on a `Builder` via `listen_unix` or `listen_tcp`.
pub opaque type Address {
  PathAddress(path: String)
  TcpAddress(host: String, port: Int)
}

/// Server configuration produced by `new`. Pass it to `start` to begin
/// listening.
pub opaque type Builder(address) {
  Builder(
    handler: Handler,
    address: address,
    max_body_size: Int,
    body_read_timeout_ms: Int,
  )
}

type Handler =
  fn(Request(BodyReader), Context) -> Response(ResponseData)

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
    address: Nil,
    max_body_size: default_max_body_size,
    body_read_timeout_ms: default_body_read_timeout_ms,
  )
}

/// Set the Unix domain socket path the server listens on.
pub fn listen_unix(
  builder: Builder(address),
  path: String,
) -> Builder(Address) {
  Builder(
    handler: builder.handler,
    address: PathAddress(path),
    max_body_size: builder.max_body_size,
    body_read_timeout_ms: builder.body_read_timeout_ms,
  )
}

/// Set the TCP `host` and `port` the server listens on. `host` must be a
/// numeric IP literal: either IPv4 (e.g. `"127.0.0.1"`, `"0.0.0.0"`) or
/// IPv6 (e.g. `"::1"`, `"::"`).
pub fn listen_tcp(
  builder: Builder(address),
  host host: String,
  port port: Int,
) -> Builder(Address) {
  Builder(
    handler: builder.handler,
    address: TcpAddress(host, port),
    max_body_size: builder.max_body_size,
    body_read_timeout_ms: builder.body_read_timeout_ms,
  )
}

/// Set the maximum body bytes the server will deliver to the handler in
/// total across all body reads. Must be `>= 0`; `start` returns
/// `InvalidMaxBodySize(bytes)` for negative values.
///
/// When the peer sends more than this many bytes, the next body read
/// returns `Error(BodyTooLarge)`. The handler may respond as it sees fit,
/// but the connection is closed after the response is sent because
/// remaining body bytes cannot be safely drained.
///
/// Default: 256 MiB.
pub fn max_body_size(
  builder: Builder(address),
  bytes: Int,
) -> Builder(address) {
  Builder(..builder, max_body_size: bytes)
}

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
/// not what `CONTENT_LENGTH` advertised.
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

/// What the server should send back as a response body. Construct with
/// `bytes`, `send_file`, or `stream`.
pub opaque type ResponseData {
  Bytes(content: BytesTree)
  File(handle: Handle, offset: Int, length: Int)
  Stream(producer: fn(StreamSender) -> Nil)
}

/// Use `send_chunk` to emit body bytes; each call writes one or more FastCGI
/// `STDOUT` records on the open connection.
pub opaque type StreamSender {
  StreamSender(socket: Socket, request_id: Int)
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

/// Emit a chunk of body bytes from inside a `Stream` producer.
///
/// Returns `Ok(Nil)` when the chunk is written, or `Error(Nil)` when the
/// underlying socket write fails (for example, the upstream proxy has
/// disconnected).
pub fn send_chunk(sender: StreamSender, data: BytesTree) -> Result(Nil, Nil) {
  let StreamSender(socket:, request_id:) = sender
  send_if_nonempty(socket, handler.encode_stdout_chunk(request_id, data))
}

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
  builder: Builder(Address),
) -> Result(actor.Started(static_supervisor.Supervisor), StartError) {
  use <- bool.guard(
    when: builder.max_body_size < 0,
    return: Error(InvalidMaxBodySize(builder.max_body_size)),
  )
  use <- bool.guard(
    when: builder.body_read_timeout_ms <= 0,
    return: Error(InvalidBodyReadTimeout(builder.body_read_timeout_ms)),
  )
  let address = builder.address
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

  case controlling_process(socket, started.pid) {
    Ok(Nil) -> {
      logging.log(
        logging.Info,
        "fcgi listening on " <> describe_address(address),
      )
      Ok(started)
    }
    Error(error) -> {
      process.unlink(started.pid)
      process.send_abnormal_exit(started.pid, atom.create("shutdown"))
      cleanup_socket(socket, address)
      Error(ListenerError(
        "controlling_process failed: " <> describe_transport_error(error),
      ))
    }
  }
}

/// Build a `supervision.ChildSpecification` so the server runs under an
/// OTP supervisor.
pub fn supervised(
  builder: Builder(Address),
) -> supervision.ChildSpecification(static_supervisor.Supervisor) {
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

type ServerHandler =
  fn(Request(Nil), Context, BodyReader) -> Response(ResponseData)

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

fn build_supervisor(
  address: Address,
  socket: Socket,
  factory_name: process.Name(
    factory_supervisor.Message(Worker, process.Subject(Nil)),
  ),
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

fn connection_factory_supervised(
  name: process.Name(factory_supervisor.Message(Worker, process.Subject(Nil))),
) -> supervision.ChildSpecification(
  factory_supervisor.Supervisor(Worker, process.Subject(Nil)),
) {
  factory_supervisor.worker_child(start_connection)
  |> factory_supervisor.named(name)
  |> factory_supervisor.supervised
  |> supervision.restart(supervision.Transient)
}

fn acceptor_supervised(
  socket: Socket,
  factory_name: process.Name(
    factory_supervisor.Message(Worker, process.Subject(Nil)),
  ),
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

fn start_supervisor(
  builder: static_supervisor.Builder,
  socket: Socket,
  address: Address,
) -> Result(actor.Started(static_supervisor.Supervisor), StartError) {
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

fn cleanup_socket(socket: Socket, address: Address) -> Nil {
  close_socket(socket)
  case address {
    PathAddress(path) -> delete_path(path)
    TcpAddress(_, _) -> Nil
  }
}

fn describe_address(address: Address) -> String {
  case address {
    PathAddress(path) -> "unix:" <> path
    TcpAddress(host, port) -> {
      let host = case string.contains(host, ":") {
        True -> "[" <> host <> "]"
        False -> host
      }
      host <> ":" <> int.to_string(port)
    }
  }
}

fn describe_transport_error(error: SocketError) -> String {
  case error {
    Posix(reason) -> atom.to_string(reason)
    PathExists(_) | InvalidHost(_) -> "unexpected transport error"
  }
}

fn accept_loop(
  listen_socket: Socket,
  factory: factory_supervisor.Supervisor(Worker, process.Subject(Nil)),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: ServerHandler,
) -> Nil {
  case accept(listen_socket) {
    Error(reason) -> {
      case atom.to_string(reason) {
        "emfile" | "enfile" -> {
          logging.log(
            logging.Warning,
            "accept failed: file descriptor exhaustion, retrying",
          )
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
            Ok(Nil) -> process.send(started.data, Nil)
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

fn start_connection(worker: Worker) -> actor.StartResult(process.Subject(Nil)) {
  let report_back = process.new_subject()
  let pid =
    process.spawn(fn() {
      let go = process.new_subject()
      process.send(report_back, go)

      case process.receive(go, socket_owner_transfer_timeout_ms) {
        Ok(Nil) -> {
          run_connection_loop(worker, handler.Idle(<<>>))
          close_socket(worker.socket)
        }
        Error(Nil) -> close_socket(worker.socket)
      }
    })

  case process.receive(report_back, socket_owner_transfer_timeout_ms) {
    Ok(go) -> Ok(actor.Started(pid:, data: go))
    Error(Nil) -> {
      process.send_exit(pid)
      Error(actor.InitFailed("connection worker did not report back"))
    }
  }
}

type Worker {
  Worker(
    socket: Socket,
    max_body_size: Int,
    body_read_timeout_ms: Int,
    handler: ServerHandler,
  )
}

type NextRequest {
  Ready(
    state: handler.State,
    request_id: Int,
    params: BitArray,
    remaining_events: List(handler.Event),
    keep_conn: Bool,
  )
  Closed
}

fn run_connection_loop(worker: Worker, state: handler.State) -> Nil {
  case next_request(worker, state, <<>>) {
    Closed -> Nil
    Ready(state:, request_id:, params:, remaining_events:, keep_conn:) ->
      case
        process_request(worker, state, request_id, params, remaining_events)
      {
        Error(_) -> Nil
        Ok(snapshots) -> resume_connection(worker, state, keep_conn, snapshots)
      }
  }
}

fn resume_connection(
  worker: Worker,
  state: handler.State,
  keep_conn: Bool,
  snapshots: Option(process.Subject(BodySnapshot)),
) -> Nil {
  use <- bool.guard(when: !keep_conn, return: Nil)
  case snapshots {
    option.None -> run_connection_loop(worker, state)
    option.Some(subject) ->
      case drain_unread_body(worker, subject) {
        Error(_) -> Nil
        Ok(next_state) -> run_connection_loop(worker, next_state)
      }
  }
}

fn next_request(
  worker: Worker,
  state: handler.State,
  pending: BitArray,
) -> NextRequest {
  let buffered = case state {
    handler.Idle(buf) -> buf
    handler.Receiving(recv) -> recv.buffer
  }
  use <- bool.lazy_guard(
    when: bit_array.byte_size(pending) == 0
      && bit_array.byte_size(buffered) == 0,
    return: fn() { wait_for_bytes(worker, state) },
  )
  let outcome =
    handler.step(state, bytes: pending, max_body_size: worker.max_body_size)
  let _ = send_if_nonempty(worker.socket, outcome.outgoing)
  case outcome.events {
    [handler.Start(request_id, params, keep_conn), ..rest] ->
      Ready(
        state: outcome.state,
        request_id:,
        params:,
        remaining_events: rest,
        keep_conn:,
      )
    _ ->
      case outcome.continuation {
        handler.CloseConnection -> Closed
        handler.WaitForMore -> wait_for_bytes(worker, outcome.state)
      }
  }
}

fn wait_for_bytes(worker: Worker, state: handler.State) -> NextRequest {
  case recv(worker.socket, 0, worker.body_read_timeout_ms) {
    Error(_) -> Closed
    Ok(<<>>) -> Closed
    Ok(more) -> next_request(worker, state, more)
  }
}

fn process_request(
  worker: Worker,
  state: handler.State,
  request_id: Int,
  params: BitArray,
  events: List(handler.Event),
) -> Result(Option(process.Subject(BodySnapshot)), Nil) {
  case parse_params(params) {
    Error(message) -> {
      logging.log(logging.Warning, "rejecting request: " <> message)
      let response = error_response(400, message)
      send_response(worker.socket, request_id, response)
      |> result.replace(option.None)
    }
    Ok(#(req, cgi)) -> {
      let overflowed = list.contains(events, handler.BodyTooLarge)
      let #(reader, snapshots) = build_body_reader(worker, state, events)
      let response = run_user_handler(worker.handler, req, cgi, reader)
      use _ <- result.try(send_response(worker.socket, request_id, response))
      case overflowed {
        True -> {
          logging.log(
            logging.Warning,
            "closing connection: body exceeded max_body_size of "
              <> int.to_string(worker.max_body_size)
              <> " bytes",
          )
          Error(Nil)
        }
        False -> Ok(snapshots)
      }
    }
  }
}

fn parse_params(params: BitArray) -> Result(#(Request(Nil), Context), String) {
  case protocol.parse_name_value_pairs(params) {
    Error(_) -> Error("malformed FastCGI parameters")
    Ok(pairs) ->
      to_http_request(pairs, Nil)
      |> result.map_error(request_error_message)
  }
}

fn error_response(status: Int, message: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(Bytes(bytes_tree.from_string(message)))
}

@internal
pub type RequestError {
  MissingMethod
  InvalidMethod(method: String)
}

type PartialRequest {
  PartialRequest(
    method: Option(String),
    https: Option(String),
    server_name: Option(String),
    server_port_raw: Option(String),
    http_host: Option(String),
    path: Option(String),
    query: Option(String),
    headers: List(#(String, String)),
    ctx: Context,
  )
}

@internal
pub fn to_http_request(
  pairs: List(#(String, String)),
  body: body,
) -> Result(#(Request(body), Context), RequestError) {
  let initial =
    PartialRequest(
      method: option.None,
      https: option.None,
      server_name: option.None,
      server_port_raw: option.None,
      http_host: option.None,
      path: option.None,
      query: option.None,
      headers: [],
      ctx: Context(
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
  fold_pairs_loop(pairs, initial, body)
}

fn fold_pairs_loop(
  pairs: List(#(String, String)),
  acc: PartialRequest,
  body: body,
) -> Result(#(Request(body), Context), RequestError) {
  case pairs {
    [] -> finalize_request(acc, body)
    [#(key, value), ..rest] -> {
      let next = apply_pair(acc, key, value)
      fold_pairs_loop(rest, next, body)
    }
  }
}

fn apply_pair(
  acc: PartialRequest,
  key: String,
  value: String,
) -> PartialRequest {
  case key {
    "REQUEST_METHOD" -> PartialRequest(..acc, method: option.Some(value))
    "HTTPS" -> PartialRequest(..acc, https: option.Some(value))
    "SERVER_NAME" -> PartialRequest(..acc, server_name: option.Some(value))
    "SERVER_PORT" -> PartialRequest(..acc, server_port_raw: option.Some(value))
    "HTTP_HOST" ->
      PartialRequest(..acc, http_host: option.Some(value), headers: [
        #("host", value),
        ..acc.headers
      ])
    "PATH_INFO" -> PartialRequest(..acc, path: option.Some(value))
    "QUERY_STRING" -> PartialRequest(..acc, query: option.Some(value))
    "CONTENT_TYPE" ->
      PartialRequest(..acc, headers: [#("content-type", value), ..acc.headers])
    "CONTENT_LENGTH" ->
      PartialRequest(..acc, headers: [#("content-length", value), ..acc.headers])
    "HTTP_" <> name ->
      PartialRequest(..acc, headers: [
        #(string.replace(string.lowercase(name), "_", "-"), value),
        ..acc.headers
      ])
    _ -> PartialRequest(..acc, ctx: apply_context_var(acc.ctx, key, value))
  }
}

fn apply_context_var(ctx: Context, key: String, value: String) -> Context {
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

fn finalize_request(
  acc: PartialRequest,
  body: body,
) -> Result(#(Request(body), Context), RequestError) {
  use method_str <- result.try(option.to_result(acc.method, MissingMethod))
  use method <- result.try(
    http.parse_method(method_str)
    |> result.replace_error(InvalidMethod(method_str)),
  )
  let scheme = case acc.https {
    option.Some(value) -> https_scheme_from_value(value)
    option.None -> http.Http
  }
  let server_name = option.unwrap(acc.server_name, "localhost")
  let server_port =
    option.then(acc.server_port_raw, fn(raw) {
      int.parse(raw)
      |> option.from_result
    })
  let #(host, port) = case acc.http_host {
    option.Some(raw) -> resolve_http_host(raw, server_name, server_port)
    option.None -> #(server_name, server_port)
  }
  let path = option.unwrap(acc.path, "/")
  let req =
    request.Request(
      method:,
      headers: acc.headers,
      body:,
      scheme:,
      host:,
      port:,
      path:,
      query: acc.query,
    )
  Ok(#(req, acc.ctx))
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
      let port =
        int.parse(port_str)
        |> option.from_result
      #(host, option.or(port, server_port))
    }
    _ -> #(raw, server_port)
  }
}

fn request_error_message(error: RequestError) -> String {
  case error {
    MissingMethod -> "missing REQUEST_METHOD parameter"
    InvalidMethod(method) -> "invalid REQUEST_METHOD: " <> method
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
    Error(exception) -> {
      logging.log(
        logging.Error,
        "handler crashed: " <> string.inspect(exception),
      )
      error_response(500, "internal server error")
    }
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
      let combined = bytes_tree.concat([header, body, terminator])
      send_if_nonempty(socket, combined)
    }
    File(handle, offset, length) -> {
      use _ <- result.try(send_tree(socket, header))
      use _ <- result.try(send_file_body(
        socket,
        request_id,
        handle,
        offset,
        length,
      ))

      send_tree(socket, terminator)
    }
    Stream(producer) -> {
      use _ <- result.try(send_tree(socket, header))

      let sender = StreamSender(socket:, request_id:)
      case exception.rescue(fn() { producer(sender) }) {
        Ok(_) -> Nil
        Error(exception) ->
          logging.log(
            logging.Error,
            "stream producer crashed: " <> string.inspect(exception),
          )
      }

      send_tree(socket, terminator)
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
  use _ <- result.try(send_bits(socket, header))
  use _ <- result.try(drain_sendfile_loop(socket, handle, offset, chunk_size))
  use _ <- result.try(send_padding(socket, padding_length))
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

fn send_padding(socket: Socket, padding_length: Int) -> Result(Nil, Nil) {
  use <- bool.guard(when: padding_length == 0, return: Ok(Nil))
  send_bits(socket, <<0:size({ padding_length * 8 })>>)
}

fn send_if_nonempty(socket: Socket, bytes: BytesTree) -> Result(Nil, Nil) {
  case bytes_tree.byte_size(bytes) {
    0 -> Ok(Nil)
    _ -> send_tree(socket, bytes)
  }
}

type BodyContext {
  BodyContext(
    state: handler.State,
    pending: BitArray,
    finished: Bool,
    overflowed: Bool,
    worker: Worker,
    snapshots: process.Subject(BodySnapshot),
  )
}

type BodySnapshot {
  BodySnapshot(state: handler.State, finished: Bool, overflowed: Bool)
}

fn build_body_reader(
  worker: Worker,
  state: handler.State,
  events: List(handler.Event),
) -> #(BodyReader, Option(process.Subject(BodySnapshot))) {
  let #(data, ended, overflowed) = collect_body_events(events)
  case ended || overflowed {
    True -> #(buffered_reader(data, overflowed), option.None)
    False -> {
      let subject = process.new_subject()
      process.send(
        subject,
        BodySnapshot(state:, finished: False, overflowed: False),
      )
      let ctx =
        BodyContext(
          state:,
          pending: data,
          finished: False,
          overflowed: False,
          worker:,
          snapshots: subject,
        )
      #(fn() { next_read(ctx) }, option.Some(subject))
    }
  }
}

fn collect_body_events(events: List(handler.Event)) -> #(BitArray, Bool, Bool) {
  collect_body_events_loop(events, <<>>, False, False)
}

fn collect_body_events_loop(
  events: List(handler.Event),
  data: BitArray,
  ended: Bool,
  overflowed: Bool,
) -> #(BitArray, Bool, Bool) {
  case events {
    [] -> #(data, ended, overflowed)
    [handler.BodyChunk(chunk), ..rest] ->
      collect_body_events_loop(
        rest,
        <<data:bits, chunk:bits>>,
        ended,
        overflowed,
      )
    [handler.BodyEnd, ..rest] ->
      collect_body_events_loop(rest, data, True, overflowed)
    [handler.BodyTooLarge, ..rest] ->
      collect_body_events_loop(rest, data, True, True)
    [handler.Start(_, _, _), ..rest] ->
      collect_body_events_loop(rest, data, ended, overflowed)
  }
}

fn buffered_reader(data: BitArray, overflowed: Bool) -> BodyReader {
  case overflowed, bit_array.byte_size(data) {
    True, _ -> fn() { Error(BodyTooLarge) }
    False, 0 -> fn() { Ok(End) }
    False, _ -> fn() { Ok(Chunk(data, fn() { Ok(End) })) }
  }
}

fn next_read(ctx: BodyContext) -> Result(Read, ReadError) {
  use <- bool.guard(when: ctx.overflowed, return: Error(BodyTooLarge))
  case bit_array.byte_size(ctx.pending), ctx.finished {
    0, True -> Ok(End)
    0, False -> pull_more(ctx)
    _, _ -> deliver_pending(ctx)
  }
}

fn deliver_pending(ctx: BodyContext) -> Result(Read, ReadError) {
  let next_ctx = BodyContext(..ctx, pending: <<>>)
  process.send(
    next_ctx.snapshots,
    BodySnapshot(
      state: next_ctx.state,
      finished: next_ctx.finished,
      overflowed: next_ctx.overflowed,
    ),
  )
  Ok(Chunk(data: ctx.pending, consume: fn() { next_read(next_ctx) }))
}

fn pull_more(ctx: BodyContext) -> Result(Read, ReadError) {
  let prior =
    BodySnapshot(
      state: ctx.state,
      finished: ctx.finished,
      overflowed: ctx.overflowed,
    )
  use #(new_data, next) <- result.try(recv_and_step(prior, ctx.worker))
  let next_ctx =
    BodyContext(
      ..ctx,
      state: next.state,
      pending: <<ctx.pending:bits, new_data:bits>>,
      finished: next.finished,
      overflowed: next.overflowed,
    )
  process.send(next_ctx.snapshots, next)
  next_read(next_ctx)
}

fn recv_and_step(
  prior: BodySnapshot,
  worker: Worker,
) -> Result(#(BitArray, BodySnapshot), ReadError) {
  use more <- result.try(worker_recv(worker))
  let outcome =
    handler.step(prior.state, bytes: more, max_body_size: worker.max_body_size)
  let _ = send_if_nonempty(worker.socket, outcome.outgoing)
  let #(data, ended, overflowed) = collect_body_events(outcome.events)
  let snapshot =
    BodySnapshot(
      state: outcome.state,
      finished: prior.finished || ended,
      overflowed: prior.overflowed || overflowed,
    )
  Ok(#(data, snapshot))
}

fn worker_recv(worker: Worker) -> Result(BitArray, ReadError) {
  case recv(worker.socket, 0, worker.body_read_timeout_ms) {
    Error(Posix(reason)) ->
      case atom.to_string(reason) {
        "timeout" -> Error(ReadTimeout)
        _ -> Error(ClientDisconnected)
      }
    Error(_) -> Error(ClientDisconnected)
    Ok(<<>>) -> Error(ClientDisconnected)
    Ok(more) -> Ok(more)
  }
}

fn drain_unread_body(
  worker: Worker,
  subject: process.Subject(BodySnapshot),
) -> Result(handler.State, Nil) {
  case process.receive(subject, 0) {
    Error(_) -> Error(Nil)
    Ok(first) -> drain_body_loop(latest_snapshot_loop(subject, first), worker)
  }
}

fn latest_snapshot_loop(
  subject: process.Subject(BodySnapshot),
  latest: BodySnapshot,
) -> BodySnapshot {
  case process.receive(subject, 0) {
    Error(_) -> latest
    Ok(snap) -> latest_snapshot_loop(subject, snap)
  }
}

fn drain_body_loop(
  snap: BodySnapshot,
  worker: Worker,
) -> Result(handler.State, Nil) {
  use <- bool.guard(when: snap.overflowed, return: Error(Nil))
  use <- bool.guard(when: snap.finished, return: Ok(snap.state))
  case recv_and_step(snap, worker) {
    Error(_) -> Error(Nil)
    Ok(#(_data, next)) -> drain_body_loop(next, worker)
  }
}

type Handle

type Socket

type SocketError {
  PathExists(path: String)
  InvalidHost(host: String)
  Posix(reason: Atom)
}

@external(erlang, "gen_tcp", "accept")
fn accept(listen: Socket) -> Result(Socket, Atom)

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

@external(erlang, "fcgi_ffi", "listen_tcp")
fn do_listen_tcp(host: String, port: Int) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "listen_unix")
fn do_listen_unix(path: String) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "open_and_size")
fn open_and_size(path: String) -> Result(#(Handle, Int), FileError)

@external(erlang, "fcgi_ffi", "recv")
fn recv(
  socket: Socket,
  size: Int,
  timeout_ms: Int,
) -> Result(BitArray, SocketError)

@external(erlang, "fcgi_ffi", "send")
fn send_bits(socket: Socket, data: BitArray) -> Result(Nil, Nil)

@external(erlang, "fcgi_ffi", "send")
fn send_tree(socket: Socket, data: BytesTree) -> Result(Nil, Nil)

@external(erlang, "fcgi_ffi", "sendfile")
fn sendfile(
  handle: Handle,
  socket: Socket,
  offset: Int,
  bytes: Int,
) -> Result(Int, Nil)
