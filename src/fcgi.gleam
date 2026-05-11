//// FastCGI Responder server. Build a server with `new`, configure it with
//// `listen_path` and `max_body_size`, then call `supervised` to add it to
//// an OTP supervision tree, or `start` to run it directly.
////
//// CGI parameters from the upstream proxy are surfaced on the handler's
//// `Request` as follows:
////
//// - `REQUEST_METHOD`, `HTTPS`, `SERVER_NAME` / `SERVER_PORT` (or
////   `HTTP_HOST`), `PATH_INFO`, and `QUERY_STRING` populate the matching
////   `Request` fields.
//// - `CONTENT_TYPE` and `CONTENT_LENGTH` become the `content-type` and
////   `content-length` headers.
//// - `HTTP_*` variables become lowercased headers with underscores
////   replaced by dashes (e.g. `HTTP_X_FORWARDED_FOR` → `x-forwarded-for`).
//// - Every other variable is exposed as a `cgi-*` header (e.g.
////   `REMOTE_ADDR` → `cgi-remote-addr`, `SCRIPT_NAME` → `cgi-script-name`).

import fcgi/internal/connection
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/erlang/atom
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/otp/factory_supervisor
import gleam/otp/static_supervisor.{type Supervisor}
import gleam/otp/supervision
import gleam/result

const default_body_read_timeout_ms = 30_000

const default_max_body_size = 268_435_456

/// A handle to the streaming request body. Use `read_chunk` for the
/// continuation-passing API or `read_all` to buffer the whole body.
pub opaque type Body {
  Body(reader: connection.BodyReader)
}

/// What `read_chunk` produced.
pub type Read {
  /// A chunk of body bytes plus a `consume` continuation that returns
  /// the next chunk when called.
  Chunk(data: BitArray, consume: fn() -> Result(Read, ReadError))
  /// The body has been fully delivered.
  ReadingFinished
}

/// Why a `read_chunk` or `read_all` failed.
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
/// `max_body_size` setting is the upper bound; `read_chunk` returns
/// `BodyTooLarge` if the peer exceeds it, surfaced here as the
/// `Result` error.
///
/// The buffered length reflects what the upstream proxy actually sent,
/// not what `CONTENT_LENGTH` advertised. See `read_chunk` for the
/// enforcement caveat.
pub fn read_all(body: Body) -> Result(BytesTree, ReadError) {
  read_all_loop(read_step(body.reader), bytes_tree.new())
}

fn read_all_loop(
  read: Result(Read, ReadError),
  acc: BytesTree,
) -> Result(BytesTree, ReadError) {
  case read {
    Error(reason) -> Error(reason)
    Ok(ReadingFinished) -> Ok(acc)
    Ok(Chunk(data, consume)) ->
      read_all_loop(consume(), bytes_tree.append(acc, data))
  }
}

/// Read the next chunk from the request body. Returns `Chunk(data,
/// consume)` where `consume` produces the next chunk, or
/// `ReadingFinished` once the body is fully delivered. Blocks for up
/// to the configured `body_read_timeout` waiting for stdin records.
///
/// Each chunk reflects whatever the upstream proxy sent in the latest
/// stdin record (typically up to ~64 KB). Slice the returned `data`
/// further if you need smaller pieces.
///
/// `ReadingFinished` signals that the upstream proxy closed stdin, not
/// that the consumed byte count matches the `CONTENT_LENGTH` header.
/// The header is forwarded on the request but not enforced against the
/// stream. Handlers that need the guarantee should compare bytes
/// consumed against `request.get_header(req, "content-length")`.
pub fn read_chunk(body: Body) -> Result(Read, ReadError) {
  read_step(body.reader)
}

fn read_step(reader: connection.BodyReader) -> Result(Read, ReadError) {
  case reader() {
    Ok(connection.BodyMore(data, next)) ->
      Ok(Chunk(data:, consume: fn() { read_step(next) }))
    Ok(connection.BodyEnded) -> Ok(ReadingFinished)
    Error(connection.ConnectionLost) -> Error(ClientDisconnected)
    Error(connection.Timeout) -> Error(ReadTimeout)
    Error(connection.TooLarge) -> Error(BodyTooLarge)
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
pub type ResponseData =
  InternalResponseData

@internal
pub type InternalResponseData {
  Bytes(content: BytesTree)
  File(handle: connection.Handle, offset: Int, length: Int)
  Stream(producer: fn(StreamSender) -> Nil)
}

/// Handle passed to a `Stream` producer. Use `send_chunk` to emit body
/// bytes; each call writes one or more FastCGI `STDOUT` records on the
/// open connection.
pub opaque type StreamSender {
  StreamSender(emit: fn(BitArray) -> Result(Nil, Nil))
}

/// Build an in-memory response body. The whole `BytesTree` is sent in
/// one or more `STDOUT` records.
pub fn bytes(content: BytesTree) -> ResponseData {
  Bytes(content:)
}

/// Emit a chunk of body bytes from inside a `Stream` producer.
///
/// The chunk is wrapped in one or more FastCGI `STDOUT` records (each
/// record carries at most 65,535 bytes; larger chunks are split). Empty
/// chunks are a no-op and are not sent on the wire.
///
/// Returns `Ok(Nil)` when the chunk is written, or `Error(Nil)` when the
/// underlying socket write fails (for example, the upstream proxy has
/// disconnected). Producers should stop emitting on `Error` since the
/// response status line is already on the wire and the connection cannot
/// be salvaged.
pub fn send_chunk(sender: StreamSender, data: BitArray) -> Result(Nil, Nil) {
  let StreamSender(emit) = sender
  emit(data)
}

/// Open a file and return a response body that streams it via
/// `file:sendfile/5` when the response is sent.
///
/// The file is opened eagerly so the response holds an open file
/// descriptor: the path may be unlinked before the response is sent
/// (for example by a deferred temp-file cleanup) and streaming will
/// still succeed. The handle is closed by the connection actor after
/// streaming completes.
///
/// `offset` is the starting byte position in the file (must be `>= 0`);
/// an `offset` past end-of-file produces an empty body. `limit` is
/// `Some(n)` to send at most `n` bytes (must be `>= 0`), or `None` to
/// stream from `offset` through end-of-file.
///
/// If `send_file` returns `Ok(body)`, the caller must use `body` as the
/// response body; discarding it leaks the file descriptor until the
/// connection actor exits.
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
  use #(handle, total_size) <- result.try(
    connection.open_and_size(path) |> result.map_error(map_file_error(_, path)),
  )
  let max_length = option.unwrap(limit, total_size)
  let length = int.clamp(total_size - offset, min: 0, max: max_length)
  Ok(File(handle:, offset:, length:))
}

fn map_file_error(error: connection.FileError, path: String) -> FileError {
  case error {
    connection.NotFound -> FileNotFound(path)
    connection.AccessDenied -> FileAccessDenied(path)
    connection.IsDirectory -> FileIsDirectory(path)
    connection.Unknown(reason) -> FileOther(path:, reason:)
  }
}

/// Build a streaming response body. The server calls `producer(sender)`
/// after sending the response headers; each call to `send_chunk(sender,
/// data)` writes one or more `STDOUT` records to the upstream proxy.
/// Useful for long-lived responses such as Server-Sent Events. See
/// `send_chunk` for the calling convention and caveats.
pub fn stream(producer: fn(StreamSender) -> Nil) -> ResponseData {
  Stream(producer:)
}

pub type HasPath

pub type MissingPath

/// Server configuration produced by `new` and refined by `listen_path`,
/// `max_body_size`, and `body_read_timeout`. Pass it to `start` to begin
/// listening.
pub opaque type Builder(path) {
  Builder(
    handler: fn(Request(Body)) -> Response(ResponseData),
    path: Option(String),
    socket_mode: Option(Int),
    max_body_size: Int,
    body_read_timeout_ms: Int,
  )
}

/// Why the listener could not start.
pub type StartError {
  /// Wraps a failure from the underlying listener, such as bind or listen
  /// failures.
  ListenerError(reason: String)
  /// The requested Unix socket path already exists. Remove it before
  /// starting, or pick a different path.
  SocketPathExists(path: String)
  /// `max_body_size` was set to a negative value.
  InvalidMaxBodySize(bytes: Int)
  /// `body_read_timeout` was set to a negative value.
  InvalidBodyReadTimeout(milliseconds: Int)
}

type Handler =
  fn(Request(Nil), connection.BodyReader) -> Response(connection.ResponseData)

/// Set how long `read_chunk` waits for the next stdin record before
/// returning `Error(ReadTimeout)`. Must be `> 0`; `start` returns
/// `InvalidBodyReadTimeout(milliseconds)` otherwise.
///
/// Applies between successive chunk reads, not to the request as a
/// whole. Default: 30,000 ms.
pub fn body_read_timeout(
  builder: Builder(path),
  milliseconds: Int,
) -> Builder(path) {
  Builder(..builder, body_read_timeout_ms: milliseconds)
}

/// Set the Unix domain socket path the server listens on. The path must
/// not already exist; `start` returns `SocketPathExists(path)` if it does.
pub fn listen_path(builder: Builder(path), path: String) -> Builder(HasPath) {
  Builder(..builder, path: option.Some(path))
}

/// Set the Unix domain socket path and the file mode applied to it after
/// `bind(2)`. `mode` is a Unix permission bitfield (e.g. `0o660`, `0o666`).
/// `start` returns `ListenerError` if the chmod fails.
pub fn listen_path_with_mode(
  builder: Builder(path),
  path: String,
  mode: Int,
) -> Builder(HasPath) {
  Builder(..builder, path: option.Some(path), socket_mode: option.Some(mode))
}

/// Set the maximum body bytes the server will deliver to the handler in
/// total across all `read_chunk` calls. Must be `>= 0`; `start` returns
/// `InvalidMaxBodySize(bytes)` for negative values.
///
/// When the peer sends more than this many bytes, the next `read_chunk`
/// call returns `Error(BodyTooLarge)`. On a keep-alive connection the
/// server then closes the socket rather than continuing to the next
/// request, since the unread overflow has already corrupted the stream.
///
/// Default: 256 MiB.
///
/// Separately, FastCGI params (env vars plus cookies) are capped at a
/// fixed 64 KiB per request and not configurable here. Requests that
/// exceed the cap are rejected with an `Overloaded` end record before
/// the handler runs.
pub fn max_body_size(builder: Builder(path), bytes: Int) -> Builder(path) {
  Builder(..builder, max_body_size: bytes)
}

/// Build a new FastCGI server with the given handler. The Unix socket
/// path must be set with `listen_path` before calling `start`.
///
/// The handler is invoked once `Params` is fully received. The request
/// body is delivered incrementally via `Body`; call `read_chunk` (or
/// `read_all` for the buffered case) to consume it.
///
/// Default: 256 MiB max body, 30 s body read timeout.
pub fn new(
  handler: fn(Request(Body)) -> Response(ResponseData),
) -> Builder(MissingPath) {
  Builder(
    handler:,
    path: option.None,
    socket_mode: option.None,
    max_body_size: default_max_body_size,
    body_read_timeout_ms: default_body_read_timeout_ms,
  )
}

/// Start the server. Returns `actor.Started(Supervisor)` whose `pid` is
/// the root supervisor of the FastCGI tree, or `StartError` on failure.
pub fn start(
  builder: Builder(HasPath),
) -> Result(actor.Started(Supervisor), StartError) {
  use <- bool.guard(
    when: builder.max_body_size < 0,
    return: Error(InvalidMaxBodySize(builder.max_body_size)),
  )
  use <- bool.guard(
    when: builder.body_read_timeout_ms <= 0,
    return: Error(InvalidBodyReadTimeout(builder.body_read_timeout_ms)),
  )
  let assert option.Some(path) = builder.path
  start_server(
    path,
    builder.socket_mode,
    builder.max_body_size,
    builder.body_read_timeout_ms,
    wrap_handler(builder.handler),
  )
}

fn wrap_handler(
  user_handler: fn(Request(Body)) -> Response(ResponseData),
) -> fn(Request(Nil), connection.BodyReader) ->
  Response(connection.ResponseData) {
  fn(req: Request(Nil), reader: connection.BodyReader) {
    let response = user_handler(request.set_body(req, Body(reader:)))
    response.set_body(response, to_response_data(response.body))
  }
}

fn to_response_data(public: ResponseData) -> connection.ResponseData {
  case public {
    Bytes(content) -> connection.Bytes(content)
    File(handle, offset, length) -> connection.File(handle:, offset:, length:)
    Stream(producer) ->
      connection.Stream(fn(internal_sender) {
        let public_sender =
          StreamSender(emit: fn(data) {
            connection.send_chunk(internal_sender, data)
          })
        producer(public_sender)
      })
  }
}

fn start_server(
  path: String,
  socket_mode: Option(Int),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: Handler,
) -> Result(actor.Started(Supervisor), StartError) {
  use socket <- result.try(open_socket(path))
  use _ <- result.try(apply_socket_mode(socket, path, socket_mode))
  let factory_name = process.new_name(prefix: "fcgi_server_factory")
  let builder =
    static_supervisor.new(static_supervisor.RestForOne)
    |> static_supervisor.add(path_janitor_supervised(path))
    |> static_supervisor.add(connection_factory_supervised(factory_name))
    |> static_supervisor.add(acceptor_supervised(
      socket,
      factory_name,
      max_body_size,
      body_read_timeout_ms,
      handler,
    ))

  use started <- result.try(start_supervisor(builder, socket, path))
  handoff_socket(socket, path, started)
}

fn start_supervisor(
  builder: static_supervisor.Builder,
  socket: connection.Socket,
  path: String,
) -> Result(actor.Started(Supervisor), StartError) {
  case static_supervisor.start(builder) {
    Ok(started) -> Ok(started)
    Error(error) -> {
      cleanup_socket(socket, path)
      Error(ListenerError(describe_start_error(error)))
    }
  }
}

fn handoff_socket(
  socket: connection.Socket,
  path: String,
  started: actor.Started(Supervisor),
) -> Result(actor.Started(Supervisor), StartError) {
  case connection.controlling_process(socket, started.pid) {
    Ok(Nil) -> Ok(started)
    Error(error) -> {
      shutdown_supervisor(started.pid)
      cleanup_socket(socket, path)
      Error(ListenerError(
        "controlling_process failed: " <> describe_transport_error(error),
      ))
    }
  }
}

fn shutdown_supervisor(pid: process.Pid) -> Nil {
  process.unlink(pid)
  process.send_abnormal_exit(pid, atom.create("shutdown"))
}

fn open_socket(path: String) -> Result(connection.Socket, StartError) {
  case connection.listen(path) {
    Ok(socket) -> Ok(socket)
    Error(connection.PathExists(path)) -> Error(SocketPathExists(path))
    Error(error) ->
      Error(ListenerError("listen failed: " <> describe_transport_error(error)))
  }
}

fn apply_socket_mode(
  socket: connection.Socket,
  path: String,
  mode: Option(Int),
) -> Result(Nil, StartError) {
  case mode {
    option.None -> Ok(Nil)
    option.Some(value) -> chmod_socket_path(socket, path, value)
  }
}

fn chmod_socket_path(
  socket: connection.Socket,
  path: String,
  mode: Int,
) -> Result(Nil, StartError) {
  case connection.chmod_path(path, mode) {
    Ok(Nil) -> Ok(Nil)
    Error(error) -> {
      cleanup_socket(socket, path)
      Error(ListenerError("chmod failed: " <> describe_transport_error(error)))
    }
  }
}

fn cleanup_socket(socket: connection.Socket, path: String) -> Nil {
  connection.close_socket(socket)
  connection.delete_path(path)
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
    connection.delete_path(path)
    actor.stop()
  })
  |> actor.start
}

fn connection_factory_supervised(
  name: process.Name(factory_supervisor.Message(connection.Spec, Nil)),
) -> supervision.ChildSpecification(
  factory_supervisor.Supervisor(connection.Spec, Nil),
) {
  factory_supervisor.worker_child(connection.start)
  |> factory_supervisor.named(name)
  |> factory_supervisor.supervised
  |> supervision.restart(supervision.Transient)
}

fn acceptor_supervised(
  socket: connection.Socket,
  factory_name: process.Name(factory_supervisor.Message(connection.Spec, Nil)),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: Handler,
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
  listen_socket: connection.Socket,
  factory: factory_supervisor.Supervisor(connection.Spec, Nil),
  max_body_size: Int,
  body_read_timeout_ms: Int,
  handler: Handler,
) -> Nil {
  case connection.accept(listen_socket) {
    Error(connection.Posix(reason)) ->
      case atom.to_string(reason) {
        "closed" -> Nil
        other -> panic as { "fcgi acceptor: accept failed: " <> other }
      }
    Error(connection.PathExists(_)) -> Nil
    Ok(client) -> {
      let spec =
        connection.Spec(
          socket: client,
          max_body_size:,
          body_read_timeout_ms:,
          handler:,
        )
      case factory_supervisor.start_child(factory, spec) {
        Error(_) -> connection.close_socket(client)
        Ok(started) -> handoff_client(client, started.pid)
      }
      accept_loop(
        listen_socket,
        factory,
        max_body_size,
        body_read_timeout_ms,
        handler,
      )
    }
  }
}

fn handoff_client(client: connection.Socket, child_pid: process.Pid) -> Nil {
  case connection.controlling_process(client, child_pid) {
    Ok(Nil) -> Nil
    Error(_) -> {
      connection.close_socket(client)
      process.send_exit(child_pid)
    }
  }
}

fn describe_start_error(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "supervisor init timeout"
    actor.InitFailed(reason) -> reason
    actor.InitExited(_) -> "supervisor init exited"
  }
}

fn describe_transport_error(error: connection.SocketError) -> String {
  case error {
    connection.PathExists(path) -> "socket path already exists: " <> path
    connection.Posix(reason) -> atom.to_string(reason)
  }
}

fn format_start_error(error: StartError) -> String {
  case error {
    ListenerError(reason) -> reason
    SocketPathExists(path) -> "socket path already exists: " <> path
    InvalidMaxBodySize(bytes) ->
      "max_body_size must be non-negative; got " <> int.to_string(bytes)
    InvalidBodyReadTimeout(milliseconds) ->
      "body_read_timeout must be positive; got " <> int.to_string(milliseconds)
  }
}

/// Build a `supervision.ChildSpecification` so the server runs under an
/// OTP supervisor. The listener opens when the parent supervisor brings
/// the child up; when the parent terminates the child, the FastCGI
/// supervisor and its workers shut down, the listening socket is
/// released by the runtime, and the Unix socket path is unlinked.
pub fn supervised(
  builder: Builder(HasPath),
) -> supervision.ChildSpecification(Supervisor) {
  supervision.supervisor(fn() {
    start(builder)
    |> result.map_error(fn(error) {
      actor.InitFailed(format_start_error(error))
    })
  })
}
