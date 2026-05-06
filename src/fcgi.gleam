//// FastCGI Responder server. Build a server with `new`, configure it with
//// `listen_path` and `max_body_size`, then call `start` to run it directly
//// or `supervised` to run it under an OTP supervisor.

import fcgi/internal/connection
import fcgi/internal/server
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/otp/supervision

/// Server configuration produced by `new` and refined by `listen_path`
/// and `max_body_size`. Pass it to `start` to begin listening.
pub opaque type Builder(in, out) {
  Builder(
    handler: fn(Request(in)) -> Response(out),
    path: String,
    max_body_size: Int,
  )
}

/// Why the listener could not start.
pub type StartError {
  /// Wraps a failure from the underlying listener, such as bind or listen
  /// failures. `reason` is a human-readable string describing the underlying
  /// error.
  ListenerError(reason: String)
  /// The requested Unix socket path already exists. Remove it before
  /// starting, or pick a different path.
  SocketPathExists(path: String)
  /// `max_body_size` was set to a negative value. Use `0` to reject all
  /// non-empty bodies, or any positive value for a real limit.
  InvalidMaxBodySize(bytes: Int)
}

/// A running server. Use `stop` to shut the server down.
pub opaque type Started {
  Started(server: server.Server)
}

/// Set the Unix domain socket path the server listens on. The path must
/// not already exist; `start` returns `SocketPathExists(path)` if it does.
pub fn listen_path(
  builder: Builder(in, out),
  path: String,
) -> Builder(in, out) {
  Builder(..builder, path:)
}

/// Set the maximum buffered request body in bytes. Must be `>= 0`;
/// `start` returns `InvalidMaxBodySize(bytes)` for negative values.
/// Default: 10 MiB.
pub fn max_body_size(
  builder: Builder(in, out),
  bytes: Int,
) -> Builder(in, out) {
  Builder(..builder, max_body_size: bytes)
}

/// Build a new FastCGI server with the given handler. The Unix socket
/// path must be set with `listen_path` before calling `start`. Default:
/// 10 MiB max body.
pub fn new(handler: fn(Request(in)) -> Response(out)) -> Builder(in, out) {
  Builder(handler:, path: "", max_body_size: 10 * 1024 * 1024)
}

/// Start the server. Returns `Started`, or `StartError` on failure.
pub fn start(
  builder: Builder(Connection, ResponseData),
) -> Result(Started, StartError) {
  use <- bool.guard(
    when: builder.path == "",
    return: Error(ListenerError("listen_path must be called with a socket path")),
  )
  use <- bool.guard(
    when: builder.max_body_size < 0,
    return: Error(InvalidMaxBodySize(builder.max_body_size)),
  )
  let template =
    server.SpecTemplate(
      max_body_size: builder.max_body_size,
      handler: adapt_handler(builder.handler),
    )
  case server.start(builder.path, template) {
    Error(server.ListenerError(reason)) -> Error(ListenerError(reason))
    Error(server.SocketPathExists(path)) -> Error(SocketPathExists(path))
    Ok(srv) -> Ok(Started(server: srv))
  }
}

/// Stop the running server. Closes the listening socket, terminates all
/// open connection actors, and removes the socket path.
pub fn stop(started: Started) -> Nil {
  let Started(srv) = started
  server.stop(srv)
}

/// Build a `supervision.ChildSpecification` so the server can run under an
/// OTP supervisor instead of being started directly with `start`. The
/// returned spec wraps an internal supervisor; the listener opens when
/// the parent supervisor brings the child up, and the running `Started`
/// is exposed as the spec's `data` field for callers who start the spec
/// themselves.
///
/// When the parent supervisor terminates the child, the FastCGI
/// supervisor and its workers shut down, the listening socket is
/// released by the runtime, and the Unix socket path is unlinked.
pub fn supervised(
  builder: Builder(Connection, ResponseData),
) -> supervision.ChildSpecification(Started) {
  supervision.supervisor(fn() {
    case start(builder) {
      Error(ListenerError(reason)) -> Error(actor.InitFailed(reason))
      Error(SocketPathExists(path)) ->
        Error(actor.InitFailed("socket path already exists: " <> path))
      Error(InvalidMaxBodySize(bytes)) ->
        Error(actor.InitFailed(
          "max_body_size must be non-negative; got " <> int.to_string(bytes),
        ))
      Ok(started) ->
        Ok(actor.Started(pid: started.server.supervisor_pid, data: started))
    }
  })
}

fn adapt_handler(
  user_handler: fn(Request(Connection)) -> Response(ResponseData),
) -> fn(Request(connection.Connection)) -> Response(connection.ResponseData) {
  fn(req: Request(connection.Connection)) {
    let connection.Connection(body) = req.body
    let public_req = request.set_body(req, Connection(body))
    let public_response = user_handler(public_req)
    response.set_body(public_response, to_response_data(public_response.body))
  }
}

fn to_response_data(public: ResponseData) -> connection.ResponseData {
  case public {
    Bytes(content) -> connection.Bytes(content)
    File(path, offset, limit) -> connection.File(path, offset, limit)
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

/// A buffered FastCGI request body. Read it with `read_body`.
pub opaque type Connection {
  Connection(body: BitArray)
}

/// Read the buffered body of a request.
/// Returns `Error(Nil)` if larger than `max_size`.
pub fn read_body(
  req: Request(Connection),
  max_size max_size: Int,
) -> Result(BitArray, Nil) {
  let Connection(body) = req.body
  case bit_array.byte_size(body) > max_size {
    True -> Error(Nil)
    False -> Ok(body)
  }
}

/// Direct access to the buffered request body for adapters that need a
/// chunked-reader shape (such as `fcgi/wisp_fcgi`). Public callers should
/// use `read_body` instead.
@internal
pub fn body(req: Request(Connection)) -> BitArray {
  let Connection(body) = req.body
  body
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
  /// `offset` is negative, or `limit` is `Some(n)` with `n < 0`. The filesystem is not touched in this case.
  InvalidRange(offset: Int, limit: Option(Int))
}

/// Validate a file and produce a `File` `ResponseData`.
///
/// `offset` is the starting byte position in the file. It must be `>= 0`.
/// An `offset` past end-of-file results in an empty body at request time.
///
/// `limit` is `Some(n)` to send at most `n` bytes (must be `>= 0`), or
/// `None` to stream from `offset` through end-of-file. `Some(0)` produces
/// an empty body.
///
/// Returns `FileError` if the file is missing, inaccessible, a directory,
/// or if `offset` or `limit` is negative.
///
/// If the file is opened successfully but a later read fails mid-stream
/// (for example, the file is truncated or unlinked while being sent),
/// the response body is silently truncated at the failure point. The
/// response headers and `END_REQUEST` record are still sent, so the
/// upstream proxy will deliver whatever bytes were written before the
/// failure. There is no way to surface a 500 at that point because the
/// status line has already been written to the wire.
pub fn send_file(
  path path: String,
  offset offset: Int,
  limit limit: Option(Int),
) -> Result(ResponseData, FileError) {
  use <- bool.guard(
    when: offset < 0 || option.unwrap(limit, 0) < 0,
    return: Error(InvalidRange(offset:, limit:)),
  )
  case connection.validate_file(path) {
    Ok(_) -> Ok(File(path:, offset:, limit:))
    Error(error) -> Error(map_file_error(error, path))
  }
}

fn map_file_error(error: connection.FileError, path: String) -> FileError {
  case error {
    connection.NotFound -> FileNotFound(path)
    connection.AccessDenied -> FileAccessDenied(path)
    connection.IsDirectory -> FileIsDirectory(path)
    connection.Unknown(reason) -> FileOther(path:, reason:)
  }
}

/// What the server should send back as a response body.
pub type ResponseData {
  /// An in-memory body. The whole `BytesTree` is sent in
  /// one or more `STDOUT` records.
  Bytes(content: BytesTree)
  /// A file body streamed from disk. `offset` is the starting byte
  /// position; `limit` is `Some(n)` to send at most `n` bytes, or
  /// `None` to send to end-of-file.
  ///
  /// Use `send_file` to build this variant. `send_file` validates the
  /// path, checks read access, and rejects negative `offset` or
  /// `limit`. Direct construction skips all of that: an invalid path
  /// surfaces later as a generic 500 response, and a negative `offset`
  /// or `limit` produces an empty body without an error. Direct
  /// construction is advanced usage and the caller owns the
  /// invariants `send_file` would otherwise enforce.
  File(path: String, offset: Int, limit: Option(Int))
  /// A body produced incrementally by a callback. The
  /// server calls `producer(sender)` after sending the response headers;
  /// each call to `send_chunk(sender, data)` writes one or more `STDOUT`
  /// records to the upstream proxy. Useful for long-lived responses such
  /// as Server-Sent Events. See `send_chunk` for the calling convention
  /// and caveats.
  Stream(producer: fn(StreamSender) -> Nil)
}

/// Handle passed to a `Stream` producer. Use `send_chunk` to emit body
/// bytes; each call writes one or more FastCGI `STDOUT` records on the
/// open connection.
pub opaque type StreamSender {
  StreamSender(emit: fn(BitArray) -> Result(Nil, Nil))
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
