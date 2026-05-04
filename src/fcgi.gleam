//// FastCGI Responder server. Build a server with `new`, configure it with
//// `bind`, `port`, and `max_body_size`, then call `start`.

import exception
import fcgi/internal/file
import fcgi/internal/handler
import fcgi/internal/params
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/dict
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/option.{type Option}
import gleam/result
import gleam/string
import glisten

const file_stream_chunk_size: Int = 65_535

/// A buffered FastCGI request body. Read it with `read_body` or `stream`.
pub opaque type Connection {
  Connection(body: BitArray)
}

/// A single read from a body stream returned by `stream`.
pub type Chunk {
  /// Indicates the body has been fully consumed; no more reads will
  /// yield data.
  Done
  /// Carries the next slice of body bytes in `data`.
  /// Call `consume(size)` to request the next chunk of up to `size` bytes;
  /// it returns the next `Chunk` or `Error(Nil)`.
  Chunk(data: BitArray, consume: fn(Int) -> Result(Chunk, Nil))
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

/// What the server should send back as a response body.
pub type ResponseData {
  /// An in-memory body. The whole `BytesTree` is sent in
  /// one or more `STDOUT` records.
  Bytes(content: BytesTree)
  /// A file body streamed from disk. `offset`
  /// is the starting byte position; `limit` is `Some(n)` to send at most
  /// `n` bytes, or `None` to send to end-of-file. Construct via `send_file`,
  /// which validates the path and range up front.
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

/// Why the listener could not start.
///
/// - `ListenerError(reason)`: wraps a failure from the underlying listener,
///   such as bind or listen failures. `reason` is a human-readable string
///   describing the underlying error.
pub type StartError {
  ListenerError(reason: String)
}

/// A running listener. Use `bound_port` to find the bound port.
pub opaque type Started {
  Started(bound_port: Int)
}

/// The port the listener bound to (resolves OS-assigned ports for `port(0)`).
pub fn bound_port(started: Started) -> Int {
  let Started(port) = started
  port
}

/// Server configuration produced by `new` and refined by `bind`, `port`,
/// `max_body_size`, and `max_params_size`. Pass it to `start` to begin
/// listening.
pub opaque type Builder(in, out) {
  Builder(
    handler: fn(Request(in)) -> Response(out),
    interface: String,
    port: Int,
    max_body_size: Int,
    max_params_size: Int,
  )
}

/// Build a new FastCGI server with the given handler.
/// Defaults: `127.0.0.1:9000`, 10 MiB max body, 256 KiB max params.
pub fn new(handler: fn(Request(in)) -> Response(out)) -> Builder(in, out) {
  Builder(
    handler:,
    interface: "127.0.0.1",
    port: 9000,
    max_body_size: 10 * 1024 * 1024,
    max_params_size: 256 * 1024,
  )
}

/// Set the listen interface. Default: `127.0.0.1`.
pub fn bind(builder: Builder(in, out), interface: String) -> Builder(in, out) {
  Builder(..builder, interface:)
}

/// Set the listen port. Default: `9000`.
/// Use `0` to let the OS assign a free port.
pub fn port(builder: Builder(in, out), port: Int) -> Builder(in, out) {
  Builder(..builder, port:)
}

/// Set the maximum buffered request body in bytes. Default: 10 MiB.
pub fn max_body_size(
  builder: Builder(in, out),
  bytes: Int,
) -> Builder(in, out) {
  Builder(..builder, max_body_size: bytes)
}

/// Set the maximum buffered FastCGI params block in bytes. Default: 256 KiB.
/// When the upstream sends more `PARAMS` than this, the server replies with
/// `FCGI_OVERLOADED` and the handler is not invoked.
pub fn max_params_size(
  builder: Builder(in, out),
  bytes: Int,
) -> Builder(in, out) {
  Builder(..builder, max_params_size: bytes)
}

/// Start the listener. Returns `Started` with the bound port populated,
/// or `StartError` on failure.
pub fn start(
  builder: Builder(Connection, ResponseData),
) -> Result(Started, StartError) {
  let listener_name = process.new_name("fcgi_listener")
  let on_init = fn(_conn) { #(handler.new(), option.None) }
  let on_message = fn(state, message, conn) {
    case message {
      glisten.Packet(bytes) -> handle_packet(bytes, state, conn, builder)
      glisten.User(_) -> glisten.continue(state)
    }
  }

  use _ <- result.try(
    glisten.new(on_init, on_message)
    |> glisten.bind(builder.interface)
    |> glisten.with_listener_name(listener_name)
    |> glisten.start(builder.port)
    |> result.map_error(fn(error) { ListenerError(string.inspect(error)) }),
  )

  let info = glisten.get_server_info(listener_name, 5000)
  Ok(Started(bound_port: info.port))
}

fn handle_packet(
  bytes: BitArray,
  state: handler.State,
  conn: glisten.Connection(user),
  builder: Builder(Connection, ResponseData),
) -> glisten.Next(handler.State, glisten.Message(user)) {
  drive_outcome(
    handler.feed(
      state,
      bytes:,
      max_body_size: builder.max_body_size,
      max_params_size: builder.max_params_size,
    ),
    conn,
    builder,
  )
}

fn drive_outcome(
  outcome: handler.Outcome,
  conn: glisten.Connection(user),
  builder: Builder(Connection, ResponseData),
) -> glisten.Next(handler.State, glisten.Message(user)) {
  let _ = send_if_nonempty(conn, outcome.outgoing)
  case outcome.action {
    handler.WaitForMore -> glisten.continue(outcome.state)
    handler.CloseConnection -> glisten.stop()
    handler.ReadyForHandler(id, params_bytes, body, keep_conn) -> {
      run_handler(id, params_bytes, body, conn, builder)
      case keep_conn {
        True ->
          drive_outcome(
            handler.feed(
              outcome.state,
              bytes: <<>>,
              max_body_size: builder.max_body_size,
              max_params_size: builder.max_params_size,
            ),
            conn,
            builder,
          )
        False -> glisten.stop()
      }
    }
  }
}

fn run_handler(
  request_id: Int,
  params_bytes: BitArray,
  body: BitArray,
  conn: glisten.Connection(user),
  builder: Builder(Connection, ResponseData),
) -> Nil {
  let response = case build_request(params_bytes, body) {
    Ok(req) -> safe_invoke_handler(builder.handler, req)
    Error(error) -> error_response(400, build_error_message(error))
  }
  let #(final_response, prepared) = prepare_or_fallback(response)
  let _ =
    send_if_nonempty(
      conn,
      handler.encode_response_header(request_id, final_response),
    )
  send_prepared_body(conn, request_id, prepared)
  let _ = send_if_nonempty(conn, handler.encode_response_terminator(request_id))
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

type PreparedResponseBody {
  PreparedBytes(data: BytesTree)
  PreparedFile(handle: file.Handle, offset: Int, limit: Option(Int))
  PreparedStream(producer: fn(StreamSender) -> Nil)
}

fn prepare_or_fallback(
  response: Response(ResponseData),
) -> #(Response(ResponseData), PreparedResponseBody) {
  case prepare_response_body(response.body) {
    Ok(prepared) -> #(response, prepared)
    Error(_) -> {
      let fallback = error_response(500, "could not open response file")
      let assert Bytes(tree) = fallback.body
      #(fallback, PreparedBytes(tree))
    }
  }
}

fn prepare_response_body(
  body: ResponseData,
) -> Result(PreparedResponseBody, file.Error) {
  case body {
    Bytes(tree) -> Ok(PreparedBytes(tree))
    File(path, offset, limit) ->
      file.open(path)
      |> result.map(fn(handle) { PreparedFile(handle, offset, limit) })
    Stream(producer) -> Ok(PreparedStream(producer))
  }
}

type BuildError {
  MalformedParams(reason: protocol.ParseFailure)
  InvalidRequest(reason: params.Error)
  InvalidContentLength(value: String)
  ContentLengthMismatch(declared: Int, actual: Int)
}

fn build_error_message(error: BuildError) -> String {
  case error {
    MalformedParams(_) -> "malformed FastCGI parameters"
    InvalidRequest(params.MissingMethod) -> "missing REQUEST_METHOD parameter"
    InvalidRequest(params.InvalidMethod(method)) ->
      "invalid REQUEST_METHOD: " <> method
    InvalidContentLength(value) -> "invalid CONTENT_LENGTH: " <> value
    ContentLengthMismatch(declared, actual) ->
      "CONTENT_LENGTH "
      <> int.to_string(declared)
      <> " does not match received body of "
      <> int.to_string(actual)
      <> " bytes"
  }
}

fn send_prepared_body(
  conn: glisten.Connection(user),
  request_id: Int,
  body: PreparedResponseBody,
) -> Nil {
  case body {
    PreparedBytes(data) -> {
      let _ =
        send_if_nonempty(
          conn,
          handler.encode_response_body_tree(request_id, data),
        )
      Nil
    }
    PreparedFile(handle, offset, limit) -> {
      let normalized_offset = int.max(0, offset)
      stream_file_loop(conn, request_id, handle, normalized_offset, limit)
      file.close(handle)
    }
    PreparedStream(producer) -> run_stream_producer(conn, request_id, producer)
  }
}

fn run_stream_producer(
  conn: glisten.Connection(user),
  request_id: Int,
  producer: fn(StreamSender) -> Nil,
) -> Nil {
  let sender =
    StreamSender(emit: fn(data) {
      send_if_nonempty(
        conn,
        handler.encode_response_body_chunk(request_id, data),
      )
    })
  producer(sender)
}

fn stream_file_loop(
  conn: glisten.Connection(user),
  request_id: Int,
  handle: file.Handle,
  offset: Int,
  remaining: Option(Int),
) -> Nil {
  let to_read = case remaining {
    option.None -> file_stream_chunk_size
    option.Some(n) -> int.min(n, file_stream_chunk_size)
  }
  use <- bool.guard(when: to_read <= 0, return: Nil)
  case file.pread(handle, offset, to_read) {
    Error(_) -> Nil
    Ok(data) ->
      send_chunk_and_continue(
        conn,
        request_id,
        handle,
        offset,
        to_read,
        remaining,
        data,
      )
  }
}

fn send_chunk_and_continue(
  conn: glisten.Connection(user),
  request_id: Int,
  handle: file.Handle,
  offset: Int,
  to_read: Int,
  remaining: Option(Int),
  data: BitArray,
) -> Nil {
  let bytes_read = bit_array.byte_size(data)
  use <- bool.guard(when: bytes_read == 0, return: Nil)
  case
    send_if_nonempty(conn, handler.encode_response_body_chunk(request_id, data))
  {
    Error(_) -> Nil
    Ok(_) -> {
      use <- bool.guard(when: bytes_read < to_read, return: Nil)
      stream_file_loop(
        conn,
        request_id,
        handle,
        offset + bytes_read,
        option.map(remaining, fn(n) { n - bytes_read }),
      )
    }
  }
}

fn build_request(
  params_bytes: BitArray,
  body: BitArray,
) -> Result(Request(Connection), BuildError) {
  use pairs <- result.try(
    protocol.parse_name_value_pairs(params_bytes)
    |> result.map_error(MalformedParams),
  )
  let env = dict.from_list(pairs)
  use _ <- result.try(check_content_length(env, body))
  params.to_http_request(env, Connection(body))
  |> result.map_error(InvalidRequest)
}

fn check_content_length(
  env: dict.Dict(String, String),
  body: BitArray,
) -> Result(Nil, BuildError) {
  case dict.get(env, "CONTENT_LENGTH") {
    Error(_) -> Ok(Nil)
    Ok("") -> Ok(Nil)
    Ok(raw) ->
      case int.parse(raw) {
        Error(_) -> Error(InvalidContentLength(raw))
        Ok(declared) if declared < 0 -> Error(InvalidContentLength(raw))
        Ok(declared) -> {
          let actual = bit_array.byte_size(body)
          case declared == actual {
            True -> Ok(Nil)
            False -> Error(ContentLengthMismatch(declared:, actual:))
          }
        }
      }
  }
}

fn send_if_nonempty(
  conn: glisten.Connection(user),
  bytes: BytesTree,
) -> Result(Nil, Nil) {
  case bytes_tree.byte_size(bytes) {
    0 -> Ok(Nil)
    _ ->
      glisten.send(conn, bytes)
      |> result.replace_error(Nil)
  }
}

fn error_response(status: Int, message: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(Bytes(bytes_tree.from_string(message)))
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

/// Returns a chunk-reader over the buffered request body. The body is
/// already in memory; calls slice it without performing any I/O.
/// Each call yields up to the requested size in bytes, advancing through
/// the body until exhausted. Non-positive sizes return `Error(Nil)` since
/// a zero-or-negative request cannot make progress.
pub fn stream(req: Request(Connection)) -> fn(Int) -> Result(Chunk, Nil) {
  let Connection(body) = req.body
  stream_loop(body, bit_array.byte_size(body), 0)
}

fn stream_loop(
  body: BitArray,
  total: Int,
  offset: Int,
) -> fn(Int) -> Result(Chunk, Nil) {
  fn(size) {
    case size <= 0, offset >= total {
      True, _ -> Error(Nil)
      _, True -> Ok(Done)
      _, False -> {
        let remaining = total - offset
        let take = int.min(size, remaining)
        let assert Ok(slice) = bit_array.slice(body, offset, take)
        Ok(Chunk(data: slice, consume: stream_loop(body, total, offset + take)))
      }
    }
  }
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
    when: !valid_range(offset, limit),
    return: Error(InvalidRange(offset:, limit:)),
  )
  case file.validate_file(path) {
    Ok(_) -> Ok(File(path:, offset:, limit:))
    Error(error) -> Error(map_file_error(error, path))
  }
}

fn valid_range(offset: Int, limit: Option(Int)) -> Bool {
  case offset >= 0, limit {
    True, option.None -> True
    True, option.Some(n) -> n >= 0
    False, _ -> False
  }
}

fn map_file_error(error: file.Error, path: String) -> FileError {
  case error {
    file.NotFound -> FileNotFound(path)
    file.AccessDenied -> FileAccessDenied(path)
    file.IsDirectory -> FileIsDirectory(path)
    file.Unknown(reason) -> FileOther(path:, reason:)
  }
}
