//// Wisp adapter for the FastCGI server. Mirrors `wisp/wisp_mist`.

import exception
import fcgi
import fcgi/internal/file
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/http/request.{type Request as HttpRequest}
import gleam/http/response.{type Response as HttpResponse}
import gleam/int
import gleam/option.{type Option}
import gleam/result
import gleam/string
import wisp

const file_stream_chunk_size: Int = 65_535

// This module is the single point of contact with `wisp/internal`, which
// Wisp does not expose as public API but is required to build a
// `wisp.Connection` from a custom body reader.
import wisp/internal

/// Adapt a Wisp handler into the FCGI handler shape so callers can compose
/// with `fcgi.new`.
pub fn handler(
  handler: fn(wisp.Request) -> wisp.Response,
  secret_key_base: String,
) -> fn(HttpRequest(fcgi.Connection)) -> HttpResponse(fcgi.ResponseData) {
  fn(request: HttpRequest(_)) {
    let connection =
      internal.make_connection(body_reader(request), secret_key_base)
    let request = request.set_body(request, connection)

    use <- exception.defer(fn() {
      case wisp.delete_temporary_files(request) {
        Ok(Nil) -> Nil
        Error(error) ->
          wisp.log_error(
            "failed to delete wisp temporary files: " <> string.inspect(error),
          )
      }
    })

    handler(request)
    |> map_response
  }
}

fn body_reader(request: HttpRequest(fcgi.Connection)) -> internal.Reader {
  let stream = fcgi.stream(request)
  fn(size) { wrap_chunk(stream(size)) }
}

fn wrap_chunk(chunk: Result(fcgi.Chunk, Nil)) -> Result(internal.Read, Nil) {
  result.map(chunk, fn(chunk) {
    case chunk {
      fcgi.Done -> internal.ReadingFinished
      fcgi.Chunk(data, consume) ->
        internal.Chunk(data, fn(size) { wrap_chunk(consume(size)) })
    }
  })
}

@internal
pub fn map_response(
  response: wisp.Response,
) -> HttpResponse(fcgi.ResponseData) {
  case response.body {
    wisp.Text(text) ->
      response.set_body(response, fcgi.Bytes(bytes_tree.from_string(text)))
    wisp.Bytes(bytes) -> response.set_body(response, fcgi.Bytes(bytes))
    wisp.File(path:, offset:, limit:) ->
      map_file_body(response, path, offset, limit)
  }
}

fn map_file_body(
  response: wisp.Response,
  path: String,
  offset: Int,
  limit: Option(Int),
) -> HttpResponse(fcgi.ResponseData) {
  case file.open(path) {
    Ok(handle) -> {
      let producer = fn(sender) {
        stream_handle(sender, handle, int.max(0, offset), limit)
        file.close(handle)
      }
      response.set_body(response, fcgi.Stream(producer))
    }
    Error(error) -> {
      wisp.log_error("wisp_fcgi: " <> string.inspect(error))
      file_error_response()
    }
  }
}

fn stream_handle(
  sender: fcgi.StreamSender,
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
      emit_and_continue(sender, handle, offset, to_read, remaining, data)
  }
}

fn emit_and_continue(
  sender: fcgi.StreamSender,
  handle: file.Handle,
  offset: Int,
  to_read: Int,
  remaining: Option(Int),
  data: BitArray,
) -> Nil {
  let bytes_read = bit_array.byte_size(data)
  use <- bool.guard(when: bytes_read == 0, return: Nil)
  case fcgi.send_chunk(sender, data) {
    Error(_) -> Nil
    Ok(_) -> {
      use <- bool.guard(when: bytes_read < to_read, return: Nil)
      stream_handle(
        sender,
        handle,
        offset + bytes_read,
        option.map(remaining, fn(n) { n - bytes_read }),
      )
    }
  }
}

fn file_error_response() -> HttpResponse(fcgi.ResponseData) {
  response.new(500)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(
    fcgi.Bytes(bytes_tree.from_string("could not open response file")),
  )
}
