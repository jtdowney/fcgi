//// Wisp adapter for the FastCGI server. Mirrors `wisp/wisp_mist`.
////
//// ## Wisp version compatibility
////
//// This adapter imports `wisp/internal` to build a `wisp.Connection`
//// from a custom body reader. It is, however, not semver-stable: a
//// future Wisp minor release could rename or reshape `wisp/internal`
//// without it being treated as a breaking change.

import exception
import fcgi
import gleam/bytes_tree
import gleam/http/request.{type Request as HttpRequest}
import gleam/http/response.{type Response as HttpResponse}
import gleam/result
import gleam/string
import wisp
import wisp/internal as wisp_internal

/// Adapt a Wisp handler into the FCGI handler shape so callers can compose
/// with `fcgi.new`.
pub fn handler(
  handler: fn(wisp.Request) -> wisp.Response,
  secret_key_base: String,
) -> fn(HttpRequest(fcgi.Body)) -> HttpResponse(fcgi.ResponseData) {
  fn(req: HttpRequest(fcgi.Body)) {
    let reader = fn(_size) { wrap_chunk(fcgi.read_chunk(req.body)) }
    let connection = wisp_internal.make_connection(reader, secret_key_base)
    let req = request.set_body(req, connection)

    use <- exception.defer(fn() {
      case wisp.delete_temporary_files(req) {
        Ok(Nil) -> Nil
        Error(error) ->
          wisp.log_error(
            "failed to delete wisp temporary files: " <> string.inspect(error),
          )
      }
    })

    handler(req)
    |> map_response
  }
}

fn wrap_chunk(
  chunk: Result(fcgi.Read, fcgi.ReadError),
) -> Result(wisp_internal.Read, Nil) {
  chunk
  |> result.replace_error(Nil)
  |> result.map(fn(chunk) {
    case chunk {
      fcgi.EndOfBody -> wisp_internal.ReadingFinished
      fcgi.Chunk(data, consume) ->
        wisp_internal.Chunk(data, fn(_size) { wrap_chunk(consume()) })
    }
  })
}

pub fn map_response(
  response: wisp.Response,
) -> HttpResponse(fcgi.ResponseData) {
  case response.body {
    wisp.Text(text) ->
      response.set_body(response, fcgi.bytes(bytes_tree.from_string(text)))
    wisp.Bytes(content) -> response.set_body(response, fcgi.bytes(content))
    wisp.File(path:, offset:, limit:) ->
      case fcgi.send_file(path:, offset:, limit:) {
        Ok(file_data) -> response.set_body(response, file_data)
        Error(error) -> {
          wisp.log_error("wisp_fcgi: " <> string.inspect(error))
          response.new(500)
          |> response.set_header("content-type", "text/plain; charset=utf-8")
          |> response.set_body(
            fcgi.bytes(bytes_tree.from_string("could not open response file")),
          )
        }
      }
  }
}
