//// Wisp adapter for the FastCGI server. Mirrors `wisp/wisp_mist`.
////
////
//// ## Wisp version compatibility
////
//// This adapter imports `wisp/internal` to build a `wisp.Connection`
//// from a custom body reader. Wisp does not expose those symbols as
//// public API, and this is the same integration point Wisp's own
//// `wisp/wisp_mist` adapter uses, so in practice the surface is
//// stable across Wisp's minor releases. It is, however, not
//// semver-stable: a future Wisp minor release could rename or
//// reshape `wisp/internal` without it being treated as a breaking
//// change.
////
//// The `gleam.toml` for this package allows the full
//// `>= 2.2.2 and < 3.0.0` Wisp range. If you adopt a new Wisp minor
//// version before this package is updated, pin Wisp to the version
//// you have validated against in your own `gleam.toml` and treat any
//// adapter compile failure on a Wisp upgrade as a signal to wait
//// for an `fcgi` release that has been tested against it.

import exception
import fcgi
import fcgi/internal/connection
import gleam/bit_array
import gleam/bytes_tree
import gleam/http/request.{type Request as HttpRequest}
import gleam/http/response.{type Response as HttpResponse}
import gleam/int
import gleam/option
import gleam/string
import wisp
import wisp/internal

/// Adapt a Wisp handler into the FCGI handler shape so callers can compose
/// with `fcgi.new`.
pub fn handler(
  handler: fn(wisp.Request) -> wisp.Response,
  secret_key_base: String,
) -> fn(HttpRequest(fcgi.Connection)) -> HttpResponse(fcgi.ResponseData) {
  fn(req: HttpRequest(_)) {
    let connection = internal.make_connection(body_reader(req), secret_key_base)
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

fn body_reader(req: HttpRequest(fcgi.Connection)) -> internal.Reader {
  let body = fcgi.body(req)
  reader_from(body, total: bit_array.byte_size(body), offset: 0)
}

fn reader_from(
  body: BitArray,
  total total: Int,
  offset offset: Int,
) -> internal.Reader {
  fn(size) {
    case size <= 0, offset >= total {
      True, _ -> Error(Nil)
      _, True -> Ok(internal.ReadingFinished)
      _, False -> {
        let take = int.min(size, total - offset)
        let assert Ok(slice) = bit_array.slice(body, offset, take)
        Ok(internal.Chunk(
          slice,
          reader_from(body, total:, offset: offset + take),
        ))
      }
    }
  }
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
  limit: option.Option(Int),
) -> HttpResponse(fcgi.ResponseData) {
  case connection.open_file(path) {
    Error(error) -> {
      wisp.log_error("wisp_fcgi: " <> string.inspect(error))
      response.new(500)
      |> response.set_header("content-type", "text/plain; charset=utf-8")
      |> response.set_body(
        fcgi.Bytes(bytes_tree.from_string("could not open response file")),
      )
    }
    Ok(handle) ->
      response.set_body(
        response,
        fcgi.Stream(fn(sender) {
          connection.stream_file(
            handle,
            offset:,
            remaining: limit,
            emit: fn(data) { fcgi.send_chunk(sender, data) },
          )
          connection.close_file(handle)
        }),
      )
  }
}
