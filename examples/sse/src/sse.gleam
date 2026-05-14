//// Server-Sent Events example for fcgi, intended to be served behind a
//// Caddy reverse proxy. The `/events` endpoint streams ten timestamped
//// events, one per second, then closes the connection. The `/` endpoint
//// returns a small HTML page that subscribes to the stream via the
//// browser's `EventSource` API.

import fcgi
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/io
import gleam/option

const event_count = 10

const event_interval_ms = 1000

const socket_path = "/tmp/fcgi_sse.sock"

pub fn main() -> Nil {
  let assert Ok(_) =
    handle_request
    |> fcgi.new
    |> fcgi.listen_unix(socket_path)
    |> fcgi.start

  io.println("Listening on unix:" <> socket_path)
  process.sleep_forever()
}

fn handle_request(
  req: Request(fcgi.BodyReader),
  _ctx: fcgi.Context,
) -> Response(fcgi.ResponseData) {
  case request.path_segments(req) {
    [] -> serve_static_file("static/index.html", "text/html; charset=utf-8")
    ["static", "app.js"] ->
      serve_static_file(
        "static/app.js",
        "application/javascript; charset=utf-8",
      )
    ["events"] -> events_stream()
    _ -> not_found()
  }
}

fn serve_static_file(
  path: String,
  content_type: String,
) -> Response(fcgi.ResponseData) {
  case fcgi.send_file(path:, offset: 0, limit: option.None) {
    Ok(body) ->
      response.new(200)
      |> response.set_header("content-type", content_type)
      |> response.set_body(body)
    Error(_) -> not_found()
  }
}

fn events_stream() -> Response(fcgi.ResponseData) {
  response.new(200)
  |> response.set_header("content-type", "text/event-stream")
  |> response.set_header("cache-control", "no-cache")
  |> response.set_header("x-accel-buffering", "no")
  |> response.set_body(fcgi.stream(emit_events(_, 1)))
}

fn emit_events(sender: fcgi.StreamSender, n: Int) -> Nil {
  use <- bool.guard(when: n > event_count, return: Nil)
  let payload =
    "event: tick\ndata: event "
    <> int.to_string(n)
    <> " of "
    <> int.to_string(event_count)
    <> "\n\n"
  case fcgi.send_chunk(sender, bit_array.from_string(payload)) {
    Error(_) -> Nil
    Ok(_) -> {
      process.sleep(event_interval_ms)
      emit_events(sender, n + 1)
    }
  }
}

fn not_found() -> Response(fcgi.ResponseData) {
  response.new(404)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(fcgi.bytes(bytes_tree.from_string("not found\n")))
}
