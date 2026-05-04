import fcgi
import fcgi/wisp_fcgi
import gleam/erlang/process
import gleam/int
import gleam/io
import wisp

pub fn main() -> Nil {
  wisp.configure_logger()
  let secret_key_base = wisp.random_string(64)

  let assert Ok(started) =
    handle_request
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(9000)
    |> fcgi.start

  io.println(
    "Listening on 127.0.0.1:" <> int.to_string(fcgi.bound_port(started)),
  )
  process.sleep_forever()
}

fn handle_request(req: wisp.Request) -> wisp.Response {
  case wisp.path_segments(req) {
    [] -> home()
    ["echo"] -> echo_body(req)
    _ -> wisp.not_found()
  }
}

fn home() -> wisp.Response {
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain; charset=utf-8")
  |> wisp.string_body("Hello, FastCGI!\n")
}

fn echo_body(req: wisp.Request) -> wisp.Response {
  use body <- wisp.require_string_body(req)
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain; charset=utf-8")
  |> wisp.string_body(body)
}
