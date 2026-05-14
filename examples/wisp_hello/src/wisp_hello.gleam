import fcgi
import gleam/erlang/process
import gleam/io
import wisp
import wisp_fcgi

const socket_path = "/tmp/fcgi_wisp_hello.sock"

pub fn main() -> Nil {
  wisp.configure_logger()
  let secret_key_base = wisp.random_string(64)

  let assert Ok(_) =
    handle_request
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_unix(socket_path)
    |> fcgi.start

  io.println("Listening on unix:" <> socket_path)
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
