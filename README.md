# fcgi

[![Package Version](https://img.shields.io/hexpm/v/fcgi)](https://hex.pm/packages/fcgi)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/fcgi/)

A FastCGI Responder server for Gleam, designed to sit behind a reverse proxy such as Caddy. Speaks the FastCGI Responder role over a Unix domain socket and exposes a `gleam/http`-shaped handler API plus a Wisp adapter.

## Installation

```sh
gleam add fcgi
```

## Usage with `gleam/http`

```gleam
import fcgi
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}

pub fn main() {
  let assert Ok(_) =
    fcgi.new(handle_request)
    |> fcgi.listen_path("/tmp/fcgi.sock")
    |> fcgi.start

  process.sleep_forever()
}

fn handle_request(
  _request: Request(fcgi.Body),
) -> Response(fcgi.ResponseData) {
  response.new(200)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(fcgi.bytes(bytes_tree.from_string("hello, joe!")))
}
```

## Usage with Wisp

```gleam
import fcgi
import fcgi/wisp_fcgi
import gleam/erlang/process
import wisp

pub fn main() {
  let secret_key_base = wisp.random_string(64)

  let assert Ok(_) =
    handle_request
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path("/tmp/fcgi.sock")
    |> fcgi.start

  process.sleep_forever()
}

fn handle_request(_request: wisp.Request) -> wisp.Response {
  wisp.ok()
  |> wisp.string_body("hello, joe!")
}
```

The socket file is created when the server starts and removed on shutdown.

## Reverse-proxy with Caddy

```caddy
example.com {
    reverse_proxy unix//tmp/fcgi.sock {
        transport fastcgi {
            env PATH_INFO {http.request.uri.path}
        }
    }
}
```

Caddy's `fastcgi` transport populates the standard FastCGI parameters that this library parses into a `gleam/http` `Request`.
