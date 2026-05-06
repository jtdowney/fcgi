import fcgi/internal/connection
import fcgi/internal/server
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/string
import support/helpers
import support/test_client

pub fn server_serves_request_over_unix_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(connection.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(connection.Bytes(bytes_tree.from_string("ok")))
  }
  let template =
    server.SpecTemplate(max_body_size: 10 * 1024 * 1024, handler: handler)

  let assert Ok(running) = server.start(path, template)

  let assert Ok(client) = test_client.connect(path)
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "localhost"),
        #("PATH_INFO", "/"),
      ],
      body: <<>>,
      keep_conn: False,
    )
  let assert Ok(_) = connection.send(client, request_bytes)

  let received = helpers.recv_until_closed(client)
  connection.close_socket(client)
  server.stop(running)

  let assert Ok(records) = helpers.decode_all_records(received)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "ok"
}

pub fn server_stop_closes_listen_socket_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(connection.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(connection.Bytes(bytes_tree.from_string("ok")))
  }
  let template =
    server.SpecTemplate(max_body_size: 10 * 1024 * 1024, handler: handler)

  let assert Ok(running) = server.start(path, template)

  let supervisor_pid = running.supervisor_pid
  server.stop(running)

  let result = test_client.connect(path)
  let assert Error(_) = result
  assert process.is_alive(supervisor_pid) == False
}
