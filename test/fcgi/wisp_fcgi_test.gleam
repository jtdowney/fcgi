import fcgi
import fcgi/wisp_fcgi
import gleam/bit_array
import gleam/bytes_tree
import gleam/http
import gleam/int
import gleam/option
import gleam/string
import simplifile
import support/helpers
import support/test_client
import wisp

pub fn map_response_maps_each_body_variant_test() {
  let text = wisp.html_response("<p>hi</p>", 200) |> wisp_fcgi.map_response
  let assert fcgi.Bytes(text_tree) = text.body
  assert bytes_tree.to_bit_array(text_tree) == <<"<p>hi</p>":utf8>>

  let bytes =
    wisp.response(200)
    |> wisp.set_body(wisp.Bytes(bytes_tree.from_string("hello")))
    |> wisp_fcgi.map_response
  let assert fcgi.Bytes(bytes_inner) = bytes.body
  assert bytes_tree.to_bit_array(bytes_inner) == <<"hello":utf8>>

  let file =
    wisp.response(200)
    |> wisp.set_body(wisp.File(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.None,
    ))
    |> wisp_fcgi.map_response
  let assert fcgi.Stream(_producer) = file.body
}

pub fn map_response_missing_file_falls_back_to_500_test() {
  let response =
    wisp.response(200)
    |> wisp.set_body(wisp.File(
      path: "test/fixtures/does_not_exist.bin",
      offset: 0,
      limit: option.None,
    ))
    |> wisp_fcgi.map_response

  assert response.status == 500
  let assert fcgi.Bytes(body) = response.body
  assert bytes_tree.to_bit_array(body)
    == <<"could not open response file":utf8>>
}

pub fn end_to_end_wisp_handler_echoes_request_test() {
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    echo_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, post_request_bytes("hi"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")

  assert body == "POST hi"
}

pub fn wisp_file_response_serves_body_after_path_unlinked_test() {
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    unlink_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, post_request_bytes("body"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "payload-from-temp-file"
}

fn unlink_file_handler(req: wisp.Request) -> wisp.Response {
  let assert Ok(path) = wisp.new_temporary_file(req)
  let assert Ok(_) =
    simplifile.write(to: path, contents: "payload-from-temp-file")
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain")
  |> wisp.set_body(wisp.File(path:, offset: 0, limit: option.None))
}

pub fn handler_deletes_temporary_files_after_response_test() {
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    temp_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, post_request_bytes("body"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, temp_dir)) = string.split_once(text, "\r\n\r\n")

  assert simplifile.is_directory(temp_dir) == Ok(False)
}

fn temp_file_handler(req: wisp.Request) -> wisp.Response {
  let assert Ok(_) = wisp.new_temporary_file(req)
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain")
  |> wisp.set_body(wisp.Text(req.body.temporary_directory))
}

fn echo_handler(req: wisp.Request) -> wisp.Response {
  use body <- wisp.require_string_body(req)
  let payload = http.method_to_string(req.method) <> " " <> body
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain; charset=utf-8")
  |> wisp.set_body(wisp.Text(payload))
}

fn post_request_bytes(body_string: String) -> BitArray {
  let body_bytes = <<body_string:utf8>>
  helpers.request_stream_bytes(
    request_id: 1,
    params: [
      #("REQUEST_METHOD", "POST"),
      #("CONTENT_LENGTH", int.to_string(bit_array.byte_size(body_bytes))),
      #("CONTENT_TYPE", "text/plain"),
    ],
    body: body_bytes,
    keep_conn: False,
  )
}
