import birdie
import fcgi
import fcgi/internal/connection
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

fn fixture_file_handler(_req: wisp.Request) -> wisp.Response {
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain")
  |> wisp.set_body(wisp.File(
    path: "test/fixtures/hello.txt",
    offset: 0,
    limit: option.None,
  ))
}

fn fixture_file_range_handler(_req: wisp.Request) -> wisp.Response {
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain")
  |> wisp.set_body(wisp.File(
    path: "test/fixtures/hello.txt",
    offset: 6,
    limit: option.Some(5),
  ))
}

fn unlink_file_handler(req: wisp.Request) -> wisp.Response {
  let assert Ok(path) = wisp.new_temporary_file(req)
  let assert Ok(_) =
    simplifile.write(to: path, contents: "payload-from-temp-file")
  wisp.response(200)
  |> wisp.set_header("content-type", "text/plain")
  |> wisp.set_body(wisp.File(path:, offset: 0, limit: option.None))
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

pub fn end_to_end_wisp_missing_file_returns_500_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let missing_file_handler = fn(_req: wisp.Request) {
    wisp.response(200)
    |> wisp.set_header("content-type", "text/plain")
    |> wisp.set_body(wisp.File(
      path: "test/fixtures/does_not_exist.bin",
      offset: 0,
      limit: option.None,
    ))
  }
  let assert Ok(started) =
    missing_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes(""))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  birdie.snap(text, "wisp_missing_file_returns_500_payload")
}

pub fn end_to_end_wisp_handler_echoes_request_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    echo_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes("hi"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")

  assert body == "POST hi"
}

pub fn wisp_file_response_streams_file_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    fixture_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes(""))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "hello world\n"
}

pub fn wisp_file_response_propagates_offset_and_limit_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    fixture_file_range_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes(""))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "world"
}

pub fn wisp_file_response_serves_body_after_path_unlinked_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    unlink_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes("body"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "payload-from-temp-file"
}

pub fn handler_deletes_temporary_files_after_response_test() {
  use path <- helpers.with_temp_socket_path
  let secret_key_base =
    "test-secret-key-base-padding-to-meet-min-length-requirements"
  let assert Ok(started) =
    temp_file_handler
    |> wisp_fcgi.handler(secret_key_base)
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, post_request_bytes("body"))
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, temp_dir)) = string.split_once(text, "\r\n\r\n")

  assert simplifile.is_directory(temp_dir) == Ok(False)
}
