import birdie
import fcgi
import fcgi/internal/connection
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string
import simplifile
import support/helpers
import support/test_client
import temporary
import unitest

pub fn main() -> Nil {
  unitest.main()
}

fn with_temp_file(fun: fn(String) -> a) -> a {
  let assert Ok(value) = temporary.create(temporary.file(), fun)
  value
}

fn simple_get_request_bytes() -> BitArray {
  helpers.request_stream_bytes(
    request_id: 1,
    params: [
      #("REQUEST_METHOD", "GET"),
      #("SERVER_NAME", "example.test"),
      #("PATH_INFO", "/hello"),
    ],
    body: <<>>,
    keep_conn: False,
  )
}

fn keep_alive_two_requests_bytes() -> BitArray {
  let one =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "example.test"),
        #("PATH_INFO", "/hello"),
      ],
      body: <<>>,
      keep_conn: True,
    )
  let two =
    helpers.request_stream_bytes(
      request_id: 2,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "example.test"),
        #("PATH_INFO", "/hello"),
      ],
      body: <<>>,
      keep_conn: True,
    )
  <<one:bits, two:bits>>
}

fn run_handler_with_body(
  body: fcgi.ResponseData,
  request_bytes: BitArray,
) -> String {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(body)
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  body_text
}

fn run_read_body(body: BitArray, max_size max_size: Int) -> String {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST")],
      body:,
      keep_conn: False,
    )

  let handler = fn(req: Request(fcgi.Connection)) {
    let summary = case fcgi.read_body(req, max_size:) {
      Ok(bytes) -> {
        let assert Ok(text) = bit_array.to_string(bytes)
        "ok:" <> int.to_string(bit_array.byte_size(bytes)) <> ":" <> text
      }
      Error(Nil) -> "err"
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string(summary)))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, summary)) = string.split_once(text, "\r\n\r\n")
  summary
}

fn run_unreachable_handler(request_bytes: BitArray) -> String {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_body(
      fcgi.Bytes(bytes_tree.from_string("HANDLER UNEXPECTEDLY INVOKED")),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  text
}

pub fn end_to_end_get_returns_handler_body_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "end_to_end_get_returns_handler_response_payload")

  let end_record = list.last(records)
  assert end_record
    == Ok(protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
}

pub fn end_to_end_send_file_streams_fixture_test() {
  let assert Ok(file_body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.None,
    )
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  assert body == "hello world\n"
}

pub fn end_to_end_send_file_streams_large_file_across_multiple_records_test() {
  use path <- with_temp_file
  let payload = bit_array.concat(list.repeat(<<"abcdefgh":utf8>>, 20_000))
  let assert Ok(_) = simplifile.write_bits(to: path, bits: payload)
  let assert Ok(file_body) =
    fcgi.send_file(path:, offset: 0, limit: option.None)
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  let assert Ok(payload_string) = bit_array.to_string(payload)
  assert body == payload_string
}

pub fn file_deleted_after_validation_returns_500_test() {
  use file_path <- with_temp_file
  use socket_path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write_bits(to: file_path, bits: <<"x":utf8>>)
  let assert Ok(file_body) =
    fcgi.send_file(path: file_path, offset: 0, limit: option.None)
  let assert Ok(_) = simplifile.delete(file_path)

  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(file_body)
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(socket_path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(socket_path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "file_deleted_after_validation_returns_500_payload")
}

pub fn end_to_end_send_file_with_offset_and_limit_test() {
  let assert Ok(file_body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 6,
      limit: option.Some(5),
    )
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  assert body == "world"
}

pub fn end_to_end_send_file_with_offset_past_eof_yields_empty_body_test() {
  let assert Ok(file_body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 999,
      limit: option.None,
    )
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  assert body == ""
}

pub fn end_to_end_send_file_with_limit_beyond_file_size_clamps_test() {
  let assert Ok(file_body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.Some(10_000),
    )
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  assert body == "hello world\n"
}

pub fn end_to_end_send_file_with_zero_limit_yields_empty_body_test() {
  let assert Ok(file_body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.Some(0),
    )
  let body = run_handler_with_body(file_body, simple_get_request_bytes())
  assert body == ""
}

pub fn keep_alive_serves_two_requests_on_one_connection_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, keep_alive_two_requests_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 500)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let end_records = list.filter(records, helpers.is_end_request)
  assert end_records
    == [
      protocol.EndRequest(
        request_id: 1,
        app_status: 0,
        protocol_status: protocol.RequestComplete,
      ),
      protocol.EndRequest(
        request_id: 2,
        app_status: 0,
        protocol_status: protocol.RequestComplete,
      ),
    ]
}

pub fn send_file_returns_file_body_for_existing_file_test() {
  let assert Ok(body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.None,
    )
  assert body
    == fcgi.File(path: "test/fixtures/hello.txt", offset: 0, limit: option.None)
}

pub fn send_file_rejects_missing_file_test() {
  assert fcgi.send_file(
      path: "test/fixtures/does_not_exist.txt",
      offset: 0,
      limit: option.None,
    )
    == Error(fcgi.FileNotFound("test/fixtures/does_not_exist.txt"))
}

pub fn send_file_rejects_directory_test() {
  assert fcgi.send_file(path: "test/fixtures", offset: 0, limit: option.None)
    == Error(fcgi.FileIsDirectory("test/fixtures"))
}

pub fn send_file_rejects_unreadable_file_test() {
  use path <- with_temp_file
  let assert Ok(_) = simplifile.write(to: path, contents: "secret")
  let assert Ok(_) = simplifile.set_permissions_octal(path, 0o000)
  assert fcgi.send_file(path:, offset: 0, limit: option.None)
    == Error(fcgi.FileAccessDenied(path))
}

pub fn send_file_rejects_negative_offset_test() {
  assert fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: -1,
      limit: option.None,
    )
    == Error(fcgi.InvalidRange(offset: -1, limit: option.None))
}

pub fn send_file_rejects_negative_limit_test() {
  assert fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.Some(-3),
    )
    == Error(fcgi.InvalidRange(offset: 0, limit: option.Some(-3)))
}

pub fn builder_max_body_size_rejects_oversized_body_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST")],
      body: <<"hi":utf8>>,
      keep_conn: False,
    )

  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_body(
      fcgi.Bytes(bytes_tree.from_string("HANDLER UNEXPECTEDLY INVOKED")),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.max_body_size(1)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_records =
    list.filter(records, fn(record) {
      case record {
        protocol.Stdout(_, _) -> True
        _ -> False
      }
    })
  assert stdout_records == []
  assert records
    == [
      protocol.EndRequest(
        request_id: 1,
        app_status: 0,
        protocol_status: protocol.Overloaded,
      ),
    ]
}

pub fn handler_panic_returns_500_response_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    panic as "handler exploded"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "handler_panic_returns_500_payload")

  let end_record = list.last(records)
  assert end_record
    == Ok(protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
}

pub fn content_length_mismatch_returns_400_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "POST"),
        #("CONTENT_LENGTH", "999"),
      ],
      body: <<"hi":utf8>>,
      keep_conn: False,
    )

  let handler = fn(_req: Request(fcgi.Connection)) {
    panic as "handler must not run for invalid content-length"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "content_length_mismatch_returns_400_payload")
}

pub fn content_length_non_numeric_returns_400_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST"), #("CONTENT_LENGTH", "abc")],
      body: <<"hi":utf8>>,
      keep_conn: False,
    )

  let handler = fn(_req: Request(fcgi.Connection)) {
    panic as "handler must not run for invalid content-length"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "content_length_non_numeric_returns_400_payload")
}

pub fn content_length_negative_returns_400_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST"), #("CONTENT_LENGTH", "-1")],
      body: <<>>,
      keep_conn: False,
    )

  let handler = fn(_req: Request(fcgi.Connection)) {
    panic as "handler must not run for invalid content-length"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "content_length_negative_returns_400_payload")
}

pub fn read_body_returns_empty_body_test() {
  assert run_read_body(<<>>, max_size: 1024) == "ok:0:"
}

pub fn read_body_returns_body_under_limit_test() {
  assert run_read_body(<<"hello":utf8>>, max_size: 1024) == "ok:5:hello"
}

pub fn read_body_accepts_body_at_exact_limit_test() {
  assert run_read_body(<<"hello":utf8>>, max_size: 5) == "ok:5:hello"
}

pub fn read_body_rejects_body_over_limit_test() {
  assert run_read_body(<<"hello":utf8>>, max_size: 4) == "err"
}

pub fn stream_response_emits_each_chunk_as_separate_stdout_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, <<"first":utf8>>)
      let _ = fcgi.send_chunk(sender, <<"second":utf8>>)
      let _ = fcgi.send_chunk(sender, <<"third":utf8>>)
      Nil
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payloads =
    list.filter_map(records, fn(record) {
      case record {
        protocol.Stdout(_, data) -> Ok(data)
        _ -> Error(Nil)
      }
    })

  // Header block + 3 chunk records + empty STDOUT terminator = 5 STDOUT records
  assert list.length(stdout_payloads) == 5

  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  assert body_text == "firstsecondthird"

  let end_record = list.last(records)
  assert end_record
    == Ok(protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
}

pub fn stream_send_chunk_splits_large_payload_into_max_size_records_test() {
  use path <- helpers.with_temp_socket_path
  let chunk = bit_array.concat(list.repeat(<<"x":utf8>>, 100_000))
  let handler = fn(_req: Request(fcgi.Connection)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, chunk)
      Nil
    }
    response.new(200)
    |> response.set_header("content-type", "application/octet-stream")
    |> response.set_body(fcgi.Stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 5000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload_sizes =
    list.filter_map(records, fn(record) {
      case record {
        protocol.Stdout(_, data) -> Ok(bit_array.byte_size(data))
        _ -> Error(Nil)
      }
    })

  assert stdout_payload_sizes == [55, 65_535, 34_465, 0]

  let stdout_total = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_total)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  let assert Ok(chunk_text) = bit_array.to_string(chunk)
  assert body_text == chunk_text
}

pub fn stream_producer_panic_still_emits_end_request_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, <<"partial":utf8>>)
      panic as "stream producer exploded"
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  fcgi.stop(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  assert body_text == "partial"

  let end_record = list.last(records)
  assert end_record
    == Ok(protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
}

pub fn missing_request_method_returns_400_test() {
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("SERVER_NAME", "example.test")],
      body: <<>>,
      keep_conn: False,
    )
  let response_text = run_unreachable_handler(request_bytes)
  birdie.snap(response_text, "missing_request_method_returns_400_payload")
}

pub fn malformed_params_returns_400_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let bad_params =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<0xFF>>))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let request_bytes = <<
    begin:bits,
    bad_params:bits,
    params_end:bits,
    stdin_end:bits,
  >>
  let response_text = run_unreachable_handler(request_bytes)
  birdie.snap(response_text, "malformed_params_returns_400_payload")
}

pub fn supervised_spec_starts_and_serves_request_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string("supervised-ok")))
  }

  let spec =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.supervised

  let assert Ok(actor.Started(pid: sup_pid, data: started)) = spec.start()
  assert process.is_alive(sup_pid)

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "supervised-ok"

  fcgi.stop(started)
  assert process.is_alive(sup_pid) == False
  assert simplifile.is_file(path) == Ok(False)
}

pub fn supervised_spec_returns_init_failed_on_listener_error_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "stale")

  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200) |> response.set_body(fcgi.Bytes(bytes_tree.new()))
  }

  let spec =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.supervised

  let assert Error(actor.InitFailed(reason)) = spec.start()
  assert reason == "socket path already exists: " <> path
}

pub fn socket_path_already_exists_returns_error_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "stale")

  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200) |> response.set_body(fcgi.Bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.SocketPathExists(reported)) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start
  assert reported == path
}

pub fn start_without_listen_path_returns_error_test() {
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200) |> response.set_body(fcgi.Bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.ListenerError(reason)) =
    handler
    |> fcgi.new
    |> fcgi.start
  assert reason == "listen_path must be called with a socket path"
}

pub fn negative_max_body_size_returns_error_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200) |> response.set_body(fcgi.Bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.InvalidMaxBodySize(reported)) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.max_body_size(-1)
    |> fcgi.start
  assert reported == -1
}

pub fn socket_path_unlinks_on_stop_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200) |> response.set_body(fcgi.Bytes(bytes_tree.new()))
  }
  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(_info) = simplifile.file_info(path)

  fcgi.stop(started)

  assert simplifile.is_file(path) == Ok(False)
}
