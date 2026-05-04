import birdie
import fcgi
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import simplifile
import support/helpers
import support/test_client
import temporary
import unitest

pub fn main() -> Nil {
  unitest.main()
}

pub fn end_to_end_get_returns_handler_body_test() {
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

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
  use path <- with_temp_file
  let assert Ok(_) = simplifile.write_bits(to: path, bits: <<"x":utf8>>)
  let assert Ok(file_body) =
    fcgi.send_file(path:, offset: 0, limit: option.None)
  let assert Ok(_) = simplifile.delete(path)

  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(file_body)
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "file_deleted_after_validation_returns_500_payload")
}

fn with_temp_file(fun: fn(String) -> a) -> a {
  let assert Ok(value) = temporary.create(temporary.file(), fun)
  value
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
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, keep_alive_two_requests_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 500)
  test_client.close(socket)

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
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.max_body_size(1)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

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
  let handler = fn(_req: Request(fcgi.Connection)) {
    panic as "handler exploded"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  assert string.contains(text, "Status: 500")
  assert string.contains(text, "internal server error")

  let end_record = list.last(records)
  assert end_record
    == Ok(protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
}

pub fn content_length_mismatch_returns_400_test() {
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
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  assert string.contains(text, "Status: 400")
  assert string.contains(text, "CONTENT_LENGTH")
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

fn run_read_body(body: BitArray, max_size max_size: Int) -> String {
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
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, summary)) = string.split_once(text, "\r\n\r\n")
  summary
}

pub fn stream_yields_bounded_chunks_test() {
  let body = <<0:size(800)>>
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST")],
      body:,
      keep_conn: False,
    )

  let handler = fn(req: Request(fcgi.Connection)) {
    let summary = drain_stream_with_size(fcgi.stream(req), 40, [])
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.Bytes(bytes_tree.from_string(summary)))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, summary)) = string.split_once(text, "\r\n\r\n")
  let chunks = string.split(summary, ",")
  let total =
    list.fold(chunks, 0, fn(acc, c) {
      let assert Ok(n) = int.parse(c)
      acc + n
    })
  assert total == 100
  assert list.length(chunks) == 3
  assert list.first(chunks) == Ok("40")
}

pub fn stream_response_emits_each_chunk_as_separate_stdout_test() {
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
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

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

fn run_unreachable_handler(request_bytes: BitArray) -> String {
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_body(
      fcgi.Bytes(bytes_tree.from_string("HANDLER UNEXPECTEDLY INVOKED")),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  text
}

fn drain_stream_with_size(
  stream: fn(Int) -> Result(fcgi.Chunk, Nil),
  size: Int,
  acc: List(String),
) -> String {
  let assert Ok(chunk) = stream(size)
  case chunk {
    fcgi.Done -> string.join(list.reverse(acc), ",")
    fcgi.Chunk(data, consume) ->
      drain_stream_with_size(consume, size, [
        int.to_string(bit_array.byte_size(data)),
        ..acc
      ])
  }
}

fn run_handler_with_body(
  body: fcgi.ResponseData,
  request_bytes: BitArray,
) -> String {
  let handler = fn(_req: Request(fcgi.Connection)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(body)
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.bind("127.0.0.1")
    |> fcgi.port(0)
    |> fcgi.start
  let port = fcgi.bound_port(started)

  let assert Ok(socket) = test_client.connect("127.0.0.1", port)
  let assert Ok(_) = test_client.send(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  test_client.close(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  body_text
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
