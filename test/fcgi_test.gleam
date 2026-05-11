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
import unitest

pub fn main() -> Nil {
  unitest.main()
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

fn echo_post_request_bytes(
  request_id: Int,
  body_text: String,
  keep_conn: Bool,
) -> BitArray {
  let body_bytes = bit_array.from_string(body_text)
  helpers.request_stream_bytes(
    request_id:,
    params: [
      #("REQUEST_METHOD", "POST"),
      #("CONTENT_LENGTH", int.to_string(bit_array.byte_size(body_bytes))),
      #("CONTENT_TYPE", "text/plain"),
    ],
    body: body_bytes,
    keep_conn:,
  )
}

fn echo_handler(
  req: Request(fcgi.Body),
) -> response.Response(fcgi.ResponseData) {
  let assert Ok(tree) = fcgi.read_all(req.body)
  let bytes = bytes_tree.to_bit_array(tree)
  let assert Ok(text) = bit_array.to_string(bytes)
  response.new(200)
  |> response.set_header("content-type", "text/plain")
  |> response.set_body(fcgi.bytes(bytes_tree.from_string("echo:" <> text)))
}

fn run_handler_with_body(
  build_body: fn() -> fcgi.ResponseData,
  request_bytes: BitArray,
) -> String {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(build_body())
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  body_text
}

fn run_unreachable_handler(request_bytes: BitArray) -> String {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_body(
      fcgi.bytes(bytes_tree.from_string("HANDLER UNEXPECTEDLY INVOKED")),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  text
}

fn drain_chunks(
  body: fcgi.Body,
  signal: process.Subject(String),
  acc: List(String),
) -> List(String) {
  drain_chunks_loop(fcgi.read_chunk(body), signal, acc)
}

fn drain_chunks_loop(
  result: Result(fcgi.Read, fcgi.ReadError),
  signal: process.Subject(String),
  acc: List(String),
) -> List(String) {
  case result {
    Error(_) -> acc
    Ok(fcgi.ReadingFinished) -> acc
    Ok(fcgi.Chunk(data, consume)) -> {
      let assert Ok(text) = bit_array.to_string(data)
      process.send(signal, text)
      drain_chunks_loop(consume(), signal, [text, ..acc])
    }
  }
}

fn body_for_request_id(
  records: List(protocol.Outgoing),
  request_id: Int,
) -> String {
  let stdout =
    list.fold(records, <<>>, fn(acc, record) {
      case record {
        protocol.Stdout(id, data) if id == request_id -> <<acc:bits, data:bits>>
        _ -> acc
      }
    })
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body_text)) = string.split_once(text, "\r\n\r\n")
  body_text
}

fn end_request_for(records: List(protocol.Outgoing), request_id: Int) -> Bool {
  list.any(records, fn(record) {
    case record {
      protocol.EndRequest(id, _, _) if id == request_id -> True
      _ -> False
    }
  })
}

pub fn end_to_end_get_returns_handler_body_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) =
          fcgi.send_file(
            path: "test/fixtures/hello.txt",
            offset: 0,
            limit: option.None,
          )
        b
      },
      simple_get_request_bytes(),
    )
  assert body == "hello world\n"
}

pub fn end_to_end_send_file_streams_large_file_across_multiple_records_test() {
  use path <- helpers.with_temp_file
  let payload = bit_array.concat(list.repeat(<<"abcdefgh":utf8>>, 20_000))
  let assert Ok(_) = simplifile.write_bits(to: path, bits: payload)
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) = fcgi.send_file(path:, offset: 0, limit: option.None)
        b
      },
      simple_get_request_bytes(),
    )
  let assert Ok(payload_string) = bit_array.to_string(payload)
  assert body == payload_string
}

pub fn file_unlinked_after_send_file_still_streams_test() {
  use file_path <- helpers.with_temp_file
  use socket_path <- helpers.with_temp_socket_path
  let assert Ok(_) =
    simplifile.write_bits(to: file_path, bits: <<"hello":utf8>>)

  let handler = fn(_req: Request(fcgi.Body)) {
    let assert Ok(file_body) =
      fcgi.send_file(path: file_path, offset: 0, limit: option.None)
    let assert Ok(_) = simplifile.delete(file_path)
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
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let has_end_request =
    list.any(records, fn(record) {
      case record {
        protocol.EndRequest(_, _, _) -> True
        _ -> False
      }
    })
  assert has_end_request
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "hello"
}

pub fn end_to_end_send_file_with_offset_and_limit_test() {
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) =
          fcgi.send_file(
            path: "test/fixtures/hello.txt",
            offset: 6,
            limit: option.Some(5),
          )
        b
      },
      simple_get_request_bytes(),
    )
  assert body == "world"
}

pub fn end_to_end_send_file_with_offset_past_eof_yields_empty_body_test() {
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) =
          fcgi.send_file(
            path: "test/fixtures/hello.txt",
            offset: 999,
            limit: option.None,
          )
        b
      },
      simple_get_request_bytes(),
    )
  assert body == ""
}

pub fn end_to_end_send_file_with_limit_beyond_file_size_clamps_test() {
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) =
          fcgi.send_file(
            path: "test/fixtures/hello.txt",
            offset: 0,
            limit: option.Some(10_000),
          )
        b
      },
      simple_get_request_bytes(),
    )
  assert body == "hello world\n"
}

pub fn end_to_end_send_file_with_zero_limit_yields_empty_body_test() {
  let body =
    run_handler_with_body(
      fn() {
        let assert Ok(b) =
          fcgi.send_file(
            path: "test/fixtures/hello.txt",
            offset: 0,
            limit: option.Some(0),
          )
        b
      },
      simple_get_request_bytes(),
    )
  assert body == ""
}

pub fn send_file_returns_file_body_for_existing_file_test() {
  let assert Ok(body) =
    fcgi.send_file(
      path: "test/fixtures/hello.txt",
      offset: 0,
      limit: option.None,
    )
  let assert fcgi.File(handle, 0, 12) = body
  connection.close_file(handle)
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
  use path <- helpers.with_temp_file
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

  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_body(
      fcgi.bytes(bytes_tree.from_string("HANDLER RESPONSE IGNORED")),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.max_body_size(1)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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
  let handler = fn(_req: Request(fcgi.Body)) { panic as "handler exploded" }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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

pub fn content_length_non_numeric_returns_400_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST"), #("CONTENT_LENGTH", "abc")],
      body: <<"hi":utf8>>,
      keep_conn: False,
    )

  let handler = fn(_req: Request(fcgi.Body)) {
    panic as "handler must not run for invalid content-length"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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

  let handler = fn(_req: Request(fcgi.Body)) {
    panic as "handler must not run for invalid content-length"
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  birdie.snap(text, "content_length_negative_returns_400_payload")
}

pub fn request_body_is_passed_through_to_handler_test() {
  use path <- helpers.with_temp_socket_path
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "POST")],
      body: <<"hello":utf8>>,
      keep_conn: False,
    )

  let handler = fn(req: Request(fcgi.Body)) {
    let assert Ok(buffered) = fcgi.read_all(req.body)
    let assert Ok(text) =
      buffered
      |> bytes_tree.to_bit_array
      |> bit_array.to_string
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(
      fcgi.bytes(bytes_tree.from_string(
        int.to_string(bytes_tree.byte_size(buffered)) <> ":" <> text,
      )),
    )
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, request_bytes)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, summary)) = string.split_once(text, "\r\n\r\n")
  assert summary == "5:hello"
}

pub fn request_body_is_streamed_chunk_by_chunk_test() {
  use path <- helpers.with_temp_socket_path
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_one =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"AAA":utf8>>),
    )
  let stdin_two =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"BBB":utf8>>),
    )
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))

  let signal = process.new_subject()
  let handler = fn(req: Request(fcgi.Body)) {
    let lines = drain_chunks(req.body, signal, [])
    let summary = string.join(list.reverse(lines), ",")
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string(summary)))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(socket, <<
      begin:bits,
      real_params:bits,
      params_end:bits,
      stdin_one:bits,
    >>)
  let assert Ok("AAA") = process.receive(signal, 1000)
  let assert Ok(_) = connection.send_bits(socket, stdin_two)
  let assert Ok("BBB") = process.receive(signal, 1000)
  let assert Ok(_) = connection.send_bits(socket, stdin_end)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, summary)) = string.split_once(text, "\r\n\r\n")
  assert summary == "AAA,BBB"
}

pub fn builder_body_read_timeout_surfaces_to_read_chunk_test() {
  use path <- helpers.with_temp_socket_path
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))

  let handler = fn(req: Request(fcgi.Body)) {
    let body = case fcgi.read_chunk(req.body) {
      Error(fcgi.ReadTimeout) -> "timeout"
      _ -> "unexpected"
    }
    response.new(408)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string(body)))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.body_read_timeout(100)
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(socket, <<
      begin:bits,
      real_params:bits,
      params_end:bits,
    >>)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "timeout"
}

pub fn read_chunk_returns_client_disconnected_when_peer_closes_test() {
  use path <- helpers.with_temp_socket_path
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_partial =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"AAA":utf8>>),
    )

  let signal = process.new_subject()
  let handler = fn(req: Request(fcgi.Body)) {
    let assert Ok(fcgi.Chunk(_, consume)) = fcgi.read_chunk(req.body)
    process.send(signal, "first-chunk")
    case consume() {
      Error(fcgi.ClientDisconnected) -> process.send(signal, "disconnected")
      _ -> process.send(signal, "unexpected")
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(socket, <<
      begin:bits,
      real_params:bits,
      params_end:bits,
      stdin_partial:bits,
    >>)
  let assert Ok("first-chunk") = process.receive(signal, 1000)
  connection.close_socket(socket)
  let assert Ok(observed) = process.receive(signal, 1000)
  helpers.stop_supervisor(started)

  assert observed == "disconnected"
}

pub fn listen_path_with_mode_chmods_the_socket_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string("ok")))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path_with_mode(path, 0o660)
    |> fcgi.start

  let assert Ok(info) = simplifile.file_info(path)
  let mode = simplifile.file_info_permissions_octal(info)
  helpers.stop_supervisor(started)

  assert mode == 0o660
}

pub fn stream_response_emits_each_chunk_as_separate_stdout_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, <<"first":utf8>>)
      let _ = fcgi.send_chunk(sender, <<"second":utf8>>)
      let _ = fcgi.send_chunk(sender, <<"third":utf8>>)
      Nil
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payloads =
    list.filter_map(records, fn(record) {
      case record {
        protocol.Stdout(_, data) -> Ok(data)
        _ -> Error(Nil)
      }
    })

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
  let handler = fn(_req: Request(fcgi.Body)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, chunk)
      Nil
    }
    response.new(200)
    |> response.set_header("content-type", "application/octet-stream")
    |> response.set_body(fcgi.stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 5000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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
  let handler = fn(_req: Request(fcgi.Body)) {
    let producer = fn(sender) {
      let _ = fcgi.send_chunk(sender, <<"partial":utf8>>)
      panic as "stream producer exploded"
    }
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.stream(producer))
  }

  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

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
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string("supervised-ok")))
  }

  let spec =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.supervised

  let assert Ok(started) = spec.start()
  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(socket, simple_get_request_bytes())
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  let stdout_payload = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout_payload)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "supervised-ok"

  helpers.stop_supervisor(started)
}

pub fn supervised_spec_returns_init_failed_on_listener_error_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "stale")

  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200) |> response.set_body(fcgi.bytes(bytes_tree.new()))
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

  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200) |> response.set_body(fcgi.bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.SocketPathExists(reported)) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start
  assert reported == path
}

pub fn negative_max_body_size_returns_error_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200) |> response.set_body(fcgi.bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.InvalidMaxBodySize(reported)) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.max_body_size(-1)
    |> fcgi.start
  assert reported == -1
}

pub fn non_positive_body_read_timeout_returns_error_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200) |> response.set_body(fcgi.bytes(bytes_tree.new()))
  }

  let assert Error(fcgi.InvalidBodyReadTimeout(reported)) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.body_read_timeout(0)
    |> fcgi.start
  assert reported == 0
}

pub fn socket_path_unlinks_on_stop_test() {
  use path <- helpers.with_temp_socket_path
  let handler = fn(_req: Request(fcgi.Body)) {
    response.new(200) |> response.set_body(fcgi.bytes(bytes_tree.new()))
  }
  let assert Ok(started) =
    handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(_info) = simplifile.file_info(path)

  helpers.stop_supervisor(started)

  assert simplifile.is_file(path) == Ok(False)
}

pub fn keep_alive_streams_two_sequential_requests_on_one_socket_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(started) =
    echo_handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(socket, echo_post_request_bytes(1, "first", True))
  let assert Ok(resp1) = test_client.recv_all(socket, 1000)

  let assert Ok(_) =
    connection.send_bits(socket, echo_post_request_bytes(2, "second", False))
  let assert Ok(resp2) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records1) = helpers.decode_all_records(resp1)
  assert end_request_for(records1, 1)
  assert body_for_request_id(records1, 1) == "echo:first"

  let assert Ok(records2) = helpers.decode_all_records(resp2)
  assert end_request_for(records2, 2)
  assert body_for_request_id(records2, 2) == "echo:second"
}

pub fn keep_alive_handles_pipelined_requests_in_one_send_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(started) =
    echo_handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let pipelined = <<
    echo_post_request_bytes(1, "alpha", True):bits,
    echo_post_request_bytes(2, "beta", False):bits,
  >>
  let assert Ok(_) = connection.send_bits(socket, pipelined)
  let assert Ok(bytes) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(bytes)
  assert end_request_for(records, 1)
  assert end_request_for(records, 2)
  assert body_for_request_id(records, 1) == "echo:alpha"
  assert body_for_request_id(records, 2) == "echo:beta"
}

pub fn keep_alive_drains_unread_body_before_next_request_test() {
  use path <- helpers.with_temp_socket_path
  let ignore_body_handler = fn(_req: Request(fcgi.Body)) {
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(fcgi.bytes(bytes_tree.from_string("ignored")))
  }
  let assert Ok(started) =
    ignore_body_handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(
      socket,
      echo_post_request_bytes(1, "unread-body", True),
    )
  let assert Ok(resp1) = test_client.recv_all(socket, 1000)

  let assert Ok(_) =
    connection.send_bits(socket, echo_post_request_bytes(2, "follow-up", False))
  let assert Ok(resp2) = test_client.recv_all(socket, 1000)
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records1) = helpers.decode_all_records(resp1)
  assert end_request_for(records1, 1)
  assert body_for_request_id(records1, 1) == "ignored"

  let assert Ok(records2) = helpers.decode_all_records(resp2)
  assert end_request_for(records2, 2)
  assert body_for_request_id(records2, 2) == "ignored"
}

pub fn keep_conn_false_closes_socket_after_response_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(started) =
    echo_handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(socket, echo_post_request_bytes(1, "only", False))
  let assert Ok(resp) = test_client.recv_all(socket, 1000)
  let second_send =
    connection.send_bits(socket, echo_post_request_bytes(2, "ignored", False))
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records) = helpers.decode_all_records(resp)
  assert end_request_for(records, 1)
  assert body_for_request_id(records, 1) == "echo:only"

  let assert Error(_) = second_send
}

pub fn keep_alive_does_not_loop_when_body_overflows_test() {
  use path <- helpers.with_temp_socket_path
  let echo_or_413_handler = fn(req: Request(fcgi.Body)) {
    case fcgi.read_all(req.body) {
      Ok(tree) -> {
        let bytes = bytes_tree.to_bit_array(tree)
        let assert Ok(text) = bit_array.to_string(bytes)
        response.new(200)
        |> response.set_header("content-type", "text/plain")
        |> response.set_body(
          fcgi.bytes(bytes_tree.from_string("echo:" <> text)),
        )
      }
      Error(fcgi.BodyTooLarge) ->
        response.new(413)
        |> response.set_header("content-type", "text/plain")
        |> response.set_body(fcgi.bytes(bytes_tree.from_string("too large")))
      Error(_) ->
        response.new(500)
        |> response.set_body(fcgi.bytes(bytes_tree.new()))
    }
  }
  let assert Ok(started) =
    echo_or_413_handler
    |> fcgi.new
    |> fcgi.listen_path(path)
    |> fcgi.max_body_size(4)
    |> fcgi.start

  let assert Ok(socket) = test_client.connect(path)
  let assert Ok(_) =
    connection.send_bits(
      socket,
      echo_post_request_bytes(1, "way-too-long", True),
    )
  let assert Ok(resp1) = test_client.recv_all(socket, 1000)
  let second_send =
    connection.send_bits(socket, echo_post_request_bytes(2, "later", False))
  connection.close_socket(socket)
  helpers.stop_supervisor(started)

  let assert Ok(records1) = helpers.decode_all_records(resp1)
  assert end_request_for(records1, 1)

  let assert Error(_) = second_send
}
