import birdie
import fcgi
import fcgi/internal/handler
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/http/response
import gleam/list
import support/helpers

const max_body = 1_048_576

fn response_payload_string(
  resp: response.Response(fcgi.ResponseData),
) -> String {
  let assert fcgi.Bytes(tree) = resp.body
  let bytes =
    bytes_tree.new()
    |> bytes_tree.append_tree(handler.encode_response_header(1, resp))
    |> bytes_tree.append_tree(handler.encode_stdout_chunk(1, tree))
    |> bytes_tree.append_tree(handler.encode_response_terminator(1))
    |> bytes_tree.to_bit_array
  let records = helpers.decode_all_records(bytes)
  let stdout_bytes = helpers.collect_stdout(records)
  let assert Ok(payload) = bit_array.to_string(stdout_bytes)
  payload
}

pub fn assembles_simple_request_test() {
  let bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "GET")],
      body: <<>>,
      keep_conn: False,
    )

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)
  assert outcome.continuation == handler.WaitForMore
  assert outcome.events
    == [
      handler.Start(
        request_id: 1,
        params: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
          |> bytes_tree.to_bit_array,
        keep_conn: False,
      ),
      handler.BodyEnd,
    ]
}

pub fn waits_for_more_when_only_begin_received_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let outcome =
    handler.step(handler.Idle(<<>>), bytes: begin, max_body_size: max_body)
  assert outcome.continuation == handler.WaitForMore
  assert outcome.events == []
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn body_too_large_after_start_emits_event_test() {
  let small_max = 16
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let chunk_one =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<0:size({ 8 * 8 })>>),
    )
  let chunk_two =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<0:size({ 1024 * 8 })>>),
    )
  let bytes = <<
    begin:bits,
    real_params:bits,
    params_end:bits,
    chunk_one:bits,
    chunk_two:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: small_max)

  let assert [
    handler.Start(_, _, _),
    handler.BodyChunk(_),
    handler.BodyTooLarge,
  ] = outcome.events
  assert outcome.continuation == handler.WaitForMore
}

pub fn body_too_large_before_start_emits_overloaded_test() {
  let small_max = 16
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let chunk =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<0:size({ 1024 * 8 })>>),
    )
  let bytes = <<begin:bits, chunk:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: small_max)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record
    == protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.Overloaded,
    )
  assert rest == <<>>
  assert outcome.events == []
  assert outcome.continuation == handler.CloseConnection
}

pub fn params_overflow_emits_overloaded_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  // Each pair encodes to ~44 bytes; two records of 800 pairs accumulate
  // ~70_400 bytes of params, which overflows the 65_536 cap.
  let batch =
    list.repeat(#("HTTP_HEADER_NAME", "value_for_overflow_padding"), 800)
  let encoded_batch =
    protocol.encode_name_value_pairs(batch) |> bytes_tree.to_bit_array
  let params_record_a =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: encoded_batch))
  let params_record_b =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: encoded_batch))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    params_record_a:bits,
    params_record_b:bits,
    params_end:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record
    == protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.Overloaded,
    )
  assert rest == <<>>
  assert outcome.continuation == handler.CloseConnection
  assert outcome.events == []
}

pub fn begin_request_with_id_zero_closes_connection_test() {
  let bytes =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 0,
      role: protocol.responder_role,
      keep_conn: True,
    ))

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  assert outcome.continuation == handler.CloseConnection
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn unknown_application_record_is_ignored_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let unknown_app =
    protocol.encode_incoming(protocol.IncomingUnknown(
      request_id: 1,
      type_byte: 99,
    ))
  let bytes = <<begin:bits, unknown_app:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  assert bytes_tree.byte_size(outcome.outgoing) == 0
  assert outcome.continuation == handler.WaitForMore
}

pub fn multiplexing_rejected_with_cant_mpx_conn_test() {
  let begin_one =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let begin_two =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 2,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let bytes = <<begin_one:bits, begin_two:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record
    == protocol.EndRequest(
      request_id: 2,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    )
  assert rest == <<>>
  assert outcome.continuation == handler.WaitForMore
  let assert handler.Receiving(_, partial) = outcome.state
  assert partial.request_id == 1
}

pub fn unknown_record_type_replies_with_unknown_type_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: True,
    ))
  let unknown =
    protocol.encode_incoming(protocol.IncomingUnknown(
      request_id: 0,
      type_byte: 99,
    ))
  let bytes = <<begin:bits, unknown:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record == protocol.UnknownType(type_byte: 99)
  assert rest == <<>>
  assert outcome.continuation == handler.WaitForMore
}

pub fn get_values_returns_capabilities_test() {
  let names = ["FCGI_MAX_CONNS", "FCGI_MAX_REQS", "FCGI_MPXS_CONNS"]
  let bytes = protocol.encode_incoming(protocol.GetValues(names:))

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  let assert protocol.GetValuesResult(pairs) = record
  assert rest == <<>>
  assert list.key_find(pairs, "FCGI_MAX_CONNS") == Error(Nil)
  assert list.key_find(pairs, "FCGI_MAX_REQS") == Error(Nil)
  assert list.key_find(pairs, "FCGI_MPXS_CONNS") == Ok("0")
  assert list.length(pairs) == 1
  assert outcome.continuation == handler.WaitForMore
}

pub fn abort_request_emits_request_complete_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let abort = protocol.encode_incoming(protocol.AbortRequest(request_id: 1))
  let bytes = <<begin:bits, abort:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record
    == protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    )
  assert rest == <<>>
  assert outcome.continuation == handler.CloseConnection
  assert outcome.state == handler.Idle(<<>>)
  assert outcome.events == []
}

pub fn non_responder_role_replies_unknown_role_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: 2,
      keep_conn: True,
    ))
  let outcome =
    handler.step(handler.Idle(<<>>), bytes: begin, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record
    == protocol.EndRequest(
      request_id: 1,
      app_status: 0,
      protocol_status: protocol.UnknownRole,
    )
  assert rest == <<>>
  assert outcome.continuation == handler.CloseConnection
  assert outcome.state == handler.Idle(<<>>)
}

pub fn split_params_records_concatenate_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let params_part_one =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_part_two =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("PATH_INFO", "/x")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    params_part_one:bits,
    params_part_two:bits,
    params_end:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  assert outcome.events
    == [
      handler.Start(
        request_id: 1,
        params: protocol.encode_name_value_pairs([
          #("REQUEST_METHOD", "POST"),
          #("PATH_INFO", "/x"),
        ])
          |> bytes_tree.to_bit_array,
        keep_conn: False,
      ),
      handler.BodyEnd,
    ]
}

pub fn split_stdin_records_emit_separate_chunks_test() {
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
  let stdin_part_one =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"hel":utf8>>),
    )
  let stdin_part_two =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<"lo":utf8>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    real_params:bits,
    params_end:bits,
    stdin_part_one:bits,
    stdin_part_two:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [
    handler.Start(_, _, _),
    handler.BodyChunk(<<"hel":utf8>>),
    handler.BodyChunk(<<"lo":utf8>>),
    handler.BodyEnd,
  ] = outcome.events
}

pub fn stdin_before_params_is_dropped_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let stdin =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<"hi":utf8>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "POST")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    stdin:bits,
    stdin_end:bits,
    real_params:bits,
    params_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [handler.Start(_, _, _), handler.BodyEnd] = outcome.events
}

pub fn terminators_arriving_in_separate_feeds_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))

  let outcome_one =
    handler.step(
      handler.Idle(<<>>),
      bytes: <<begin:bits, real_params:bits>>,
      max_body_size: max_body,
    )
  assert outcome_one.continuation == handler.WaitForMore
  assert outcome_one.events == []

  let outcome_two =
    handler.step(outcome_one.state, bytes: params_end, max_body_size: max_body)
  assert outcome_two.continuation == handler.WaitForMore
  let assert [handler.Start(1, params_buffer, False)] = outcome_two.events
  assert params_buffer
    == {
      protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
      |> bytes_tree.to_bit_array
    }

  let outcome_three =
    handler.step(outcome_two.state, bytes: stdin_end, max_body_size: max_body)
  assert outcome_three.events == [handler.BodyEnd]
}

pub fn get_values_filters_unknown_names_test() {
  let names = ["UNKNOWN", "FCGI_MPXS_CONNS"]
  let bytes = protocol.encode_incoming(protocol.GetValues(names:))

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record == protocol.GetValuesResult(pairs: [#("FCGI_MPXS_CONNS", "0")])
  assert rest == <<>>
}

pub fn unsupported_version_closes_connection_test() {
  let bytes = <<
    2:size(8),
    1:size(8),
    1:size(16),
    0:size(16),
    0:size(8),
    0:size(8),
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  assert outcome.continuation == handler.CloseConnection
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn malformed_begin_request_closes_connection_test() {
  let bytes = <<
    1:size(8),
    1:size(8),
    1:size(16),
    4:size(16),
    0:size(8),
    0:size(8),
    0:size(32),
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  assert outcome.continuation == handler.CloseConnection
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn mismatched_params_id_is_ignored_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let stray_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 2,
      data: protocol.encode_name_value_pairs([#("STRAY", "yes")])
        |> bytes_tree.to_bit_array,
    ))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    stray_params:bits,
    real_params:bits,
    params_end:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [handler.Start(1, params_buffer, False), handler.BodyEnd] =
    outcome.events
  assert params_buffer
    == {
      protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
      |> bytes_tree.to_bit_array
    }
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn mismatched_stdin_id_is_ignored_test() {
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
  let stray_stdin =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 2, data: <<"stray":utf8>>),
    )
  let real_stdin =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"hello":utf8>>),
    )
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    real_params:bits,
    params_end:bits,
    stray_stdin:bits,
    real_stdin:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [
    handler.Start(1, _, False),
    handler.BodyChunk(<<"hello":utf8>>),
    handler.BodyEnd,
  ] = outcome.events
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn mismatched_abort_id_is_ignored_test() {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id: 1,
      role: protocol.responder_role,
      keep_conn: False,
    ))
  let stray_abort =
    protocol.encode_incoming(protocol.AbortRequest(request_id: 2))
  let real_params =
    protocol.encode_incoming(protocol.Params(
      request_id: 1,
      data: protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
        |> bytes_tree.to_bit_array,
    ))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id: 1, data: <<>>))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<>>))
  let bytes = <<
    begin:bits,
    stray_abort:bits,
    real_params:bits,
    params_end:bits,
    stdin_end:bits,
  >>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [handler.Start(1, _, False), handler.BodyEnd] = outcome.events
  assert bytes_tree.byte_size(outcome.outgoing) == 0
}

pub fn get_values_followed_by_request_drains_buffered_records_test() {
  let get_values =
    protocol.encode_incoming(protocol.GetValues(names: ["FCGI_MPXS_CONNS"]))
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 7,
      params: [#("REQUEST_METHOD", "GET")],
      body: <<>>,
      keep_conn: False,
    )
  let bytes = <<get_values:bits, request_bytes:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [handler.Start(7, params_buffer, False), handler.BodyEnd] =
    outcome.events
  assert params_buffer
    == {
      protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
      |> bytes_tree.to_bit_array
    }

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  let assert protocol.GetValuesResult(pairs) = record
  assert list.key_find(pairs, "FCGI_MPXS_CONNS") == Ok("0")
  assert rest == <<>>
}

pub fn unknown_type_followed_by_request_drains_buffered_records_test() {
  let unknown =
    protocol.encode_incoming(protocol.IncomingUnknown(
      request_id: 0,
      type_byte: 99,
    ))
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "GET")],
      body: <<>>,
      keep_conn: False,
    )
  let bytes = <<unknown:bits, request_bytes:bits>>

  let outcome =
    handler.step(handler.Idle(<<>>), bytes:, max_body_size: max_body)

  let assert [handler.Start(1, _, False), handler.BodyEnd] = outcome.events

  let #(record, rest) =
    helpers.parse_outgoing(bytes_tree.to_bit_array(outcome.outgoing))
  assert record == protocol.UnknownType(type_byte: 99)
  assert rest == <<>>
}

pub fn feed_resumes_across_partial_records_test() {
  let bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [#("REQUEST_METHOD", "GET")],
      body: <<>>,
      keep_conn: False,
    )
  let total = bit_array.byte_size(bytes)
  let split_at = total / 2
  let assert Ok(first) = bit_array.slice(bytes, 0, split_at)
  let assert Ok(second) = bit_array.slice(bytes, split_at, total - split_at)

  let outcome_one =
    handler.step(handler.Idle(<<>>), bytes: first, max_body_size: max_body)
  assert outcome_one.continuation == handler.WaitForMore

  let outcome_two =
    handler.step(outcome_one.state, bytes: second, max_body_size: max_body)
  let assert [handler.Start(1, params_buffer, False), handler.BodyEnd] =
    outcome_two.events
  assert params_buffer
    == {
      protocol.encode_name_value_pairs([#("REQUEST_METHOD", "GET")])
      |> bytes_tree.to_bit_array
    }
}

pub fn writes_bytes_response_with_cgi_header_block_test() {
  let body = fcgi.bytes(bytes_tree.from_string("hello"))
  let resp =
    response.new(200)
    |> response.set_header("content-type", "text/plain")
    |> response.set_body(body)
  let assert fcgi.Bytes(tree) = resp.body
  let bytes =
    bytes_tree.new()
    |> bytes_tree.append_tree(handler.encode_response_header(1, resp))
    |> bytes_tree.append_tree(handler.encode_stdout_chunk(1, tree))
    |> bytes_tree.append_tree(handler.encode_response_terminator(1))
    |> bytes_tree.to_bit_array

  let records = helpers.decode_all_records(bytes)
  let assert [stdout_header, stdout_body, stdout_empty, end_record] = records
  let assert protocol.Stdout(1, _) = stdout_header
  let assert protocol.Stdout(1, _) = stdout_body
  let assert protocol.Stdout(1, <<>>) = stdout_empty
  let assert protocol.EndRequest(1, 0, protocol.RequestComplete) = end_record

  let stdout_bytes = helpers.collect_stdout(records)
  let assert Ok(payload_string) = bit_array.to_string(stdout_bytes)
  birdie.snap(payload_string, "writes_bytes_response_payload")
}

pub fn drops_response_headers_with_crlf_in_value_test() {
  let resp =
    response.new(200)
    |> response.set_header("x-clean", "ok")
    |> response.set_header(
      "x-evil",
      "value\r\nStatus: 500 Internal Server Error",
    )
    |> response.set_body(fcgi.bytes(bytes_tree.new()))
  birdie.snap(
    response_payload_string(resp),
    "drops_response_headers_with_crlf_in_value",
  )
}

pub fn drops_response_headers_with_crlf_in_name_test() {
  let resp =
    response.new(200)
    |> response.set_header("x-good\r\nx-injected", "anything")
    |> response.set_body(fcgi.bytes(bytes_tree.new()))
  birdie.snap(
    response_payload_string(resp),
    "drops_response_headers_with_crlf_in_name",
  )
}
