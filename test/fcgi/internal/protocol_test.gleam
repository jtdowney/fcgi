import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/list
import gleam/string
import qcheck
import support/helpers

@external(erlang, "crypto", "strong_rand_bytes")
fn strong_rand_bytes(n: Int) -> BitArray

fn outgoing_fixed_size_record_generator() -> qcheck.Generator(protocol.Outgoing) {
  qcheck.from_generators(end_request_generator(), [unknown_type_generator()])
}

fn incoming_fixed_size_record_generator() -> qcheck.Generator(protocol.Incoming) {
  qcheck.from_generators(begin_request_generator(), [abort_request_generator()])
}

fn request_id_generator() -> qcheck.Generator(Int) {
  qcheck.bounded_int(0, 65_535)
}

fn role_byte_generator() -> qcheck.Generator(Int) {
  qcheck.bounded_int(0, 65_535)
}

fn begin_request_generator() -> qcheck.Generator(protocol.Incoming) {
  use id <- qcheck.bind(request_id_generator())
  use role <- qcheck.bind(role_byte_generator())
  use keep <- qcheck.map(qcheck.bool())
  protocol.BeginRequest(request_id: id, role:, keep_conn: keep)
}

fn abort_request_generator() -> qcheck.Generator(protocol.Incoming) {
  qcheck.map(request_id_generator(), protocol.AbortRequest)
}

fn protocol_status_generator() -> qcheck.Generator(protocol.Status) {
  qcheck.from_generators(qcheck.return(protocol.RequestComplete), [
    qcheck.return(protocol.CantMultiplexConnection),
    qcheck.return(protocol.Overloaded),
    qcheck.return(protocol.UnknownRole),
  ])
}

fn end_request_generator() -> qcheck.Generator(protocol.Outgoing) {
  use id <- qcheck.bind(request_id_generator())
  use status <- qcheck.bind(qcheck.bounded_int(0, 4_294_967_295))
  use protocol_status <- qcheck.map(protocol_status_generator())
  protocol.EndRequest(request_id: id, app_status: status, protocol_status:)
}

fn unknown_type_generator() -> qcheck.Generator(protocol.Outgoing) {
  qcheck.map(qcheck.bounded_int(50, 255), protocol.UnknownType)
}

fn name_value_pairs_generator() -> qcheck.Generator(List(#(String, String))) {
  qcheck.list_from(name_value_pair_generator())
}

fn name_value_pair_generator() -> qcheck.Generator(#(String, String)) {
  use name <- qcheck.bind(short_name_generator())
  use value <- qcheck.map(short_name_generator())
  #(name, value)
}

fn incoming_data_record_generator() -> qcheck.Generator(protocol.Incoming) {
  qcheck.from_generators(params_generator(), [
    stdin_generator(),
    get_values_generator(),
  ])
}

fn outgoing_data_record_generator() -> qcheck.Generator(protocol.Outgoing) {
  qcheck.from_generators(stdout_generator(), [get_values_result_generator()])
}

fn body_bit_array_generator() -> qcheck.Generator(BitArray) {
  qcheck.generic_byte_aligned_bit_array(
    qcheck.bounded_int(0, 255),
    qcheck.bounded_int(0, 1024),
  )
}

fn params_generator() -> qcheck.Generator(protocol.Incoming) {
  use id <- qcheck.bind(request_id_generator())
  use data <- qcheck.map(body_bit_array_generator())
  protocol.Params(request_id: id, data:)
}

fn stdin_generator() -> qcheck.Generator(protocol.Incoming) {
  use id <- qcheck.bind(request_id_generator())
  use data <- qcheck.map(body_bit_array_generator())
  protocol.Stdin(request_id: id, data:)
}

fn stdout_generator() -> qcheck.Generator(protocol.Outgoing) {
  use id <- qcheck.bind(request_id_generator())
  use data <- qcheck.map(body_bit_array_generator())
  protocol.Stdout(request_id: id, data:)
}

fn get_values_generator() -> qcheck.Generator(protocol.Incoming) {
  qcheck.map(qcheck.list_from(short_name_generator()), protocol.GetValues)
}

fn get_values_result_generator() -> qcheck.Generator(protocol.Outgoing) {
  qcheck.map(name_value_pairs_generator(), protocol.GetValuesResult)
}

fn short_name_generator() -> qcheck.Generator(String) {
  qcheck.generic_string(
    qcheck.printable_ascii_codepoint(),
    qcheck.bounded_int(0, 32),
  )
}

fn chunk_stdout_input_generator() -> qcheck.Generator(#(Int, BitArray)) {
  use id <- qcheck.bind(request_id_generator())
  use body <- qcheck.map(qcheck.generic_byte_aligned_bit_array(
    qcheck.bounded_int(0, 255),
    qcheck.bounded_int(0, 1024),
  ))
  #(id, body)
}

pub fn outgoing_fixed_size_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    outgoing_fixed_size_record_generator(),
  )
  let tree = protocol.encode_record(record)
  let bytes = bytes_tree.to_bit_array(tree)
  let #(parsed, rest) = helpers.parse_outgoing(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn incoming_fixed_size_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    incoming_fixed_size_record_generator(),
  )
  let bytes = helpers.encode_incoming(record)
  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn name_value_pairs_round_trip_test() {
  use pairs <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    name_value_pairs_generator(),
  )
  let bytes = protocol.encode_name_value_pairs(pairs) |> bytes_tree.to_bit_array
  let assert Ok(decoded) = protocol.parse_name_value_pairs(bytes)
  assert decoded == pairs
}

pub fn name_value_pairs_round_trip_unicode_test() {
  let pairs = [
    #("greeting", "café"),
    #("city", "東京"),
    #("party", "🎉🎊"),
    #("名前", "値"),
  ]
  let bytes = protocol.encode_name_value_pairs(pairs) |> bytes_tree.to_bit_array
  let assert Ok(decoded) = protocol.parse_name_value_pairs(bytes)
  assert decoded == pairs
}

pub fn name_value_pairs_handles_length_prefix_boundary_test() {
  let max_short = string.repeat("a", 127)
  let min_long = string.repeat("b", 128)
  let cases = [
    [],
    [#("", "")],
    [#("k", "v")],
    [#(max_short, max_short)],
    [#(min_long, "v")],
    [#("k", min_long)],
    [#(min_long, min_long)],
    [
      #("k1", "v1"),
      #(max_short, "v"),
      #("k", min_long),
      #(min_long, max_short),
    ],
  ]
  list.each(cases, fn(pairs) {
    let bytes =
      protocol.encode_name_value_pairs(pairs) |> bytes_tree.to_bit_array
    let assert Ok(decoded) = protocol.parse_name_value_pairs(bytes)
    assert decoded == pairs
  })
}

pub fn incoming_data_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    incoming_data_record_generator(),
  )
  let bytes = helpers.encode_incoming(record)
  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn outgoing_data_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    outgoing_data_record_generator(),
  )
  let tree = protocol.encode_record(record)
  let bytes = bytes_tree.to_bit_array(tree)
  let #(parsed, rest) = helpers.parse_outgoing(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn chunk_stdout_records_round_trip_test() {
  use #(id, body) <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    chunk_stdout_input_generator(),
  )
  let records = protocol.chunk_stdout(id, body)
  list.each(records, fn(record) {
    let assert protocol.Stdout(record_id, data) = record
    assert record_id == id
    assert bit_array.byte_size(data) <= protocol.max_record_content_size
  })
  let concatenated =
    list.fold(records, <<>>, fn(acc, record) {
      let assert protocol.Stdout(_, data) = record
      <<acc:bits, data:bits>>
    })
  assert concatenated == body
}

pub fn chunk_stdout_handles_boundary_sizes_test() {
  let id = 7
  let sizes = [
    0,
    1,
    protocol.max_record_content_size - 1,
    protocol.max_record_content_size,
    protocol.max_record_content_size + 1,
    2 * protocol.max_record_content_size,
    2 * protocol.max_record_content_size + 1,
    3 * protocol.max_record_content_size,
  ]
  list.each(sizes, fn(size) {
    let body = strong_rand_bytes(size)
    let records = protocol.chunk_stdout(id, body)
    list.each(records, fn(record) {
      let assert protocol.Stdout(record_id, data) = record
      assert record_id == id
      assert bit_array.byte_size(data) <= protocol.max_record_content_size
    })
    let concatenated =
      list.fold(records, <<>>, fn(acc, record) {
        let assert protocol.Stdout(_, data) = record
        <<acc:bits, data:bits>>
      })
    assert concatenated == body
  })
}

pub fn parse_record_skips_padding_to_next_record_test() {
  let stdin_record =
    helpers.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"hello":utf8>>),
    )
  let abort_record =
    helpers.encode_incoming(protocol.AbortRequest(request_id: 2))
  let bytes = <<stdin_record:bits, abort_record:bits>>

  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == protocol.Stdin(request_id: 1, data: <<"hello":utf8>>)
  assert rest == abort_record
}

pub fn parse_record_returns_trailing_bytes_as_rest_test() {
  let stdin_record =
    helpers.encode_incoming(protocol.Stdin(request_id: 1, data: <<"hi":utf8>>))
  let trailing = <<0xAA, 0xBB, 0xCC>>
  let bytes = <<stdin_record:bits, trailing:bits>>

  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == protocol.Stdin(request_id: 1, data: <<"hi":utf8>>)
  assert rest == trailing
}

pub fn parse_record_rejects_unsupported_version_test() {
  let bytes = <<
    2:size(8),
    protocol.begin_request_type:size(8),
    1:size(16),
    8:size(16),
    0:size(8),
    0:size(8),
    1:size(16),
    0:size(8),
    0:size(40),
  >>
  assert protocol.parse_record(bytes)
    == protocol.ParseError(protocol.UnsupportedVersion(2))
}

pub fn parse_record_returns_need_more_for_truncated_header_test() {
  assert protocol.parse_record(<<
      1:size(8),
      protocol.begin_request_type:size(8),
      0:size(16),
    >>)
    == protocol.NeedMore
}

pub fn parse_record_returns_need_more_for_truncated_body_test() {
  let bytes = <<
    1:size(8),
    protocol.begin_request_type:size(8),
    1:size(16),
    8:size(16),
    0:size(8),
    0:size(8),
    1:size(16),
    0:size(8),
  >>
  assert protocol.parse_record(bytes) == protocol.NeedMore
}

pub fn parse_record_rejects_malformed_begin_request_test() {
  let bytes = <<
    1:size(8),
    protocol.begin_request_type:size(8),
    1:size(16),
    3:size(16),
    5:size(8),
    0:size(8),
    0:size(8),
    0:size(8),
    0:size(8),
    0:size(40),
  >>
  assert protocol.parse_record(bytes)
    == protocol.ParseError(protocol.MalformedRecord)
}

pub fn parse_name_value_pairs_rejects_invalid_utf8_in_name_test() {
  let bytes = <<2:size(8), 1:size(8), 0xC3, 0x28, "v":utf8>>
  assert protocol.parse_name_value_pairs(bytes)
    == Error(protocol.MalformedNameValue)
}

pub fn parse_name_value_pairs_rejects_invalid_utf8_in_value_test() {
  let bytes = <<1:size(8), 2:size(8), "n":utf8, 0xC3, 0x28>>
  assert protocol.parse_name_value_pairs(bytes)
    == Error(protocol.MalformedNameValue)
}

pub fn parse_name_value_pairs_rejects_truncated_value_test() {
  let bytes = <<3:size(8), 5:size(8), "abc":utf8, "x":utf8>>
  assert protocol.parse_name_value_pairs(bytes)
    == Error(protocol.MalformedNameValue)
}

pub fn parse_name_value_pairs_rejects_truncated_length_prefix_test() {
  let bytes = <<1:size(1), 0:size(7)>>
  assert protocol.parse_name_value_pairs(bytes)
    == Error(protocol.MalformedNameValue)
}

pub fn frame_bits_at_max_content_size_succeeds_test() {
  let body = <<0:size({ protocol.max_record_content_size * 8 })>>
  let tree = protocol.frame_bits_unchecked(protocol.stdout_type, 1, body)
  let framed = bytes_tree.to_bit_array(tree)
  let assert <<
    1:size(8),
    record_type:size(8),
    1:size(16),
    content_length:size(16),
    _padding:size(8),
    0:size(8),
    _rest:bits,
  >> = framed
  assert record_type == protocol.stdout_type
  assert content_length == protocol.max_record_content_size
}
