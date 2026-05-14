import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/list
import qcheck
import support/helpers

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
  use name <- qcheck.bind(short_or_long_string_generator())
  use value <- qcheck.map(short_or_long_string_generator())
  #(name, value)
}

fn short_or_long_string_generator() -> qcheck.Generator(String) {
  qcheck.from_generators(
    qcheck.generic_string(
      qcheck.printable_ascii_codepoint(),
      qcheck.bounded_int(0, 127),
    ),
    [
      qcheck.generic_string(
        qcheck.printable_ascii_codepoint(),
        qcheck.bounded_int(128, 200),
      ),
    ],
  )
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
    qcheck.bounded_int(0, 70_000),
  ))
  #(id, body)
}

pub fn outgoing_fixed_size_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    outgoing_fixed_size_record_generator(),
  )
  let bytes = bytes_tree.to_bit_array(protocol.encode_record(record))
  let #(parsed, rest) = helpers.parse_outgoing(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn incoming_fixed_size_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    incoming_fixed_size_record_generator(),
  )
  let bytes = protocol.encode_incoming(record)
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

pub fn incoming_data_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    incoming_data_record_generator(),
  )
  let bytes = protocol.encode_incoming(record)
  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn outgoing_data_records_round_trip_test() {
  use record <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(100),
    outgoing_data_record_generator(),
  )
  let bytes = bytes_tree.to_bit_array(protocol.encode_record(record))
  let #(parsed, rest) = helpers.parse_outgoing(bytes)
  assert parsed == record
  assert rest == <<>>
}

pub fn chunk_stdout_records_round_trip_test() {
  use #(id, body) <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(10),
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

pub fn chunk_stdout_size_zero_yields_no_records_test() {
  let records = protocol.chunk_stdout(1, <<>>)
  assert records == []
}

pub fn chunk_stdout_size_one_yields_single_record_test() {
  let body = <<0:size(8)>>
  let records = protocol.chunk_stdout(1, body)
  assert records == [protocol.Stdout(1, body)]
}

pub fn chunk_stdout_size_at_max_yields_single_full_record_test() {
  let body = <<0:size({ 65_535 * 8 })>>
  let records = protocol.chunk_stdout(1, body)
  let assert [protocol.Stdout(_, data)] = records
  assert bit_array.byte_size(data) == 65_535
  assert data == body
}

pub fn chunk_stdout_size_one_over_max_yields_two_records_test() {
  let body = <<0:size({ 65_536 * 8 })>>
  let records = protocol.chunk_stdout(1, body)
  let assert [protocol.Stdout(_, head), protocol.Stdout(_, tail)] = records
  assert bit_array.byte_size(head) == 65_535
  assert bit_array.byte_size(tail) == 1
  assert <<head:bits, tail:bits>> == body
}

pub fn chunk_stdout_size_exactly_two_full_records_test() {
  let body = <<0:size({ 131_070 * 8 })>>
  let records = protocol.chunk_stdout(1, body)
  let assert [protocol.Stdout(_, first), protocol.Stdout(_, second)] = records
  assert bit_array.byte_size(first) == 65_535
  assert bit_array.byte_size(second) == 65_535
  assert <<first:bits, second:bits>> == body
}

pub fn chunk_stdout_size_one_over_two_full_records_test() {
  let body = <<0:size({ 131_071 * 8 })>>
  let records = protocol.chunk_stdout(1, body)
  let assert [
    protocol.Stdout(_, first),
    protocol.Stdout(_, second),
    protocol.Stdout(_, third),
  ] = records
  assert bit_array.byte_size(first) == 65_535
  assert bit_array.byte_size(second) == 65_535
  assert bit_array.byte_size(third) == 1
  assert <<first:bits, second:bits, third:bits>> == body
}

pub fn parse_record_skips_padding_to_next_record_test() {
  let stdin_record =
    protocol.encode_incoming(
      protocol.Stdin(request_id: 1, data: <<"hello":utf8>>),
    )
  let abort_record =
    protocol.encode_incoming(protocol.AbortRequest(request_id: 2))
  let bytes = <<stdin_record:bits, abort_record:bits>>

  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == protocol.Stdin(request_id: 1, data: <<"hello":utf8>>)
  assert rest == abort_record
}

pub fn parse_record_returns_trailing_bytes_as_rest_test() {
  let stdin_record =
    protocol.encode_incoming(protocol.Stdin(request_id: 1, data: <<"hi":utf8>>))
  let trailing = <<0xAA, 0xBB, 0xCC>>
  let bytes = <<stdin_record:bits, trailing:bits>>

  let assert protocol.Parsed(parsed, rest) = protocol.parse_record(bytes)
  assert parsed == protocol.Stdin(request_id: 1, data: <<"hi":utf8>>)
  assert rest == trailing
}

pub fn parse_record_rejects_unsupported_version_test() {
  let bytes = <<
    2:size(8),
    1:size(8),
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
  assert protocol.parse_record(<<1:size(8), 1:size(8), 0:size(16)>>)
    == protocol.NeedMore
}

pub fn parse_record_returns_need_more_for_truncated_body_test() {
  let bytes = <<
    1:size(8),
    1:size(8),
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
    1:size(8),
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
