import fcgi/internal/protocol
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/list
import gleam/otp/actor
import temporary

const supported_version = 1

@external(erlang, "gen_server", "stop")
fn gen_server_stop(pid: process.Pid) -> Nil

pub fn stop_supervisor(started: actor.Started(a)) -> Nil {
  gen_server_stop(started.pid)
}

pub fn with_temp_socket_path(fun: fn(String) -> a) -> a {
  let assert Ok(value) =
    temporary.create(temporary.directory(), fn(dir) { fun(dir <> "/sock") })
  value
}

pub fn with_temp_file(fun: fn(String) -> a) -> a {
  let assert Ok(value) = temporary.create(temporary.file(), fun)
  value
}

pub fn request_stream_bytes(
  request_id request_id: Int,
  params params: List(#(String, String)),
  body body: BitArray,
  keep_conn keep_conn: Bool,
) -> BitArray {
  let begin =
    protocol.encode_incoming(protocol.BeginRequest(
      request_id:,
      role: protocol.responder_role,
      keep_conn:,
    ))
  let params_data =
    protocol.encode_name_value_pairs(params)
    |> bytes_tree.to_bit_array
  let params_record =
    protocol.encode_incoming(protocol.Params(request_id:, data: params_data))
  let params_end =
    protocol.encode_incoming(protocol.Params(request_id:, data: <<>>))
  let stdin = protocol.encode_incoming(protocol.Stdin(request_id:, data: body))
  let stdin_end =
    protocol.encode_incoming(protocol.Stdin(request_id:, data: <<>>))
  <<
    begin:bits,
    params_record:bits,
    params_end:bits,
    stdin:bits,
    stdin_end:bits,
  >>
}

pub fn parse_outgoing(buffer: BitArray) -> #(protocol.Outgoing, BitArray) {
  let assert <<
    version:size(8),
    record_type:size(8),
    id:size(16),
    content_length:size(16),
    padding_length:size(8),
    _reserved:size(8),
    rest:bits,
  >> = buffer
  let assert True = version == supported_version
  let total = content_length + padding_length
  let assert True = bit_array.byte_size(rest) >= total

  let trailer_length = bit_array.byte_size(rest) - total
  let assert Ok(body) = bit_array.slice(rest, 0, content_length)
  let assert Ok(remaining) = bit_array.slice(rest, total, trailer_length)
  parse_outgoing_body(record_type, id, body, remaining)
}

fn parse_outgoing_body(
  record_type: Int,
  id: Int,
  body: BitArray,
  rest: BitArray,
) -> #(protocol.Outgoing, BitArray) {
  case record_type {
    3 -> parse_end_request(id, body, rest)
    6 -> #(protocol.Stdout(id, body), rest)
    10 -> parse_get_values_result(body, rest)
    11 -> parse_unknown_type(body, rest)
    _ -> panic as "unrecognised outgoing record type"
  }
}

fn parse_end_request(
  id: Int,
  body: BitArray,
  rest: BitArray,
) -> #(protocol.Outgoing, BitArray) {
  let assert <<
    app_status:size(32),
    protocol_status:size(8),
    _reserved:size(24),
  >> = body
  let status = case protocol_status {
    0 -> protocol.RequestComplete
    1 -> protocol.CantMultiplexConnection
    2 -> protocol.Overloaded
    3 -> protocol.UnknownRole
    _ -> panic as "unrecognised protocol status"
  }
  #(
    protocol.EndRequest(request_id: id, app_status:, protocol_status: status),
    rest,
  )
}

fn parse_get_values_result(
  body: BitArray,
  rest: BitArray,
) -> #(protocol.Outgoing, BitArray) {
  let assert Ok(pairs) = protocol.parse_name_value_pairs(body)
  #(protocol.GetValuesResult(pairs), rest)
}

fn parse_unknown_type(
  body: BitArray,
  rest: BitArray,
) -> #(protocol.Outgoing, BitArray) {
  let assert <<type_byte:size(8), _reserved:size(56)>> = body
  #(protocol.UnknownType(type_byte), rest)
}

pub fn decode_all_records(bytes: BitArray) -> List(protocol.Outgoing) {
  decode_all_records_loop(bytes, [])
}

fn decode_all_records_loop(
  bytes: BitArray,
  acc: List(protocol.Outgoing),
) -> List(protocol.Outgoing) {
  case bit_array.byte_size(bytes) {
    0 -> list.reverse(acc)
    _ -> {
      let #(record, rest) = parse_outgoing(bytes)
      decode_all_records_loop(rest, [record, ..acc])
    }
  }
}

pub fn collect_stdout(records: List(protocol.Outgoing)) -> BitArray {
  list.fold(records, <<>>, fn(acc, record) {
    case record {
      protocol.Stdout(_, data) -> <<acc:bits, data:bits>>
      _ -> acc
    }
  })
}
