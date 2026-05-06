import fcgi/internal/connection
import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/list
import temporary

const supported_version: Int = 1

pub type OutgoingParseResult {
  OutgoingParsed(record: protocol.Outgoing, rest: BitArray)
  OutgoingNeedMore
  OutgoingParseError(reason: protocol.ParseFailure)
}

pub fn with_temp_socket_path(fun: fn(String) -> a) -> a {
  let assert Ok(value) =
    temporary.create(temporary.directory(), fn(dir) { fun(dir <> "/sock") })
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
  let params_data = protocol.encode_name_value_pairs(params)
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

pub fn is_end_request(record: protocol.Outgoing) -> Bool {
  case record {
    protocol.EndRequest(_, _, _) -> True
    _ -> False
  }
}

pub fn parse_outgoing(buffer: BitArray) -> OutgoingParseResult {
  case buffer {
    <<
      version:size(8),
      record_type:size(8),
      id:size(16),
      content_length:size(16),
      padding_length:size(8),
      _reserved:size(8),
      rest:bits,
    >> -> {
      use <- bool.guard(
        when: version != supported_version,
        return: OutgoingParseError(protocol.UnsupportedVersion(version)),
      )
      let total = content_length + padding_length
      use <- bool.guard(
        when: bit_array.byte_size(rest) < total,
        return: OutgoingNeedMore,
      )

      let trailer_length = bit_array.byte_size(rest) - total
      let assert Ok(body) = bit_array.slice(rest, 0, content_length)
      let assert Ok(remaining) = bit_array.slice(rest, total, trailer_length)
      parse_outgoing_body(record_type, id, body, remaining)
    }
    _ -> OutgoingNeedMore
  }
}

fn parse_outgoing_body(
  record_type: Int,
  id: Int,
  body: BitArray,
  rest: BitArray,
) -> OutgoingParseResult {
  case record_type {
    3 -> parse_end_request(id, body, rest)
    6 -> OutgoingParsed(protocol.Stdout(id, body), rest)
    10 -> parse_get_values_result(body, rest)
    11 -> parse_unknown_type(body, rest)
    _ -> OutgoingParseError(protocol.MalformedRecord)
  }
}

fn parse_end_request(
  id: Int,
  body: BitArray,
  rest: BitArray,
) -> OutgoingParseResult {
  case body {
    <<app_status:size(32), protocol_status:size(8), _reserved:size(24)>> ->
      case status_from_int(protocol_status) {
        Ok(status) ->
          OutgoingParsed(
            protocol.EndRequest(
              request_id: id,
              app_status:,
              protocol_status: status,
            ),
            rest,
          )
        Error(_) -> OutgoingParseError(protocol.MalformedRecord)
      }
    _ -> OutgoingParseError(protocol.MalformedRecord)
  }
}

fn status_from_int(n: Int) -> Result(protocol.Status, Nil) {
  case n {
    0 -> Ok(protocol.RequestComplete)
    1 -> Ok(protocol.CantMultiplexConnection)
    2 -> Ok(protocol.Overloaded)
    3 -> Ok(protocol.UnknownRole)
    _ -> Error(Nil)
  }
}

fn parse_get_values_result(
  body: BitArray,
  rest: BitArray,
) -> OutgoingParseResult {
  case protocol.parse_name_value_pairs(body) {
    Error(reason) -> OutgoingParseError(reason)
    Ok(pairs) -> OutgoingParsed(protocol.GetValuesResult(pairs), rest)
  }
}

fn parse_unknown_type(body: BitArray, rest: BitArray) -> OutgoingParseResult {
  case body {
    <<type_byte:size(8), _reserved:size(56)>> ->
      OutgoingParsed(protocol.UnknownType(type_byte), rest)
    _ -> OutgoingParseError(protocol.MalformedRecord)
  }
}

pub type DecodeError {
  Truncated(remaining: BitArray)
  DecodeParseError(reason: protocol.ParseFailure)
}

pub fn decode_all_records(
  bytes: BitArray,
) -> Result(List(protocol.Outgoing), DecodeError) {
  decode_all_records_loop(bytes, [])
}

fn decode_all_records_loop(
  bytes: BitArray,
  acc: List(protocol.Outgoing),
) -> Result(List(protocol.Outgoing), DecodeError) {
  case bit_array.byte_size(bytes), parse_outgoing(bytes) {
    0, _ -> Ok(list.reverse(acc))
    _, OutgoingParsed(record, rest) ->
      decode_all_records_loop(rest, [record, ..acc])
    _, OutgoingNeedMore -> Error(Truncated(bytes))
    _, OutgoingParseError(reason) -> Error(DecodeParseError(reason))
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

pub fn recv_until_closed(socket: connection.Socket) -> BitArray {
  recv_until_closed_loop(socket, <<>>)
}

fn recv_until_closed_loop(
  socket: connection.Socket,
  acc: BitArray,
) -> BitArray {
  case connection.recv(socket, 0, 1000) {
    Ok(bytes) -> recv_until_closed_loop(socket, <<acc:bits, bytes:bits>>)
    Error(_) -> acc
  }
}
