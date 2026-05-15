import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/list
import gleam/pair
import gleam/result

pub const max_record_content_size = 65_535

pub const responder_role = 1

const supported_version = 1

pub type Outgoing {
  EndRequest(request_id: Int, app_status: Int, protocol_status: Status)
  Stdout(request_id: Int, data: BitArray)
  GetValuesResult(pairs: List(#(String, String)))
  UnknownType(type_byte: Int)
}

pub type Status {
  RequestComplete
  CantMultiplexConnection
  Overloaded
  UnknownRole
}

pub fn encode_record(record: Outgoing) -> BytesTree {
  case record {
    EndRequest(id, app_status, protocol_status) -> {
      let body = <<
        app_status:size(32),
        status_to_int(protocol_status):size(8),
        0:size(24),
      >>
      frame_bits(3, id, body)
    }
    Stdout(id, data) -> frame_bits(6, id, data)
    GetValuesResult(pairs) -> frame_tree(10, 0, encode_name_value_pairs(pairs))
    UnknownType(type_byte) -> {
      let body = <<type_byte:size(8), 0:size(56)>>
      frame_bits(11, 0, body)
    }
  }
}

pub fn frame_bits(
  record_type: Int,
  request_id: Int,
  body: BitArray,
) -> BytesTree {
  frame_tree(record_type, request_id, bytes_tree.from_bit_array(body))
}

pub fn frame_tree(
  record_type: Int,
  request_id: Int,
  body: BytesTree,
) -> BytesTree {
  let content_length = bytes_tree.byte_size(body)
  let padding_length = padding_for(content_length)
  let header = <<
    supported_version:size(8),
    record_type:size(8),
    request_id:size(16),
    content_length:size(16),
    padding_length:size(8),
    0:size(8),
  >>
  let padding = <<0:size({ padding_length * 8 })>>
  bytes_tree.from_bit_array(header)
  |> bytes_tree.append_tree(body)
  |> bytes_tree.append(padding)
}

fn padding_for(content_length: Int) -> Int {
  let remainder = content_length % 8
  case remainder {
    0 -> 0
    _ -> 8 - remainder
  }
}

fn status_to_int(status: Status) -> Int {
  case status {
    RequestComplete -> 0
    CantMultiplexConnection -> 1
    Overloaded -> 2
    UnknownRole -> 3
  }
}

pub fn encode_name_value_pairs(pairs: List(#(String, String))) -> BytesTree {
  list.fold(pairs, bytes_tree.new(), fn(acc, pair) {
    let #(name, value) = pair
    let name_bytes = bit_array.from_string(name)
    let value_bytes = bit_array.from_string(value)
    let name_length = bit_array.byte_size(name_bytes)
    let value_length = bit_array.byte_size(value_bytes)
    acc
    |> bytes_tree.append(encode_length(name_length))
    |> bytes_tree.append(encode_length(value_length))
    |> bytes_tree.append(name_bytes)
    |> bytes_tree.append(value_bytes)
  })
}

fn encode_length(n: Int) -> BitArray {
  case n < 128 {
    True -> <<n:size(8)>>
    False -> <<1:size(1), n:size(31)>>
  }
}

pub fn encode_stdout_frame_header(
  request_id: Int,
  content_length: Int,
) -> #(BitArray, Int) {
  let padding_length = padding_for(content_length)
  let header = <<
    supported_version:size(8),
    6:size(8),
    request_id:size(16),
    content_length:size(16),
    padding_length:size(8),
    0:size(8),
  >>
  #(header, padding_length)
}

pub fn chunk_stdout(request_id: Int, body: BitArray) -> List(Outgoing) {
  chunk_stdout_loop(request_id, body, [])
}

fn chunk_stdout_loop(
  request_id: Int,
  body: BitArray,
  acc: List(Outgoing),
) -> List(Outgoing) {
  let total = bit_array.byte_size(body)
  use <- bool.guard(when: total == 0, return: list.reverse(acc))
  use <- bool.guard(
    when: total <= max_record_content_size,
    return: list.reverse([Stdout(request_id, body), ..acc]),
  )

  // Safe: the guard above ensures total > max_record_content_size.
  let assert Ok(head) = bit_array.slice(body, 0, max_record_content_size)
  let assert Ok(tail) =
    bit_array.slice(
      body,
      max_record_content_size,
      total - max_record_content_size,
    )

  chunk_stdout_loop(request_id, tail, [Stdout(request_id, head), ..acc])
}

pub type Incoming {
  BeginRequest(request_id: Int, role: Int, keep_conn: Bool)
  AbortRequest(request_id: Int)
  Params(request_id: Int, data: BitArray)
  Stdin(request_id: Int, data: BitArray)
  GetValues(names: List(String))
  IncomingUnknown(request_id: Int, type_byte: Int)
}

pub type ParseFailure {
  UnsupportedVersion(version: Int)
  MalformedRecord
  MalformedNameValue
}

pub type ParseResult {
  Parsed(record: Incoming, rest: BitArray)
  NeedMore
  ParseError(reason: ParseFailure)
}

pub fn parse_record(buffer: BitArray) -> ParseResult {
  case buffer {
    <<
      version:size(8),
      record_type:size(8),
      id:size(16),
      content_length:size(16),
      padding_length:size(8),
      _reserved:size(8),
      rest:bits,
    >> ->
      parse_after_header(
        version,
        record_type,
        id,
        content_length,
        padding_length,
        rest,
      )
    _ -> NeedMore
  }
}

fn parse_after_header(
  version: Int,
  record_type: Int,
  id: Int,
  content_length: Int,
  padding_length: Int,
  rest: BitArray,
) -> ParseResult {
  use <- bool.guard(
    when: version != supported_version,
    return: ParseError(UnsupportedVersion(version)),
  )
  case rest {
    <<
      body:bytes-size(content_length),
      _:bytes-size(padding_length),
      remaining:bits,
    >> -> parse_body(record_type, id, body, remaining)
    _ -> NeedMore
  }
}

fn parse_body(
  record_type: Int,
  id: Int,
  body: BitArray,
  rest: BitArray,
) -> ParseResult {
  case record_type {
    1 -> parse_begin_request(id, body, rest)
    2 -> Parsed(AbortRequest(id), rest)
    4 -> Parsed(Params(id, body), rest)
    5 -> Parsed(Stdin(id, body), rest)
    9 -> parse_get_values(body, rest)
    _ -> Parsed(IncomingUnknown(request_id: id, type_byte: record_type), rest)
  }
}

fn parse_begin_request(id: Int, body: BitArray, rest: BitArray) -> ParseResult {
  case body {
    <<role:size(16), _:size(7), keep_bit:size(1), _reserved:size(40)>> ->
      Parsed(
        BeginRequest(request_id: id, role:, keep_conn: keep_bit == 1),
        rest,
      )
    _ -> ParseError(MalformedRecord)
  }
}

fn parse_get_values(body: BitArray, rest: BitArray) -> ParseResult {
  case parse_name_value_pairs(body) {
    Error(reason) -> ParseError(reason)
    Ok(pairs) -> {
      let names = list.map(pairs, pair.first)
      Parsed(GetValues(names), rest)
    }
  }
}

pub fn parse_name_value_pairs(
  bytes: BitArray,
) -> Result(List(#(String, String)), ParseFailure) {
  parse_name_value_pairs_loop(bytes, [])
}

fn parse_name_value_pairs_loop(
  bytes: BitArray,
  acc: List(#(String, String)),
) -> Result(List(#(String, String)), ParseFailure) {
  use <- bool.guard(
    when: bit_array.byte_size(bytes) == 0,
    return: Ok(list.reverse(acc)),
  )

  use #(pair, rest) <- result.try(parse_one_pair(bytes))
  parse_name_value_pairs_loop(rest, [pair, ..acc])
}

fn parse_one_pair(
  bytes: BitArray,
) -> Result(#(#(String, String), BitArray), ParseFailure) {
  use #(name_length, after_name_length) <- result.try(parse_length(bytes))
  use #(value_length, after_value_length) <- result.try(parse_length(
    after_name_length,
  ))
  case after_value_length {
    <<
      name_bytes:bytes-size(name_length),
      value_bytes:bytes-size(value_length),
      rest:bits,
    >> -> {
      use name <- result.try(
        bit_array.to_string(name_bytes)
        |> result.replace_error(MalformedNameValue),
      )
      use value <- result.map(
        bit_array.to_string(value_bytes)
        |> result.replace_error(MalformedNameValue),
      )
      #(#(name, value), rest)
    }
    _ -> Error(MalformedNameValue)
  }
}

fn parse_length(bytes: BitArray) -> Result(#(Int, BitArray), ParseFailure) {
  case bytes {
    <<0:size(1), n:size(7), rest:bits>> -> Ok(#(n, rest))
    <<1:size(1), n:size(31), rest:bits>> -> Ok(#(n, rest))
    _ -> Error(MalformedNameValue)
  }
}
