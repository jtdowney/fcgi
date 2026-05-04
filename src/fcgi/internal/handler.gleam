import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/string

pub type State {
  Idle(buffer: BitArray)
  Receiving(buffer: BitArray, partial: PartialRequest)
}

pub type PartialRequest {
  PartialRequest(
    request_id: Int,
    keep_conn: Bool,
    params: BitArray,
    params_done: Bool,
    params_overflow: Bool,
    stdin: BitArray,
    stdin_done: Bool,
    stdin_overflow: Bool,
  )
}

pub type Action {
  WaitForMore
  ReadyForHandler(
    request_id: Int,
    params: BitArray,
    body: BitArray,
    keep_conn: Bool,
  )
  CloseConnection
}

pub type Outcome {
  Outcome(state: State, outgoing: BytesTree, action: Action)
}

pub fn new() -> State {
  Idle(<<>>)
}

pub fn feed(
  state: State,
  bytes bytes: BitArray,
  max_body_size max_body_size: Int,
  max_params_size max_params_size: Int,
) -> Outcome {
  let combined = with_buffer(state, <<state.buffer:bits, bytes:bits>>)
  feed_loop(combined, bytes_tree.new(), max_body_size, max_params_size)
}

fn with_buffer(state: State, buffer: BitArray) -> State {
  case state {
    Idle(_) -> Idle(buffer)
    Receiving(_, partial) -> Receiving(buffer, partial)
  }
}

fn feed_loop(
  state: State,
  outgoing: BytesTree,
  max_body_size: Int,
  max_params_size: Int,
) -> Outcome {
  case protocol.parse_record(state.buffer) {
    protocol.NeedMore -> Outcome(state:, outgoing:, action: WaitForMore)
    protocol.ParseError(_) ->
      Outcome(state:, outgoing:, action: CloseConnection)
    protocol.Parsed(record, rest) ->
      dispatch_record(
        state,
        record,
        rest,
        outgoing,
        max_body_size,
        max_params_size,
      )
  }
}

fn dispatch_record(
  state: State,
  record: protocol.Incoming,
  rest: BitArray,
  outgoing: BytesTree,
  max_body_size: Int,
  max_params_size: Int,
) -> Outcome {
  let #(new_state, new_outgoing, action_opt) =
    apply_record(state, record, max_body_size, max_params_size)
  let advanced = with_buffer(new_state, rest)

  let combined_outgoing = bytes_tree.append_tree(outgoing, new_outgoing)
  case action_opt {
    option.None | option.Some(WaitForMore) ->
      feed_loop(advanced, combined_outgoing, max_body_size, max_params_size)
    option.Some(action) ->
      Outcome(state: advanced, outgoing: combined_outgoing, action:)
  }
}

fn apply_record(
  state: State,
  record: protocol.Incoming,
  max_body_size: Int,
  max_params_size: Int,
) -> #(State, BytesTree, Option(Action)) {
  case state, record {
    Idle(_), protocol.BeginRequest(0, _, _) -> #(
      state,
      bytes_tree.new(),
      option.Some(CloseConnection),
    )
    Idle(buffer), protocol.BeginRequest(id, role, keep) ->
      apply_begin_request_idle(buffer, id, role, keep)
    Receiving(_, _), protocol.BeginRequest(0, _, _) -> #(
      state,
      bytes_tree.new(),
      option.Some(CloseConnection),
    )
    Receiving(buffer, partial), protocol.BeginRequest(id, _, _) ->
      apply_begin_request_busy(buffer, partial, id)
    Receiving(buffer, partial), protocol.Params(id, data)
      if id == partial.request_id
    -> apply_params(buffer, partial, data, max_params_size)
    Receiving(buffer, partial), protocol.Stdin(id, data)
      if id == partial.request_id
    -> apply_stdin(buffer, partial, data, max_body_size)
    Receiving(_, partial), protocol.AbortRequest(id)
      if id == partial.request_id
    -> apply_abort(partial)
    _, protocol.GetValues(names) -> apply_get_values(state, names)
    _, protocol.IncomingUnknown(id, type_byte) ->
      apply_unknown_type_record(state, id, type_byte)
    Idle(_), protocol.Params(_, _) -> ignore_out_of_order_record(state)
    Idle(_), protocol.Stdin(_, _) -> ignore_out_of_order_record(state)
    Idle(_), protocol.AbortRequest(_) -> ignore_out_of_order_record(state)
    Receiving(_, _), protocol.Params(_, _) -> ignore_out_of_order_record(state)
    Receiving(_, _), protocol.Stdin(_, _) -> ignore_out_of_order_record(state)
    Receiving(_, _), protocol.AbortRequest(_) ->
      ignore_out_of_order_record(state)
  }
}

fn ignore_out_of_order_record(
  state: State,
) -> #(State, BytesTree, Option(Action)) {
  #(state, bytes_tree.new(), option.None)
}

fn apply_begin_request_idle(
  buffer: BitArray,
  id: Int,
  role: Int,
  keep: Bool,
) -> #(State, BytesTree, Option(Action)) {
  case role == protocol.responder_role {
    True -> {
      let partial =
        PartialRequest(
          request_id: id,
          keep_conn: keep,
          params: <<>>,
          params_done: False,
          params_overflow: False,
          stdin: <<>>,
          stdin_done: False,
          stdin_overflow: False,
        )
      #(Receiving(buffer, partial), bytes_tree.new(), option.None)
    }
    False -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: id,
          app_status: 0,
          protocol_status: protocol.UnknownRole,
        ))
      #(Idle(buffer), reply, option.Some(next_action_after_request(keep)))
    }
  }
}

fn apply_begin_request_busy(
  buffer: BitArray,
  partial: PartialRequest,
  id: Int,
) -> #(State, BytesTree, Option(Action)) {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: id,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    ))
  #(Receiving(buffer, partial), reply, option.Some(WaitForMore))
}

fn apply_params(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_params_size: Int,
) -> #(State, BytesTree, Option(Action)) {
  case bit_array.byte_size(data) {
    0 -> {
      let updated = PartialRequest(..partial, params_done: True)
      maybe_ready(buffer, updated)
    }
    _ -> {
      let updated = case partial.params_overflow {
        True -> partial
        False -> {
          let combined = <<partial.params:bits, data:bits>>
          case bit_array.byte_size(combined) > max_params_size {
            True ->
              PartialRequest(..partial, params: <<>>, params_overflow: True)
            False -> PartialRequest(..partial, params: combined)
          }
        }
      }
      #(Receiving(buffer, updated), bytes_tree.new(), option.None)
    }
  }
}

fn apply_stdin(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> #(State, BytesTree, Option(Action)) {
  case bit_array.byte_size(data) {
    0 -> {
      let updated = PartialRequest(..partial, stdin_done: True)
      maybe_ready(buffer, updated)
    }
    _ -> {
      let updated = case partial.stdin_overflow {
        True -> partial
        False -> {
          let combined = <<partial.stdin:bits, data:bits>>
          case bit_array.byte_size(combined) > max_body_size {
            True -> PartialRequest(..partial, stdin: <<>>, stdin_overflow: True)
            False -> PartialRequest(..partial, stdin: combined)
          }
        }
      }
      #(Receiving(buffer, updated), bytes_tree.new(), option.None)
    }
  }
}

fn apply_abort(partial: PartialRequest) -> #(State, BytesTree, Option(Action)) {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: partial.request_id,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
  #(
    Idle(<<>>),
    reply,
    option.Some(next_action_after_request(partial.keep_conn)),
  )
}

fn next_action_after_request(keep_conn: Bool) -> Action {
  case keep_conn {
    True -> WaitForMore
    False -> CloseConnection
  }
}

fn apply_get_values(
  state: State,
  names: List(String),
) -> #(State, BytesTree, Option(Action)) {
  let pairs = list.filter_map(names, lookup_capability)
  let reply = protocol.encode_record(protocol.GetValuesResult(pairs:))
  #(state, reply, option.Some(WaitForMore))
}

fn lookup_capability(name: String) -> Result(#(String, String), Nil) {
  case name {
    "FCGI_MAX_CONNS" -> Ok(#(name, "1"))
    "FCGI_MAX_REQS" -> Ok(#(name, "1"))
    "FCGI_MPXS_CONNS" -> Ok(#(name, "0"))
    _ -> Error(Nil)
  }
}

fn apply_unknown_type_record(
  state: State,
  request_id: Int,
  type_byte: Int,
) -> #(State, BytesTree, Option(Action)) {
  case request_id {
    0 -> {
      let reply = protocol.encode_record(protocol.UnknownType(type_byte:))
      #(state, reply, option.Some(WaitForMore))
    }
    _ -> #(state, bytes_tree.new(), option.None)
  }
}

fn maybe_ready(
  buffer: BitArray,
  partial: PartialRequest,
) -> #(State, BytesTree, Option(Action)) {
  case partial.params_done && partial.stdin_done {
    True -> finalize_request(partial)
    False -> #(Receiving(buffer, partial), bytes_tree.new(), option.None)
  }
}

fn finalize_request(
  partial: PartialRequest,
) -> #(State, BytesTree, Option(Action)) {
  case partial.stdin_overflow || partial.params_overflow {
    True -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: partial.request_id,
          app_status: 0,
          protocol_status: protocol.Overloaded,
        ))
      #(
        Idle(<<>>),
        reply,
        option.Some(next_action_after_request(partial.keep_conn)),
      )
    }
    False -> {
      let action =
        ReadyForHandler(
          request_id: partial.request_id,
          params: partial.params,
          body: partial.stdin,
          keep_conn: partial.keep_conn,
        )
      #(Idle(<<>>), bytes_tree.new(), option.Some(action))
    }
  }
}

pub fn encode_response_header(
  request_id: Int,
  resp: Response(body),
) -> BytesTree {
  let header_block = render_header_block(resp)
  encode_stdout_payload(request_id, <<header_block:utf8>>)
}

pub fn encode_response_body_chunk(
  request_id: Int,
  data: BitArray,
) -> BytesTree {
  encode_stdout_payload(request_id, data)
}

pub fn encode_response_body_tree(
  request_id: Int,
  data: BytesTree,
) -> BytesTree {
  let size = bytes_tree.byte_size(data)
  use <- bool.guard(when: size == 0, return: bytes_tree.new())
  use <- bool.guard(
    when: size > protocol.max_record_content_size,
    return: encode_response_body_chunk(
      request_id,
      bytes_tree.to_bit_array(data),
    ),
  )

  protocol.encode_stdout(request_id, data)
}

pub fn encode_response_terminator(request_id: Int) -> BytesTree {
  let terminator = protocol.Stdout(request_id, <<>>)
  let end =
    protocol.EndRequest(
      request_id:,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    )
  encode_records([terminator, end])
}

fn encode_stdout_payload(request_id: Int, payload: BitArray) -> BytesTree {
  case bit_array.byte_size(payload) {
    0 -> bytes_tree.new()
    _ -> encode_records(protocol.chunk_stdout(request_id, payload))
  }
}

fn encode_records(records: List(protocol.Outgoing)) -> BytesTree {
  list.fold(records, bytes_tree.new(), fn(acc, record) {
    bytes_tree.append_tree(acc, protocol.encode_record(record))
  })
}

fn render_header_block(resp: Response(anything)) -> String {
  let status_line = render_status_line(resp.status)
  let headers =
    resp.headers
    |> list.filter(fn(h) { is_safe_header(h.0, h.1) })
    |> list.map(fn(h) { h.0 <> ": " <> h.1 <> "\r\n" })
    |> string.concat
  status_line <> headers <> "\r\n"
}

fn render_status_line(status: Int) -> String {
  case reason_phrase(status) {
    "" -> "Status: " <> int.to_string(status) <> "\r\n"
    phrase -> "Status: " <> int.to_string(status) <> " " <> phrase <> "\r\n"
  }
}

fn is_safe_header(name: String, value: String) -> Bool {
  !contains_crlf(name) && !contains_crlf(value)
}

fn contains_crlf(value: String) -> Bool {
  string.contains(value, "\r") || string.contains(value, "\n")
}

fn reason_phrase(status: Int) -> String {
  case status {
    100 -> "Continue"
    101 -> "Switching Protocols"
    200 -> "OK"
    201 -> "Created"
    202 -> "Accepted"
    204 -> "No Content"
    301 -> "Moved Permanently"
    302 -> "Found"
    303 -> "See Other"
    304 -> "Not Modified"
    307 -> "Temporary Redirect"
    308 -> "Permanent Redirect"
    400 -> "Bad Request"
    401 -> "Unauthorized"
    403 -> "Forbidden"
    404 -> "Not Found"
    405 -> "Method Not Allowed"
    409 -> "Conflict"
    410 -> "Gone"
    413 -> "Content Too Large"
    415 -> "Unsupported Media Type"
    422 -> "Unprocessable Content"
    429 -> "Too Many Requests"
    500 -> "Internal Server Error"
    501 -> "Not Implemented"
    502 -> "Bad Gateway"
    503 -> "Service Unavailable"
    504 -> "Gateway Timeout"
    _ -> ""
  }
}
