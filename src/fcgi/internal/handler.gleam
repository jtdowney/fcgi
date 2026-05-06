import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/string

pub fn encode_stdout_chunk(request_id: Int, payload: BitArray) -> BytesTree {
  case bit_array.byte_size(payload) {
    0 -> bytes_tree.new()
    _ -> encode_records(protocol.chunk_stdout(request_id, payload))
  }
}

pub fn encode_response_body_tree(
  request_id: Int,
  data: BytesTree,
) -> BytesTree {
  let size = bytes_tree.byte_size(data)
  use <- bool.guard(when: size == 0, return: bytes_tree.new())
  use <- bool.guard(
    when: size > protocol.max_record_content_size,
    return: encode_stdout_chunk(request_id, bytes_tree.to_bit_array(data)),
  )

  protocol.encode_stdout(request_id, data)
}

pub fn encode_response_header(
  request_id: Int,
  resp: Response(body),
) -> BytesTree {
  let header_block = render_header_block(resp)
  encode_stdout_chunk(request_id, <<header_block:utf8>>)
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

fn encode_records(records: List(protocol.Outgoing)) -> BytesTree {
  list.fold(records, bytes_tree.new(), fn(acc, record) {
    bytes_tree.append_tree(acc, protocol.encode_record(record))
  })
}

fn render_header_block(resp: Response(anything)) -> String {
  let status_line = "Status: " <> int.to_string(resp.status) <> "\r\n"
  let headers =
    resp.headers
    |> list.filter(fn(h) { is_safe_header(h.0, h.1) })
    |> list.map(fn(h) { h.0 <> ": " <> h.1 <> "\r\n" })
    |> string.concat
  status_line <> headers <> "\r\n"
}

fn is_safe_header(name: String, value: String) -> Bool {
  !contains_crlf(name) && !contains_crlf(value)
}

fn contains_crlf(value: String) -> Bool {
  string.contains(value, "\r") || string.contains(value, "\n")
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

pub type PartialRequest {
  PartialRequest(
    request_id: Int,
    keep_conn: Bool,
    params: BitArray,
    params_done: Bool,
    stdin: BitArray,
    stdin_done: Bool,
    overflow: Bool,
  )
}

pub type State {
  Idle(buffer: BitArray)
  Receiving(buffer: BitArray, partial: PartialRequest)
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

pub fn new() -> State {
  Idle(<<>>)
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
    protocol.Parsed(record, rest) -> {
      let #(new_state, new_outgoing, action) =
        apply_record(state, record, max_body_size, max_params_size)
      let advanced = with_buffer(new_state, rest)
      let combined = bytes_tree.append_tree(outgoing, new_outgoing)
      case action {
        WaitForMore ->
          feed_loop(advanced, combined, max_body_size, max_params_size)
        _ -> Outcome(state: advanced, outgoing: combined, action:)
      }
    }
  }
}

fn apply_record(
  state: State,
  record: protocol.Incoming,
  max_body_size: Int,
  max_params_size: Int,
) -> #(State, BytesTree, Action) {
  case state, record {
    _, protocol.BeginRequest(0, _, _) -> #(
      state,
      bytes_tree.new(),
      CloseConnection,
    )
    Idle(buffer), protocol.BeginRequest(id, role, keep) ->
      apply_begin_request_idle(buffer, id, role, keep)
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
    _, _ -> #(state, bytes_tree.new(), WaitForMore)
  }
}

fn apply_begin_request_idle(
  buffer: BitArray,
  id: Int,
  role: Int,
  keep: Bool,
) -> #(State, BytesTree, Action) {
  case role == protocol.responder_role {
    True -> {
      let partial =
        PartialRequest(
          request_id: id,
          keep_conn: keep,
          params: <<>>,
          params_done: False,
          stdin: <<>>,
          stdin_done: False,
          overflow: False,
        )
      #(Receiving(buffer, partial), bytes_tree.new(), WaitForMore)
    }
    False -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: id,
          app_status: 0,
          protocol_status: protocol.UnknownRole,
        ))
      #(Idle(buffer), reply, action_after_request(keep))
    }
  }
}

fn apply_begin_request_busy(
  buffer: BitArray,
  partial: PartialRequest,
  id: Int,
) -> #(State, BytesTree, Action) {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: id,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    ))
  #(Receiving(buffer, partial), reply, WaitForMore)
}

fn apply_params(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_params_size: Int,
) -> #(State, BytesTree, Action) {
  case bit_array.byte_size(data) {
    0 -> maybe_ready(buffer, PartialRequest(..partial, params_done: True))
    _ -> {
      let #(params, overflow) =
        merge_input(partial.params, partial.overflow, data, max_params_size)
      let updated = PartialRequest(..partial, params:, overflow:)
      #(Receiving(buffer, updated), bytes_tree.new(), WaitForMore)
    }
  }
}

fn apply_stdin(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> #(State, BytesTree, Action) {
  case bit_array.byte_size(data) {
    0 -> maybe_ready(buffer, PartialRequest(..partial, stdin_done: True))
    _ -> {
      let #(stdin, overflow) =
        merge_input(partial.stdin, partial.overflow, data, max_body_size)
      let updated = PartialRequest(..partial, stdin:, overflow:)
      #(Receiving(buffer, updated), bytes_tree.new(), WaitForMore)
    }
  }
}

fn merge_input(
  current: BitArray,
  overflow: Bool,
  data: BitArray,
  max: Int,
) -> #(BitArray, Bool) {
  use <- bool.guard(when: overflow, return: #(<<>>, True))
  let combined = <<current:bits, data:bits>>
  case bit_array.byte_size(combined) > max {
    True -> #(<<>>, True)
    False -> #(combined, False)
  }
}

fn apply_abort(partial: PartialRequest) -> #(State, BytesTree, Action) {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: partial.request_id,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
  #(Idle(<<>>), reply, action_after_request(partial.keep_conn))
}

fn apply_get_values(
  state: State,
  names: List(String),
) -> #(State, BytesTree, Action) {
  let pairs = list.filter_map(names, lookup_capability)
  let reply = protocol.encode_record(protocol.GetValuesResult(pairs:))
  #(state, reply, WaitForMore)
}

fn apply_unknown_type_record(
  state: State,
  request_id: Int,
  type_byte: Int,
) -> #(State, BytesTree, Action) {
  case request_id {
    0 -> {
      let reply = protocol.encode_record(protocol.UnknownType(type_byte:))
      #(state, reply, WaitForMore)
    }
    _ -> #(state, bytes_tree.new(), WaitForMore)
  }
}

// Static informational values reported in response to FCGI_GET_VALUES.
// The server accepts unbounded concurrent connections (limited only by OS
// resources) and does not multiplex requests on a single connection, so the
// max-conns and max-reqs values are conservative round numbers rather than
// runtime-derived caps.
fn lookup_capability(name: String) -> Result(#(String, String), Nil) {
  case name {
    "FCGI_MAX_CONNS" -> Ok(#(name, "1000"))
    "FCGI_MAX_REQS" -> Ok(#(name, "1000"))
    "FCGI_MPXS_CONNS" -> Ok(#(name, "0"))
    _ -> Error(Nil)
  }
}

fn action_after_request(keep_conn: Bool) -> Action {
  case keep_conn {
    True -> WaitForMore
    False -> CloseConnection
  }
}

fn maybe_ready(
  buffer: BitArray,
  partial: PartialRequest,
) -> #(State, BytesTree, Action) {
  case partial.params_done && partial.stdin_done {
    True -> finalize_request(partial)
    False -> #(Receiving(buffer, partial), bytes_tree.new(), WaitForMore)
  }
}

fn finalize_request(partial: PartialRequest) -> #(State, BytesTree, Action) {
  case partial.overflow {
    True -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: partial.request_id,
          app_status: 0,
          protocol_status: protocol.Overloaded,
        ))
      #(Idle(<<>>), reply, action_after_request(partial.keep_conn))
    }
    False -> {
      let action =
        ReadyForHandler(
          request_id: partial.request_id,
          params: partial.params,
          body: partial.stdin,
          keep_conn: partial.keep_conn,
        )
      #(Idle(<<>>), bytes_tree.new(), action)
    }
  }
}
