import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/option
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

pub fn encode_overloaded_end(request_id: Int) -> BytesTree {
  protocol.encode_record(protocol.EndRequest(
    request_id:,
    app_status: 0,
    protocol_status: protocol.Overloaded,
  ))
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

pub type Event {
  Start(request_id: Int, params: BitArray, keep_conn: Bool)
  BodyChunk(data: BitArray)
  BodyEnd
  BodyTooLarge
}

pub type Continuation {
  WaitForMore
  CloseConnection
}

pub type Outcome {
  Outcome(
    state: State,
    outgoing: BytesTree,
    events: List(Event),
    continuation: Continuation,
  )
}

pub type PartialRequest {
  PartialRequest(
    request_id: Int,
    keep_conn: Bool,
    params: BitArray,
    params_done: Bool,
    started: Bool,
    pre_start_stdin: BitArray,
    stdin_received: Int,
    stdin_done: Bool,
    body_overflowed: Bool,
    params_overflowed: Bool,
  )
}

pub type State {
  Idle(buffer: BitArray)
  Receiving(buffer: BitArray, partial: PartialRequest)
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
  feed_loop(combined, bytes_tree.new(), [], max_body_size, max_params_size)
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
  events_rev: List(Event),
  max_body_size: Int,
  max_params_size: Int,
) -> Outcome {
  case protocol.parse_record(state.buffer) {
    protocol.NeedMore ->
      Outcome(
        state:,
        outgoing:,
        events: list.reverse(events_rev),
        continuation: WaitForMore,
      )
    protocol.ParseError(_) ->
      Outcome(
        state:,
        outgoing:,
        events: list.reverse(events_rev),
        continuation: CloseConnection,
      )
    protocol.Parsed(record, rest) -> {
      let step = apply_record(state, record, max_body_size, max_params_size)
      let advanced = with_buffer(step.state, rest)
      let combined_outgoing = bytes_tree.append_tree(outgoing, step.outgoing)
      let combined_events =
        list.fold(step.events, events_rev, fn(acc, evt) { [evt, ..acc] })
      case step.terminate, is_request_boundary(state, step.state) {
        option.Some(continuation), _ ->
          Outcome(
            state: advanced,
            outgoing: combined_outgoing,
            events: list.reverse(combined_events),
            continuation:,
          )
        option.None, True ->
          Outcome(
            state: advanced,
            outgoing: combined_outgoing,
            events: list.reverse(combined_events),
            continuation: WaitForMore,
          )
        option.None, False ->
          feed_loop(
            advanced,
            combined_outgoing,
            combined_events,
            max_body_size,
            max_params_size,
          )
      }
    }
  }
}

fn is_request_boundary(before: State, after_state: State) -> Bool {
  case before, after_state {
    Receiving(_, _), Idle(_) -> True
    _, _ -> False
  }
}

type Step {
  Step(
    state: State,
    outgoing: BytesTree,
    events: List(Event),
    terminate: option.Option(Continuation),
  )
}

fn step(state: State) -> Step {
  Step(state:, outgoing: bytes_tree.new(), events: [], terminate: option.None)
}

fn apply_record(
  state: State,
  record: protocol.Incoming,
  max_body_size: Int,
  max_params_size: Int,
) -> Step {
  case state, record {
    _, protocol.BeginRequest(0, _, _) ->
      Step(
        state:,
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.Some(CloseConnection),
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
    _, _ -> step(state)
  }
}

fn apply_begin_request_idle(
  buffer: BitArray,
  id: Int,
  role: Int,
  keep: Bool,
) -> Step {
  case role == protocol.responder_role {
    True -> {
      let partial =
        PartialRequest(
          request_id: id,
          keep_conn: keep,
          params: <<>>,
          params_done: False,
          started: False,
          pre_start_stdin: <<>>,
          stdin_received: 0,
          stdin_done: False,
          body_overflowed: False,
          params_overflowed: False,
        )
      step(Receiving(buffer, partial))
    }
    False -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: id,
          app_status: 0,
          protocol_status: protocol.UnknownRole,
        ))
      Step(
        state: Idle(buffer),
        outgoing: reply,
        events: [],
        terminate: option.Some(CloseConnection),
      )
    }
  }
}

fn apply_begin_request_busy(
  buffer: BitArray,
  partial: PartialRequest,
  id: Int,
) -> Step {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: id,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    ))
  Step(
    state: Receiving(buffer, partial),
    outgoing: reply,
    events: [],
    terminate: option.None,
  )
}

fn apply_params(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_params_size: Int,
) -> Step {
  case bit_array.byte_size(data) {
    0 -> finish_params(buffer, partial)
    _ -> {
      let #(params, overflow) =
        merge_input(
          partial.params,
          partial.params_overflowed,
          data,
          max_params_size,
        )
      let updated =
        PartialRequest(..partial, params:, params_overflowed: overflow)
      step(Receiving(buffer, updated))
    }
  }
}

fn finish_params(buffer: BitArray, partial: PartialRequest) -> Step {
  let updated = PartialRequest(..partial, params_done: True)
  case updated.params_overflowed {
    True -> overloaded_end(<<>>, updated)
    False -> emit_start(buffer, updated)
  }
}

fn emit_start(buffer: BitArray, partial: PartialRequest) -> Step {
  let started = PartialRequest(..partial, started: True, pre_start_stdin: <<>>)
  let start_event =
    Start(
      request_id: partial.request_id,
      params: partial.params,
      keep_conn: partial.keep_conn,
    )
  let body_events = case bit_array.byte_size(partial.pre_start_stdin) {
    0 -> []
    _ -> [BodyChunk(partial.pre_start_stdin)]
  }
  let events = case partial.stdin_done {
    True -> [start_event, ..list.append(body_events, [BodyEnd])]
    False -> [start_event, ..body_events]
  }
  let next_state = case partial.stdin_done {
    True -> Idle(buffer)
    False -> Receiving(buffer, started)
  }
  Step(
    state: next_state,
    outgoing: bytes_tree.new(),
    events:,
    terminate: option.None,
  )
}

fn apply_stdin(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> Step {
  case bit_array.byte_size(data) {
    0 -> finish_stdin(buffer, partial)
    _ -> apply_stdin_chunk(buffer, partial, data, max_body_size)
  }
}

fn finish_stdin(buffer: BitArray, partial: PartialRequest) -> Step {
  let updated = PartialRequest(..partial, stdin_done: True)
  case updated.started {
    True ->
      Step(
        state: Idle(buffer),
        outgoing: bytes_tree.new(),
        events: [BodyEnd],
        terminate: option.None,
      )
    False -> step(Receiving(buffer, updated))
  }
}

fn apply_stdin_chunk(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> Step {
  use <- bool.guard(
    when: partial.body_overflowed,
    return: step(Receiving(buffer, partial)),
  )
  let new_total = partial.stdin_received + bit_array.byte_size(data)
  case new_total > max_body_size {
    True -> apply_stdin_overflow(buffer, partial)
    False -> apply_stdin_in_bounds(buffer, partial, data, new_total)
  }
}

fn apply_stdin_overflow(buffer: BitArray, partial: PartialRequest) -> Step {
  let updated =
    PartialRequest(..partial, body_overflowed: True, pre_start_stdin: <<>>)
  case partial.started {
    True ->
      Step(
        state: Receiving(buffer, updated),
        outgoing: bytes_tree.new(),
        events: [BodyTooLarge],
        terminate: option.None,
      )
    False -> overloaded_end(buffer, partial)
  }
}

fn apply_stdin_in_bounds(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  new_total: Int,
) -> Step {
  case partial.started {
    True -> {
      let updated = PartialRequest(..partial, stdin_received: new_total)
      Step(
        state: Receiving(buffer, updated),
        outgoing: bytes_tree.new(),
        events: [BodyChunk(data)],
        terminate: option.None,
      )
    }
    False -> {
      let combined = <<partial.pre_start_stdin:bits, data:bits>>
      let updated =
        PartialRequest(
          ..partial,
          pre_start_stdin: combined,
          stdin_received: new_total,
        )
      step(Receiving(buffer, updated))
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

fn apply_abort(partial: PartialRequest) -> Step {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: partial.request_id,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
  Step(
    state: Idle(<<>>),
    outgoing: reply,
    events: [],
    terminate: option.Some(CloseConnection),
  )
}

fn apply_get_values(state: State, names: List(String)) -> Step {
  let pairs = list.filter_map(names, lookup_capability)
  let reply = protocol.encode_record(protocol.GetValuesResult(pairs:))
  Step(state:, outgoing: reply, events: [], terminate: option.None)
}

fn apply_unknown_type_record(
  state: State,
  request_id: Int,
  type_byte: Int,
) -> Step {
  case request_id {
    0 -> {
      let reply = protocol.encode_record(protocol.UnknownType(type_byte:))
      Step(state:, outgoing: reply, events: [], terminate: option.None)
    }
    _ -> step(state)
  }
}

fn overloaded_end(buffer: BitArray, partial: PartialRequest) -> Step {
  Step(
    state: Idle(buffer),
    outgoing: encode_overloaded_end(partial.request_id),
    events: [],
    terminate: option.Some(CloseConnection),
  )
}

/// Informational values reported in response to FCGI_GET_VALUES. The server
/// does not multiplex, so MAX_REQS mirrors MAX_CONNS. These are advertised
/// as soft hints; upstream proxies should rely on their own pooling
/// configuration as the authoritative cap.
fn lookup_capability(name: String) -> Result(#(String, String), Nil) {
  case name {
    "FCGI_MAX_CONNS" -> Ok(#(name, "100000"))
    "FCGI_MAX_REQS" -> Ok(#(name, "100000"))
    "FCGI_MPXS_CONNS" -> Ok(#(name, "0"))
    _ -> Error(Nil)
  }
}
