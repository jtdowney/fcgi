import fcgi/internal/protocol
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/option
import gleam/string

pub fn encode_overloaded_end(request_id: Int) -> BytesTree {
  protocol.encode_record(protocol.EndRequest(
    request_id:,
    app_status: 0,
    protocol_status: protocol.Overloaded,
  ))
}

pub fn encode_response_header(
  request_id: Int,
  resp: Response(body),
) -> BytesTree {
  encode_stdout_chunk(request_id, encode_headers(resp))
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

pub fn encode_stdout_chunk(request_id: Int, payload: BytesTree) -> BytesTree {
  use <- bool.guard(
    when: bytes_tree.byte_size(payload) == 0,
    return: bytes_tree.new(),
  )
  encode_records(protocol.chunk_stdout(
    request_id,
    bytes_tree.to_bit_array(payload),
  ))
}

fn contains_crlf(value: String) -> Bool {
  string.contains(value, "\r") || string.contains(value, "\n")
}

fn encode_records(records: List(protocol.Outgoing)) -> BytesTree {
  list.fold(records, bytes_tree.new(), fn(acc, record) {
    bytes_tree.append_tree(acc, protocol.encode_record(record))
  })
}

fn is_safe_header(name: String, value: String) -> Bool {
  !contains_crlf(name) && !contains_crlf(value)
}

fn encode_headers(resp: Response(anything)) -> BytesTree {
  let start =
    bytes_tree.from_string("Status: " <> int.to_string(resp.status) <> "\r\n")
  let with_headers =
    list.fold(resp.headers, start, fn(acc, header) {
      let #(name, value) = header
      case is_safe_header(name, value) {
        True ->
          acc
          |> bytes_tree.append_string(name)
          |> bytes_tree.append_string(": ")
          |> bytes_tree.append_string(value)
          |> bytes_tree.append_string("\r\n")
        False -> acc
      }
    })
  bytes_tree.append_string(with_headers, "\r\n")
}

pub type Continuation {
  WaitForMore
  CloseConnection
}

pub type Event {
  Start(request_id: Int, params: BitArray, keep_conn: Bool)
  BodyChunk(data: BitArray)
  BodyEnd
  BodyTooLarge
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
    started: Bool,
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

type Action {
  Action(
    state: State,
    outgoing: BytesTree,
    events: List(Event),
    terminate: option.Option(Continuation),
  )
}

pub fn step(
  state: State,
  bytes bytes: BitArray,
  max_body_size max_body_size: Int,
) -> Outcome {
  let combined = with_buffer(state, <<state.buffer:bits, bytes:bits>>)
  step_loop(combined, bytes_tree.new(), [], max_body_size)
}

fn step_loop(
  state: State,
  outgoing: BytesTree,
  events_rev: List(Event),
  max_body_size: Int,
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
      let action = handle_record(state, record, max_body_size)
      let advanced = with_buffer(action.state, rest)
      let combined_outgoing = bytes_tree.append_tree(outgoing, action.outgoing)
      let combined_events =
        list.fold(action.events, events_rev, fn(acc, evt) { [evt, ..acc] })
      case action.terminate, is_request_boundary(state, action.state) {
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
          step_loop(advanced, combined_outgoing, combined_events, max_body_size)
      }
    }
  }
}

fn handle_abort(partial: PartialRequest) -> Action {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: partial.request_id,
      app_status: 0,
      protocol_status: protocol.RequestComplete,
    ))
  Action(
    state: Idle(<<>>),
    outgoing: reply,
    events: [],
    terminate: option.Some(CloseConnection),
  )
}

fn handle_begin_request_busy(
  buffer: BitArray,
  partial: PartialRequest,
  id: Int,
) -> Action {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: id,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    ))
  Action(
    state: Receiving(buffer, partial),
    outgoing: reply,
    events: [],
    terminate: option.None,
  )
}

fn handle_begin_request_idle(
  buffer: BitArray,
  id: Int,
  role: Int,
  keep: Bool,
) -> Action {
  case role == protocol.responder_role {
    True -> {
      let partial =
        PartialRequest(
          request_id: id,
          keep_conn: keep,
          params: <<>>,
          started: False,
          stdin_received: 0,
          stdin_done: False,
          body_overflowed: False,
          params_overflowed: False,
        )
      unchanged(Receiving(buffer, partial))
    }
    False -> {
      let reply =
        protocol.encode_record(protocol.EndRequest(
          request_id: id,
          app_status: 0,
          protocol_status: protocol.UnknownRole,
        ))
      Action(
        state: Idle(buffer),
        outgoing: reply,
        events: [],
        terminate: option.Some(CloseConnection),
      )
    }
  }
}

fn handle_get_values(state: State, names: List(String)) -> Action {
  let pairs = list.filter_map(names, lookup_capability)
  let reply = protocol.encode_record(protocol.GetValuesResult(pairs:))
  Action(state:, outgoing: reply, events: [], terminate: option.None)
}

fn handle_params(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
) -> Action {
  case bit_array.byte_size(data) {
    0 -> finish_params(buffer, partial)
    _ -> {
      let #(params, overflow) =
        merge_input(
          partial.params,
          partial.params_overflowed,
          data,
          protocol.max_record_content_size,
        )
      let updated =
        PartialRequest(..partial, params:, params_overflowed: overflow)
      unchanged(Receiving(buffer, updated))
    }
  }
}

fn handle_record(
  state: State,
  record: protocol.Incoming,
  max_body_size: Int,
) -> Action {
  case state, record {
    _, protocol.BeginRequest(0, _, _) ->
      Action(
        state:,
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.Some(CloseConnection),
      )
    Idle(buffer), protocol.BeginRequest(id, role, keep) ->
      handle_begin_request_idle(buffer, id, role, keep)
    Receiving(buffer, partial), protocol.BeginRequest(id, _, _) ->
      handle_begin_request_busy(buffer, partial, id)
    Receiving(buffer, partial), protocol.Params(id, data)
      if id == partial.request_id
    -> handle_params(buffer, partial, data)
    Receiving(buffer, partial), protocol.Stdin(id, data)
      if id == partial.request_id
    -> handle_stdin(buffer, partial, data, max_body_size)
    Receiving(_, partial), protocol.AbortRequest(id)
      if id == partial.request_id
    -> handle_abort(partial)
    _, protocol.GetValues(names) -> handle_get_values(state, names)
    _, protocol.IncomingUnknown(0, type_byte) -> {
      let reply = protocol.encode_record(protocol.UnknownType(type_byte:))
      Action(state:, outgoing: reply, events: [], terminate: option.None)
    }
    _, _ -> unchanged(state)
  }
}

fn handle_stdin(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> Action {
  case bit_array.byte_size(data) {
    0 -> finish_stdin(buffer, partial)
    _ -> handle_stdin_chunk(buffer, partial, data, max_body_size)
  }
}

fn handle_stdin_chunk(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  max_body_size: Int,
) -> Action {
  use <- bool.guard(
    when: partial.body_overflowed,
    return: unchanged(Receiving(buffer, partial)),
  )
  let new_total = partial.stdin_received + bit_array.byte_size(data)
  case new_total > max_body_size {
    True -> handle_stdin_overflow(buffer, partial)
    False -> handle_stdin_in_bounds(buffer, partial, data, new_total)
  }
}

fn handle_stdin_in_bounds(
  buffer: BitArray,
  partial: PartialRequest,
  data: BitArray,
  new_total: Int,
) -> Action {
  let updated = PartialRequest(..partial, stdin_received: new_total)
  case partial.started {
    True ->
      Action(
        state: Receiving(buffer, updated),
        outgoing: bytes_tree.new(),
        events: [BodyChunk(data)],
        terminate: option.None,
      )
    False -> unchanged(Receiving(buffer, updated))
  }
}

fn handle_stdin_overflow(buffer: BitArray, partial: PartialRequest) -> Action {
  let updated = PartialRequest(..partial, body_overflowed: True)
  case partial.started {
    True ->
      Action(
        state: Receiving(buffer, updated),
        outgoing: bytes_tree.new(),
        events: [BodyTooLarge],
        terminate: option.None,
      )
    False -> overloaded_end(buffer, partial)
  }
}

fn emit_start(buffer: BitArray, partial: PartialRequest) -> Action {
  let started = PartialRequest(..partial, started: True)
  let start_event =
    Start(
      request_id: partial.request_id,
      params: partial.params,
      keep_conn: partial.keep_conn,
    )
  let events = case partial.stdin_done {
    True -> [start_event, BodyEnd]
    False -> [start_event]
  }
  let next_state = case partial.stdin_done {
    True -> Idle(buffer)
    False -> Receiving(buffer, started)
  }
  Action(
    state: next_state,
    outgoing: bytes_tree.new(),
    events:,
    terminate: option.None,
  )
}

fn finish_params(buffer: BitArray, partial: PartialRequest) -> Action {
  case partial.params_overflowed {
    True -> overloaded_end(<<>>, partial)
    False -> emit_start(buffer, partial)
  }
}

fn finish_stdin(buffer: BitArray, partial: PartialRequest) -> Action {
  let updated = PartialRequest(..partial, stdin_done: True)
  case updated.started {
    True ->
      Action(
        state: Idle(buffer),
        outgoing: bytes_tree.new(),
        events: [BodyEnd],
        terminate: option.None,
      )
    False -> unchanged(Receiving(buffer, updated))
  }
}

fn is_request_boundary(before: State, after_state: State) -> Bool {
  case before, after_state {
    Receiving(_, _), Idle(_) -> True
    _, _ -> False
  }
}

/// Informational values reported in response to FCGI_GET_VALUES. Only
/// `FCGI_MPXS_CONNS` is answered; `FCGI_MAX_CONNS` and `FCGI_MAX_REQS`
/// are omitted because the server enforces no internal cap.
fn lookup_capability(name: String) -> Result(#(String, String), Nil) {
  case name {
    "FCGI_MPXS_CONNS" -> Ok(#(name, "0"))
    _ -> Error(Nil)
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

fn overloaded_end(buffer: BitArray, partial: PartialRequest) -> Action {
  Action(
    state: Idle(buffer),
    outgoing: encode_overloaded_end(partial.request_id),
    events: [],
    terminate: option.Some(CloseConnection),
  )
}

fn unchanged(state: State) -> Action {
  Action(state:, outgoing: bytes_tree.new(), events: [], terminate: option.None)
}

fn with_buffer(state: State, buffer: BitArray) -> State {
  case state {
    Idle(_) -> Idle(buffer)
    Receiving(_, partial) -> Receiving(buffer, partial)
  }
}
