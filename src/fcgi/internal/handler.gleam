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
  let size = bytes_tree.byte_size(payload)
  use <- bool.guard(when: size == 0, return: bytes_tree.new())
  case size <= protocol.max_record_content_size {
    True -> protocol.encode_stdout_record(request_id, payload)
    False ->
      encode_records(protocol.chunk_stdout(
        request_id,
        bytes_tree.to_bit_array(payload),
      ))
  }
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

pub type State {
  Idle(buffer: BitArray)
  Receiving(ReceivingState)
}

pub type ReceivingState {
  ReceivingState(
    buffer: BitArray,
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
  let combined = case state {
    Idle(buf) -> Idle(<<buf:bits, bytes:bits>>)
    Receiving(recv) ->
      Receiving(ReceivingState(..recv, buffer: <<recv.buffer:bits, bytes:bits>>))
  }
  step_loop(combined, bytes_tree.new(), [], max_body_size)
}

fn step_loop(
  state: State,
  outgoing: BytesTree,
  events_rev: List(Event),
  max_body_size: Int,
) -> Outcome {
  let buffer = case state {
    Idle(buf) -> buf
    Receiving(recv) -> recv.buffer
  }
  case protocol.parse_record(buffer) {
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
      let advanced = case action.state {
        Idle(_) -> Idle(rest)
        Receiving(recv) -> Receiving(ReceivingState(..recv, buffer: rest))
      }
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

fn handle_abort(request_id: Int) -> Action {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id:,
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

fn handle_begin_request_busy(recv: ReceivingState, id: Int) -> Action {
  let reply =
    protocol.encode_record(protocol.EndRequest(
      request_id: id,
      app_status: 0,
      protocol_status: protocol.CantMultiplexConnection,
    ))
  Action(
    state: Receiving(recv),
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
    True ->
      Action(
        state: Receiving(ReceivingState(
          buffer:,
          request_id: id,
          keep_conn: keep,
          params: <<>>,
          started: False,
          stdin_received: 0,
          stdin_done: False,
          body_overflowed: False,
          params_overflowed: False,
        )),
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.None,
      )
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

fn handle_params(recv: ReceivingState, data: BitArray) -> Action {
  case bit_array.byte_size(data) {
    0 -> finish_params(recv)
    _ -> {
      let #(params, overflow) =
        merge_input(
          recv.params,
          recv.params_overflowed,
          data,
          protocol.max_record_content_size,
        )
      Action(
        state: Receiving(
          ReceivingState(..recv, params:, params_overflowed: overflow),
        ),
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.None,
      )
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

    Receiving(recv), protocol.BeginRequest(id, _, _) ->
      handle_begin_request_busy(recv, id)
    Receiving(recv), protocol.Params(id, data) if id == recv.request_id ->
      handle_params(recv, data)
    Receiving(recv), protocol.Stdin(id, data) if id == recv.request_id ->
      handle_stdin(recv, data, max_body_size)
    Receiving(recv), protocol.AbortRequest(id) if id == recv.request_id ->
      handle_abort(recv.request_id)

    _, protocol.GetValues(names) -> handle_get_values(state, names)
    _, protocol.IncomingUnknown(0, type_byte) -> {
      let reply = protocol.encode_record(protocol.UnknownType(type_byte:))
      Action(state:, outgoing: reply, events: [], terminate: option.None)
    }

    _, _ ->
      Action(
        state:,
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.None,
      )
  }
}

fn handle_stdin(
  recv: ReceivingState,
  data: BitArray,
  max_body_size: Int,
) -> Action {
  case bit_array.byte_size(data) {
    0 -> finish_stdin(recv)
    _ -> handle_stdin_chunk(recv, data, max_body_size)
  }
}

fn handle_stdin_chunk(
  recv: ReceivingState,
  data: BitArray,
  max_body_size: Int,
) -> Action {
  use <- bool.guard(
    when: recv.body_overflowed,
    return: Action(
      state: Receiving(recv),
      outgoing: bytes_tree.new(),
      events: [],
      terminate: option.None,
    ),
  )
  let new_total = recv.stdin_received + bit_array.byte_size(data)
  case new_total > max_body_size {
    True -> handle_stdin_overflow(recv)
    False -> handle_stdin_in_bounds(recv, data, new_total)
  }
}

fn handle_stdin_in_bounds(
  recv: ReceivingState,
  data: BitArray,
  new_total: Int,
) -> Action {
  let updated = Receiving(ReceivingState(..recv, stdin_received: new_total))
  case recv.started {
    True ->
      Action(
        state: updated,
        outgoing: bytes_tree.new(),
        events: [BodyChunk(data)],
        terminate: option.None,
      )
    False ->
      Action(
        state: updated,
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.None,
      )
  }
}

fn handle_stdin_overflow(recv: ReceivingState) -> Action {
  case recv.started {
    True ->
      Action(
        state: Receiving(ReceivingState(..recv, body_overflowed: True)),
        outgoing: bytes_tree.new(),
        events: [BodyTooLarge],
        terminate: option.None,
      )
    False ->
      Action(
        state: Idle(recv.buffer),
        outgoing: encode_overloaded_end(recv.request_id),
        events: [],
        terminate: option.Some(CloseConnection),
      )
  }
}

fn emit_start(recv: ReceivingState) -> Action {
  let start_event =
    Start(
      request_id: recv.request_id,
      params: recv.params,
      keep_conn: recv.keep_conn,
    )
  case recv.stdin_done {
    True ->
      Action(
        state: Idle(recv.buffer),
        outgoing: bytes_tree.new(),
        events: [start_event, BodyEnd],
        terminate: option.None,
      )
    False ->
      Action(
        state: Receiving(ReceivingState(..recv, started: True)),
        outgoing: bytes_tree.new(),
        events: [start_event],
        terminate: option.None,
      )
  }
}

fn finish_params(recv: ReceivingState) -> Action {
  case recv.params_overflowed {
    True ->
      Action(
        state: Idle(<<>>),
        outgoing: encode_overloaded_end(recv.request_id),
        events: [],
        terminate: option.Some(CloseConnection),
      )
    False -> emit_start(recv)
  }
}

fn finish_stdin(recv: ReceivingState) -> Action {
  case recv.started {
    True ->
      Action(
        state: Idle(recv.buffer),
        outgoing: bytes_tree.new(),
        events: [BodyEnd],
        terminate: option.None,
      )
    False ->
      Action(
        state: Receiving(ReceivingState(..recv, stdin_done: True)),
        outgoing: bytes_tree.new(),
        events: [],
        terminate: option.None,
      )
  }
}

fn is_request_boundary(before: State, after_state: State) -> Bool {
  case before, after_state {
    Receiving(_), Idle(_) -> True
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
