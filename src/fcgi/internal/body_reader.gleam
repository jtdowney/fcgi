import fcgi/internal/handler
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/erlang/process
import gleam/option.{type Option, None, Some}
import gleam/result

pub type ReadStep {
  Done
  More(data: BitArray, consume: fn() -> ReadStep)
  TooLarge
  Disconnected
  Timeout
}

pub type RecvOutcome {
  RecvClosed
  RecvData(data: BitArray)
  RecvTimeout
}

pub opaque type Tracker {
  Tracker(subject: process.Subject(Snapshot))
}

type Context {
  Context(
    state: handler.State,
    pending: BitArray,
    finished: Bool,
    overflowed: Bool,
    max_body_size: Int,
    recv: fn() -> RecvOutcome,
    send: fn(BytesTree) -> Nil,
    tracker: Option(process.Subject(Snapshot)),
  )
}

type Snapshot {
  Snapshot(state: handler.State, finished: Bool, overflowed: Bool)
}

pub fn finalize(
  tracker tracker: Tracker,
  max_body_size max_body_size: Int,
  recv recv: fn() -> RecvOutcome,
  send send: fn(BytesTree) -> Nil,
) -> Result(handler.State, Nil) {
  let Tracker(subject) = tracker
  use initial <- result.try(
    process.receive(subject, 0)
    |> result.replace_error(Nil),
  )
  let final = drain_tracker_loop(subject, initial)
  drain_body_loop(final, max_body_size, recv, send)
}

pub fn start(
  state state: handler.State,
  events events: List(handler.Event),
  max_body_size max_body_size: Int,
  recv recv: fn() -> RecvOutcome,
  send send: fn(BytesTree) -> Nil,
) -> #(fn() -> ReadStep, Option(Tracker)) {
  let #(data, ended, overflowed) =
    collect_body_events(events, <<>>, False, False)
  let subject = case ended || overflowed {
    True -> None
    False -> Some(process.new_subject())
  }
  let ctx =
    Context(
      state:,
      pending: data,
      finished: ended,
      overflowed:,
      max_body_size:,
      recv:,
      send:,
      tracker: subject,
    )
  send_snapshot(ctx)
  #(fn() { read_step(ctx) }, option.map(subject, Tracker))
}

fn collect_body_events(
  events: List(handler.Event),
  data: BitArray,
  ended: Bool,
  overflowed: Bool,
) -> #(BitArray, Bool, Bool) {
  case events {
    [] -> #(data, ended, overflowed)
    [handler.BodyChunk(chunk), ..rest] ->
      collect_body_events(rest, <<data:bits, chunk:bits>>, ended, overflowed)
    [handler.BodyEnd, ..rest] ->
      collect_body_events(rest, data, True, overflowed)
    [handler.BodyTooLarge, ..rest] ->
      collect_body_events(rest, data, True, True)
    [handler.Start(_, _, _), ..rest] ->
      collect_body_events(rest, data, ended, overflowed)
  }
}

fn deliver_pending(ctx: Context) -> ReadStep {
  let next_ctx = Context(..ctx, pending: <<>>)
  send_snapshot(next_ctx)
  More(data: ctx.pending, consume: fn() { read_step(next_ctx) })
}

fn drain_body_loop(
  snap: Snapshot,
  max_body_size: Int,
  recv: fn() -> RecvOutcome,
  send: fn(BytesTree) -> Nil,
) -> Result(handler.State, Nil) {
  use <- bool.guard(when: snap.overflowed, return: Error(Nil))
  use <- bool.guard(when: snap.finished, return: Ok(snap.state))
  case recv() {
    RecvClosed -> Error(Nil)
    RecvTimeout -> Error(Nil)
    RecvData(more) -> {
      let outcome = handler.step(snap.state, bytes: more, max_body_size:)
      send(outcome.outgoing)
      let #(_data, ended, overflowed) =
        collect_body_events(outcome.events, <<>>, False, False)
      let next =
        Snapshot(
          state: outcome.state,
          finished: snap.finished || ended,
          overflowed: snap.overflowed || overflowed,
        )
      drain_body_loop(next, max_body_size, recv, send)
    }
  }
}

fn drain_tracker_loop(
  subject: process.Subject(Snapshot),
  latest: Snapshot,
) -> Snapshot {
  case process.receive(subject, 0) {
    Error(_) -> latest
    Ok(snap) -> drain_tracker_loop(subject, snap)
  }
}

fn pull_more(ctx: Context) -> ReadStep {
  case ctx.recv() {
    RecvTimeout -> Timeout
    RecvClosed -> Disconnected
    RecvData(more) -> {
      let outcome =
        handler.step(ctx.state, bytes: more, max_body_size: ctx.max_body_size)
      ctx.send(outcome.outgoing)
      let #(new_data, ended, overflowed) =
        collect_body_events(outcome.events, <<>>, False, False)
      let next_ctx =
        Context(
          ..ctx,
          state: outcome.state,
          pending: <<ctx.pending:bits, new_data:bits>>,
          finished: ctx.finished || ended,
          overflowed: ctx.overflowed || overflowed,
        )
      send_snapshot(next_ctx)
      read_step(next_ctx)
    }
  }
}

fn send_snapshot(ctx: Context) -> Nil {
  case ctx.tracker {
    None -> Nil
    Some(subject) ->
      process.send(
        subject,
        Snapshot(
          state: ctx.state,
          finished: ctx.finished,
          overflowed: ctx.overflowed,
        ),
      )
  }
}

fn read_step(ctx: Context) -> ReadStep {
  use <- bool.guard(when: ctx.overflowed, return: TooLarge)
  let pending_size = bit_array.byte_size(ctx.pending)
  case pending_size, ctx.finished {
    0, True -> Done
    0, False -> pull_more(ctx)
    _, _ -> deliver_pending(ctx)
  }
}
