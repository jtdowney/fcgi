import gleam/bytes_tree.{type BytesTree}
import gleam/result
import mug

pub type Socket =
  mug.Socket

const idle_timeout_ms: Int = 50

pub fn connect(host: String, port: Int) -> Result(Socket, mug.ConnectError) {
  mug.new(host, port:)
  |> mug.timeout(milliseconds: 1000)
  |> mug.connect()
}

pub fn send(socket: Socket, bytes: BitArray) -> Result(Nil, mug.Error) {
  mug.send(socket, bytes)
}

pub fn recv_all(
  socket: Socket,
  timeout_ms: Int,
) -> Result(BitArray, mug.Error) {
  recv_all_loop(socket, timeout_ms, bytes_tree.new())
  |> result.map(bytes_tree.to_bit_array)
}

fn recv_all_loop(
  socket: Socket,
  timeout_ms: Int,
  acc: BytesTree,
) -> Result(BytesTree, mug.Error) {
  case mug.receive(socket, timeout_milliseconds: timeout_ms) {
    Ok(data) ->
      recv_all_loop(socket, idle_timeout_ms, bytes_tree.append(acc, data))
    Error(mug.Timeout) | Error(mug.Closed) -> Ok(acc)
    Error(reason) -> Error(reason)
  }
}

pub fn close(socket: Socket) -> Nil {
  let _ = mug.shutdown(socket)
  Nil
}
