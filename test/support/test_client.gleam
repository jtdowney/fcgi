import fcgi/internal/connection
import gleam/bytes_tree.{type BytesTree}
import gleam/result

const idle_timeout_ms = 50

@external(erlang, "fcgi_test_ffi", "connect")
pub fn connect(
  path: String,
) -> Result(connection.Socket, connection.SocketError)

pub fn recv_all(
  socket: connection.Socket,
  timeout_ms: Int,
) -> Result(BitArray, connection.SocketError) {
  recv_all_loop(socket, timeout_ms, bytes_tree.new())
  |> result.map(bytes_tree.to_bit_array)
}

fn recv_all_loop(
  socket: connection.Socket,
  timeout_ms: Int,
  acc: BytesTree,
) -> Result(BytesTree, connection.SocketError) {
  case connection.recv(socket, 0, timeout_ms) {
    Ok(data) ->
      recv_all_loop(socket, idle_timeout_ms, bytes_tree.append(acc, data))
    Error(_) -> Ok(acc)
  }
}
