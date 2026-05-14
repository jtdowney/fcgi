import gleam/bytes_tree.{type BytesTree}
import gleam/result
import support/sockets.{type Socket, type SocketError}

const idle_timeout_ms = 50

@external(erlang, "fcgi_test_ffi", "connect_tcp")
pub fn connect_tcp(host: String, port: Int) -> Result(Socket, SocketError)

@external(erlang, "fcgi_test_ffi", "connect_unix")
pub fn connect_unix(path: String) -> Result(Socket, SocketError)

@external(erlang, "fcgi_test_ffi", "socket_port")
pub fn socket_port(socket: Socket) -> Result(Int, SocketError)

pub fn recv_all(
  socket: Socket,
  timeout_ms: Int,
) -> Result(BitArray, SocketError) {
  recv_all_loop(socket, timeout_ms, bytes_tree.new())
  |> result.map(bytes_tree.to_bit_array)
}

fn recv_all_loop(
  socket: Socket,
  timeout_ms: Int,
  acc: BytesTree,
) -> Result(BytesTree, SocketError) {
  case sockets.recv(socket, 0, timeout_ms) {
    Ok(data) ->
      recv_all_loop(socket, idle_timeout_ms, bytes_tree.append(acc, data))
    Error(_) -> Ok(acc)
  }
}
