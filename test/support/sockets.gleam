import gleam/erlang/atom.{type Atom}

pub type Socket

pub type SocketError {
  PathExists(path: String)
  InvalidHost(host: String)
  Posix(reason: Atom)
}

@external(erlang, "gen_tcp", "accept")
pub fn accept(listen: Socket) -> Result(Socket, Atom)

@external(erlang, "fcgi_ffi", "listen_tcp")
pub fn bind_tcp(host: String, port: Int) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "close_socket")
pub fn close_socket(socket: Socket) -> Nil

@external(erlang, "fcgi_ffi", "delete_path")
pub fn delete_path(path: String) -> Nil

@external(erlang, "fcgi_ffi", "listen_unix")
pub fn listen_unix(path: String) -> Result(Socket, SocketError)

@external(erlang, "fcgi_ffi", "recv")
pub fn recv(
  socket: Socket,
  size: Int,
  timeout_ms: Int,
) -> Result(BitArray, SocketError)

@external(erlang, "fcgi_ffi", "send")
pub fn send_bits(socket: Socket, data: BitArray) -> Result(Nil, SocketError)
