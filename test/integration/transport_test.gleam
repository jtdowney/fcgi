import gleam/erlang/process
import simplifile
import support/helpers
import support/sockets
import support/test_client

pub fn listen_unix_creates_socket_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(socket) = sockets.listen_unix(path)
  let assert Ok(info) = simplifile.file_info(path)
  assert simplifile.file_info_type(info) == simplifile.Other
  sockets.close_socket(socket)
}

pub fn unix_round_trip_echoes_bytes_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = sockets.listen_unix(path)

  process.spawn(fn() {
    let assert Ok(server) = sockets.accept(listen_sock)
    let assert Ok(bytes) = sockets.recv(server, 5, 1000)
    let assert Ok(_) = sockets.send_bits(server, bytes)
    sockets.close_socket(server)
  })

  let assert Ok(client) = test_client.connect_unix(path)
  let assert Ok(_) = sockets.send_bits(client, <<"hello":utf8>>)
  let assert Ok(echoed) = sockets.recv(client, 5, 1000)
  assert echoed == <<"hello":utf8>>

  sockets.close_socket(client)
  sockets.close_socket(listen_sock)
}

pub fn listen_unix_fails_when_path_exists_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "stale")
  let assert Error(sockets.PathExists(reported)) = sockets.listen_unix(path)
  assert reported == path
}

pub fn listen_unix_fails_when_server_is_live_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = sockets.listen_unix(path)
  let assert Error(sockets.PathExists(reported)) = sockets.listen_unix(path)
  assert reported == path
  sockets.close_socket(listen_sock)
}

pub fn listen_unix_replaces_stale_socket_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(first_sock) = sockets.listen_unix(path)
  sockets.close_socket(first_sock)
  let assert Ok(info) = simplifile.file_info(path)
  assert simplifile.file_info_type(info) == simplifile.Other

  let assert Ok(listen_sock) = sockets.listen_unix(path)
  let assert Ok(client) = test_client.connect_unix(path)

  sockets.close_socket(client)
  sockets.close_socket(listen_sock)
}

pub fn listen_tcp_binds_loopback_test() {
  let assert Ok(socket) = sockets.bind_tcp("127.0.0.1", 0)
  let assert Ok(port) = test_client.socket_port(socket)
  assert port > 0
  sockets.close_socket(socket)
}

pub fn tcp_round_trip_echoes_bytes_test() {
  let assert Ok(listen_sock) = sockets.bind_tcp("127.0.0.1", 0)
  let assert Ok(port) = test_client.socket_port(listen_sock)

  process.spawn(fn() {
    let assert Ok(server) = sockets.accept(listen_sock)
    let assert Ok(bytes) = sockets.recv(server, 5, 1000)
    let assert Ok(_) = sockets.send_bits(server, bytes)
    sockets.close_socket(server)
  })

  let assert Ok(client) = test_client.connect_tcp("127.0.0.1", port)
  let assert Ok(_) = sockets.send_bits(client, <<"hello":utf8>>)
  let assert Ok(echoed) = sockets.recv(client, 5, 1000)
  assert echoed == <<"hello":utf8>>

  sockets.close_socket(client)
  sockets.close_socket(listen_sock)
}

pub fn listen_tcp_rejects_invalid_host_test() {
  let assert Error(_) = sockets.bind_tcp("not an address", 0)
}

pub fn delete_path_removes_existing_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "hi")
  sockets.delete_path(path)
  assert simplifile.is_file(path) == Ok(False)
}

pub fn delete_path_is_noop_when_missing_test() {
  use path <- helpers.with_temp_socket_path
  sockets.delete_path(path)
  assert simplifile.is_file(path) == Ok(False)
}
