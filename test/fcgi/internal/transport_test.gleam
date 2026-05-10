import fcgi/internal/connection
import gleam/erlang/process
import simplifile
import support/helpers
import support/test_client

pub fn unix_listen_creates_socket_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(socket) = connection.listen(path)
  let assert Ok(info) = simplifile.file_info(path)
  assert simplifile.file_info_type(info) == simplifile.Other
  connection.close_socket(socket)
}

pub fn unix_round_trip_echoes_bytes_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = connection.listen(path)

  process.spawn(fn() {
    let assert Ok(server) = connection.accept(listen_sock)
    let assert Ok(bytes) = connection.recv(server, 5, 1000)
    let assert Ok(_) = connection.send_bits(server, bytes)
    connection.close_socket(server)
  })

  let assert Ok(client) = test_client.connect(path)
  let assert Ok(_) = connection.send_bits(client, <<"hello":utf8>>)
  let assert Ok(echoed) = connection.recv(client, 5, 1000)
  assert echoed == <<"hello":utf8>>

  connection.close_socket(client)
  connection.close_socket(listen_sock)
}

pub fn unix_listen_fails_when_path_exists_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "stale")
  let assert Error(connection.PathExists(reported)) = connection.listen(path)
  assert reported == path
}

pub fn delete_path_removes_existing_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(_) = simplifile.write(path, "hi")
  connection.delete_path(path)
  assert simplifile.is_file(path) == Ok(False)
}

pub fn delete_path_is_noop_when_missing_test() {
  use path <- helpers.with_temp_socket_path
  connection.delete_path(path)
  assert simplifile.is_file(path) == Ok(False)
}
