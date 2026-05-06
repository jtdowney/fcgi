import fcgi/internal/connection
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response
import gleam/option
import gleam/string
import simplifile
import support/helpers
import support/test_client

pub fn connection_actor_serves_one_request_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = connection.listen(path)

  process.spawn(fn() {
    let assert Ok(server) = connection.accept(listen_sock)
    let handler = fn(_req: Request(connection.Connection)) {
      response.new(200)
      |> response.set_header("content-type", "text/plain")
      |> response.set_body(connection.Bytes(bytes_tree.from_string("ok")))
    }
    let spec =
      connection.Spec(
        socket: server,
        max_body_size: 10 * 1024 * 1024,
        handler: handler,
      )
    let assert Ok(started) = connection.start(spec)
    let assert Ok(_) = connection.controlling_process(server, started.pid)
    let assert Ok(_) = connection.setopts_active_once(server)
    Nil
  })

  let assert Ok(client) = test_client.connect(path)
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "localhost"),
        #("PATH_INFO", "/"),
      ],
      body: <<>>,
      keep_conn: False,
    )
  let assert Ok(_) = connection.send(client, request_bytes)

  let received = helpers.recv_until_closed(client)
  connection.close_socket(client)
  connection.close_socket(listen_sock)

  let assert Ok(records) = helpers.decode_all_records(received)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "ok"
}

pub fn connection_actor_stops_on_socket_closed_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = connection.listen(path)

  let parent = process.new_subject()

  process.spawn(fn() {
    let assert Ok(server) = connection.accept(listen_sock)
    let handler = fn(_req: Request(connection.Connection)) {
      response.new(200)
      |> response.set_body(connection.Bytes(bytes_tree.from_string("never")))
    }
    let spec =
      connection.Spec(
        socket: server,
        max_body_size: 10 * 1024 * 1024,
        handler: handler,
      )
    let assert Ok(started) = connection.start(spec)
    let assert Ok(_) = connection.controlling_process(server, started.pid)
    let assert Ok(_) = connection.setopts_active_once(server)
    process.send(parent, started.pid)
  })

  let assert Ok(client) = test_client.connect(path)
  let assert Ok(actor_pid) = process.receive(parent, 1000)

  connection.close_socket(client)
  connection.close_socket(listen_sock)

  let monitor = process.monitor(actor_pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let assert Ok(_down) = process.selector_receive(selector, 500)
}

pub fn connection_actor_streams_file_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = connection.listen(path)

  process.spawn(fn() {
    let assert Ok(server) = connection.accept(listen_sock)
    let handler = fn(_req: Request(connection.Connection)) {
      response.new(200)
      |> response.set_header("content-type", "text/plain")
      |> response.set_body(connection.File(
        "test/fixtures/hello.txt",
        0,
        option.None,
      ))
    }
    let spec =
      connection.Spec(
        socket: server,
        max_body_size: 10 * 1024 * 1024,
        handler: handler,
      )
    let assert Ok(started) = connection.start(spec)
    let assert Ok(_) = connection.controlling_process(server, started.pid)
    let assert Ok(_) = connection.setopts_active_once(server)
    Nil
  })

  let assert Ok(client) = test_client.connect(path)
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "localhost"),
        #("PATH_INFO", "/"),
      ],
      body: <<>>,
      keep_conn: False,
    )
  let assert Ok(_) = connection.send(client, request_bytes)

  let received = helpers.recv_until_closed(client)
  connection.close_socket(client)
  connection.close_socket(listen_sock)

  let assert Ok(records) = helpers.decode_all_records(received)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  let assert Ok(expected) = simplifile.read("test/fixtures/hello.txt")
  assert body == expected
}

pub fn connection_actor_runs_stream_producer_test() {
  use path <- helpers.with_temp_socket_path
  let assert Ok(listen_sock) = connection.listen(path)

  process.spawn(fn() {
    let assert Ok(server) = connection.accept(listen_sock)
    let producer = fn(sender) {
      let _ = connection.send_chunk(sender, <<"chunk1":utf8>>)
      let _ = connection.send_chunk(sender, <<"chunk2":utf8>>)
      Nil
    }
    let handler = fn(_req: Request(connection.Connection)) {
      response.new(200)
      |> response.set_header("content-type", "text/plain")
      |> response.set_body(connection.Stream(producer))
    }
    let spec =
      connection.Spec(
        socket: server,
        max_body_size: 10 * 1024 * 1024,
        handler: handler,
      )
    let assert Ok(started) = connection.start(spec)
    let assert Ok(_) = connection.controlling_process(server, started.pid)
    let assert Ok(_) = connection.setopts_active_once(server)
    Nil
  })

  let assert Ok(client) = test_client.connect(path)
  let request_bytes =
    helpers.request_stream_bytes(
      request_id: 1,
      params: [
        #("REQUEST_METHOD", "GET"),
        #("SERVER_NAME", "localhost"),
        #("PATH_INFO", "/"),
      ],
      body: <<>>,
      keep_conn: False,
    )
  let assert Ok(_) = connection.send(client, request_bytes)

  let received = helpers.recv_until_closed(client)
  connection.close_socket(client)
  connection.close_socket(listen_sock)

  let assert Ok(records) = helpers.decode_all_records(received)
  let stdout = helpers.collect_stdout(records)
  let assert Ok(text) = bit_array.to_string(stdout)
  let assert Ok(#(_headers, body)) = string.split_once(text, "\r\n\r\n")
  assert body == "chunk1chunk2"
}
