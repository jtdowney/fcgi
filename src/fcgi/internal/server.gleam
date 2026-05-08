import fcgi/internal/connection
import gleam/erlang/atom
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/otp/actor
import gleam/otp/factory_supervisor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result

pub type SpecTemplate {
  SpecTemplate(
    max_body_size: Int,
    body_read_timeout_ms: Int,
    handler: fn(Request(Nil), connection.BodyReader) ->
      Response(connection.ResponseData),
  )
}

pub type Server {
  Server(supervisor_pid: process.Pid, path: String)
}

pub type StartError {
  ListenerError(reason: String)
  SocketPathExists(path: String)
}

pub fn start(
  path: String,
  template: SpecTemplate,
) -> Result(Server, StartError) {
  use socket <- result.try(open_socket(path))

  let factory_name = process.new_name(prefix: "fcgi_server_factory")

  let builder =
    static_supervisor.new(static_supervisor.RestForOne)
    |> static_supervisor.add(path_janitor_supervised(path))
    |> static_supervisor.add(connection_factory_supervised(factory_name))
    |> static_supervisor.add(acceptor_supervised(socket, factory_name, template))

  case static_supervisor.start(builder) {
    Error(error) -> {
      cleanup_socket(socket, path)
      Error(ListenerError(describe_start_error(error)))
    }
    Ok(started) ->
      case connection.controlling_process(socket, started.pid) {
        Ok(Nil) -> Ok(Server(supervisor_pid: started.pid, path:))
        Error(error) -> {
          shutdown_supervisor(started.pid)
          cleanup_socket(socket, path)
          Error(ListenerError(
            "controlling_process failed: " <> describe_transport_error(error),
          ))
        }
      }
  }
}

pub fn stop(server: Server) -> Nil {
  shutdown_supervisor(server.supervisor_pid)
}

type JanitorState {
  JanitorState(path: String)
}

fn path_janitor_supervised(
  path: String,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start_path_janitor(path) })
  |> supervision.restart(supervision.Transient)
}

fn start_path_janitor(path: String) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(_subject) {
    process.trap_exits(True)
    let selector =
      process.new_selector()
      |> process.select_trapped_exits(fn(_msg) { Nil })
    actor.initialised(JanitorState(path:))
    |> actor.selecting(selector)
    |> actor.returning(Nil)
    |> Ok
  })
  |> actor.on_message(fn(state, _message) {
    connection.delete_path(state.path)
    actor.stop()
  })
  |> actor.start
}

fn start_acceptor(
  socket: connection.Socket,
  factory_name: process.Name(factory_supervisor.Message(connection.Spec, Nil)),
  template: SpecTemplate,
) -> actor.StartResult(Nil) {
  let factory = factory_supervisor.get_by_name(factory_name)
  let pid = process.spawn(fn() { accept_loop(socket, factory, template) })
  Ok(actor.Started(pid:, data: Nil))
}

fn acceptor_supervised(
  socket: connection.Socket,
  factory_name: process.Name(factory_supervisor.Message(connection.Spec, Nil)),
  template: SpecTemplate,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start_acceptor(socket, factory_name, template) })
  |> supervision.restart(supervision.Transient)
}

fn accept_loop(
  listen_socket: connection.Socket,
  factory: factory_supervisor.Supervisor(connection.Spec, Nil),
  template: SpecTemplate,
) -> Nil {
  case connection.accept(listen_socket) {
    Error(connection.Posix(reason)) ->
      case atom.to_string(reason) {
        "closed" -> Nil
        other -> panic as { "fcgi acceptor: accept failed: " <> other }
      }
    Error(connection.PathExists(_)) -> Nil
    Ok(client) -> {
      let spec =
        connection.Spec(
          socket: client,
          max_body_size: template.max_body_size,
          body_read_timeout_ms: template.body_read_timeout_ms,
          handler: template.handler,
        )
      case factory_supervisor.start_child(factory, spec) {
        Error(_) -> connection.close_socket(client)
        Ok(started) -> handoff_client(client, started.pid)
      }
      accept_loop(listen_socket, factory, template)
    }
  }
}

fn handoff_client(client: connection.Socket, child_pid: process.Pid) -> Nil {
  case connection.controlling_process(client, child_pid) {
    Ok(Nil) -> Nil
    Error(_) -> {
      connection.close_socket(client)
      process.send_exit(child_pid)
    }
  }
}

fn shutdown_supervisor(pid: process.Pid) -> Nil {
  process.unlink(pid)
  let monitor = process.monitor(pid)
  process.send_abnormal_exit(pid, atom.create("shutdown"))
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let _ = process.selector_receive(selector, 5000)
  Nil
}

fn open_socket(path: String) -> Result(connection.Socket, StartError) {
  case connection.listen(path) {
    Ok(socket) -> Ok(socket)
    Error(connection.PathExists(path)) -> Error(SocketPathExists(path))
    Error(error) ->
      Error(ListenerError("listen failed: " <> describe_transport_error(error)))
  }
}

fn cleanup_socket(socket: connection.Socket, path: String) -> Nil {
  connection.close_socket(socket)
  connection.delete_path(path)
}

fn connection_factory_supervised(
  name: process.Name(factory_supervisor.Message(connection.Spec, Nil)),
) -> supervision.ChildSpecification(
  factory_supervisor.Supervisor(connection.Spec, Nil),
) {
  factory_supervisor.worker_child(connection.start)
  |> factory_supervisor.named(name)
  |> factory_supervisor.supervised
  |> supervision.restart(supervision.Transient)
}

fn describe_start_error(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "supervisor init timeout"
    actor.InitFailed(reason) -> reason
    actor.InitExited(_) -> "supervisor init exited"
  }
}

fn describe_transport_error(error: connection.SocketError) -> String {
  case error {
    connection.PathExists(path) -> "socket path already exists: " <> path
    connection.Posix(reason) -> atom.to_string(reason)
  }
}
