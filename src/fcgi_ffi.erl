-module(fcgi_ffi).

-include_lib("kernel/include/file.hrl").

-export([validate_file/1, open/1, pread/3, close/1,
         listen/1, socket_close/1, accept/1, send/2,
         setopts_active_once/1, controlling_process/2, recv/3,
         delete_path/1]).

-define(LISTEN_OPTS, [binary, {active, false}, {packet, raw}]).

validate_file(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = directory}} -> {error, is_directory};
        {ok, #file_info{access = none}} -> {error, access_denied};
        {ok, #file_info{access = write}} -> {error, access_denied};
        {ok, _} -> {ok, nil};
        {error, Reason} -> {error, translate_error(Reason)}
    end.

open(Path) ->
    case file:open(Path, [read, raw, binary]) of
        {ok, _} = Reply -> Reply;
        {error, Reason} -> {error, translate_error(Reason)}
    end.

pread(IoDevice, Position, Size) ->
    case file:pread(IoDevice, Position, Size) of
        {ok, _} = Reply -> Reply;
        eof -> {ok, <<>>};
        {error, Reason} -> {error, translate_error(Reason)}
    end.

close(IoDevice) ->
    _ = file:close(IoDevice),
    nil.

translate_error(enoent) -> not_found;
translate_error(eacces) -> access_denied;
translate_error(eperm) -> access_denied;
translate_error(eisdir) -> is_directory;
translate_error(Reason) ->
    {unknown, list_to_binary(io_lib:format("~p", [Reason]))}.

listen(PathBin) ->
    case file:read_file_info(PathBin) of
        {ok, _} -> {error, {path_exists, PathBin}};
        {error, enoent} ->
            wrap_posix(gen_tcp:listen(0, [{ifaddr, {local, PathBin}} | ?LISTEN_OPTS]));
        {error, R} -> {error, {posix, R}}
    end.

socket_close(Socket) ->
    _ = gen_tcp:close(Socket),
    nil.

accept(Listen) ->
    wrap_posix(gen_tcp:accept(Listen)).

send(Socket, Data) ->
    wrap_posix(gen_tcp:send(Socket, Data)).

setopts_active_once(Socket) ->
    wrap_posix(inet:setopts(Socket, [{active, once}])).

controlling_process(Socket, Pid) ->
    wrap_posix(gen_tcp:controlling_process(Socket, Pid)).

recv(Socket, Size, TimeoutMs) ->
    wrap_posix(gen_tcp:recv(Socket, Size, TimeoutMs)).

delete_path(PathBin) ->
    _ = file:delete(PathBin),
    nil.

wrap_posix(ok) -> {ok, nil};
wrap_posix({ok, _} = Reply) -> Reply;
wrap_posix({error, R}) -> {error, {posix, R}}.
