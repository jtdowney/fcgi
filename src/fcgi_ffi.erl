-module(fcgi_ffi).

-include_lib("kernel/include/file.hrl").

-export([open_and_size/1, sendfile/4, listen_tcp/2, listen_unix/1, close_socket/1, send/2,
         controlling_process/2, recv/3, delete_path/1, close_file/1, socket_port/1]).

-define(LISTEN_OPTS, [binary, {active, false}, {packet, raw}, {backlog, 1024}]).

open_and_size(Path) ->
    case file:open(Path, [read, raw, binary]) of
        {ok, IoDevice} ->
            size_and_rewind(IoDevice, Path);
        {error, Reason} ->
            {error, translate_error(Path, Reason)}
    end.

size_and_rewind(IoDevice, Path) ->
    case file:position(IoDevice, eof) of
        {ok, Size} ->
            rewind_with_size(IoDevice, Path, Size);
        {error, Reason} ->
            close_and_translate(IoDevice, Path, Reason)
    end.

rewind_with_size(IoDevice, Path, Size) ->
    case file:position(IoDevice, 0) of
        {ok, 0} ->
            {ok, {IoDevice, Size}};
        {error, Reason} ->
            close_and_translate(IoDevice, Path, Reason)
    end.

close_and_translate(IoDevice, Path, Reason) ->
    _ = file:close(IoDevice),
    {error, translate_error(Path, Reason)}.

sendfile(IoDevice, Socket, Offset, Bytes) ->
    case file:sendfile(IoDevice, Socket, Offset, Bytes, []) of
        {ok, Sent} ->
            {ok, Sent};
        {error, _} ->
            {error, nil}
    end.

listen_tcp(Host, Port) ->
    case inet:parse_address(binary_to_list(Host)) of
        {ok, Ip} ->
            Family =
                case tuple_size(Ip) of
                    4 ->
                        inet;
                    8 ->
                        inet6
                end,
            map_posix(gen_tcp:listen(Port, [Family, {ip, Ip} | ?LISTEN_OPTS]));
        {error, _} ->
            {error, {invalid_host, Host}}
    end.

listen_unix(PathBin) ->
    case file:read_file_info(PathBin) of
        {ok, #file_info{type = other}} ->
            case is_stale_socket(PathBin) of
                true ->
                    _ = file:delete(PathBin),
                    map_posix(gen_tcp:listen(0, [{ifaddr, {local, PathBin}} | ?LISTEN_OPTS]));
                false ->
                    {error, {path_exists, PathBin}}
            end;
        {ok, _} ->
            {error, {path_exists, PathBin}};
        {error, enoent} ->
            map_posix(gen_tcp:listen(0, [{ifaddr, {local, PathBin}} | ?LISTEN_OPTS]));
        {error, Reason} ->
            {error, {posix, Reason}}
    end.

is_stale_socket(PathBin) ->
    case gen_tcp:connect({local, PathBin}, 0, [binary, {active, false}], 100) of
        {ok, Sock} ->
            _ = gen_tcp:close(Sock),
            false;
        {error, econnrefused} ->
            true;
        {error, _} ->
            false
    end.

close_socket(Socket) ->
    _ = gen_tcp:close(Socket),
    nil.

socket_port(Socket) ->
    case inet:port(Socket) of
        {ok, Port} ->
            {ok, Port};
        {error, Reason} ->
            {error, {posix, Reason}}
    end.

send(Socket, Data) ->
    case gen_tcp:send(Socket, Data) of
        ok ->
            {ok, nil};
        {error, _} ->
            {error, nil}
    end.

controlling_process(Socket, Pid) ->
    map_posix(gen_tcp:controlling_process(Socket, Pid)).

recv(Socket, Size, TimeoutMs) ->
    map_posix(gen_tcp:recv(Socket, Size, TimeoutMs)).

delete_path(PathBin) ->
    _ = file:delete(PathBin),
    nil.

close_file(Handle) ->
    _ = file:close(Handle),
    nil.

map_posix(ok) ->
    {ok, nil};
map_posix({ok, _} = Reply) ->
    Reply;
map_posix({error, Reason}) ->
    {error, {posix, Reason}}.

translate_error(Path, enoent) ->
    {file_not_found, Path};
translate_error(Path, eacces) ->
    {file_access_denied, Path};
translate_error(Path, eperm) ->
    {file_access_denied, Path};
translate_error(Path, eisdir) ->
    {file_is_directory, Path};
translate_error(Path, Reason) ->
    {file_other, Path, list_to_binary(io_lib:format("~p", [Reason]))}.
