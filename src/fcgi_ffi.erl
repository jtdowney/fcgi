-module(fcgi_ffi).

-include_lib("kernel/include/file.hrl").

-export([
    open_and_size/1,
    sendfile/4,
    listen/1,
    socket_close/1,
    accept/1,
    send/2,
    controlling_process/2,
    recv/3,
    delete_path/1,
    close_file/1
]).

-define(LISTEN_OPTS, [binary, {active, false}, {packet, raw}]).

open_and_size(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = directory}} ->
            {error, is_directory};
        {ok, #file_info{access = none}} ->
            {error, access_denied};
        {ok, #file_info{access = write}} ->
            {error, access_denied};
        {ok, _} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, IoDevice} ->
                    size_and_rewind(IoDevice);
                {error, Reason} ->
                    {error, translate_error(Reason)}
            end;
        {error, Reason} ->
            {error, translate_error(Reason)}
    end.

size_and_rewind(IoDevice) ->
    case file:position(IoDevice, eof) of
        {ok, Size} ->
            rewind_with_size(IoDevice, Size);
        {error, Reason} ->
            close_and_translate(IoDevice, Reason)
    end.

rewind_with_size(IoDevice, Size) ->
    case file:position(IoDevice, 0) of
        {ok, 0} ->
            {ok, {IoDevice, Size}};
        {error, Reason} ->
            close_and_translate(IoDevice, Reason)
    end.

close_and_translate(IoDevice, Reason) ->
    _ = file:close(IoDevice),
    {error, translate_error(Reason)}.

sendfile(IoDevice, Socket, Offset, Bytes) ->
    case file:sendfile(IoDevice, Socket, Offset, Bytes, []) of
        {ok, Sent} ->
            {ok, Sent};
        {error, Reason} ->
            {error, translate_error(Reason)}
    end.

listen(PathBin) ->
    case file:read_file_info(PathBin) of
        {ok, _} ->
            {error, {path_exists, PathBin}};
        {error, enoent} ->
            wrap_posix(gen_tcp:listen(0, [{ifaddr, {local, PathBin}} | ?LISTEN_OPTS]));
        {error, R} ->
            {error, {posix, R}}
    end.

socket_close(Socket) ->
    _ = gen_tcp:close(Socket),
    nil.

accept(Listen) ->
    wrap_posix(gen_tcp:accept(Listen)).

send(Socket, Data) ->
    wrap_posix(gen_tcp:send(Socket, Data)).

controlling_process(Socket, Pid) ->
    wrap_posix(gen_tcp:controlling_process(Socket, Pid)).

recv(Socket, Size, TimeoutMs) ->
    wrap_posix(gen_tcp:recv(Socket, Size, TimeoutMs)).

delete_path(PathBin) ->
    _ = file:delete(PathBin),
    nil.

close_file(Handle) ->
    _ = file:close(Handle),
    nil.

wrap_posix(ok) ->
    {ok, nil};
wrap_posix({ok, _} = Reply) ->
    Reply;
wrap_posix({error, R}) ->
    {error, {posix, R}}.

translate_error(enoent) ->
    not_found;
translate_error(eacces) ->
    access_denied;
translate_error(eperm) ->
    access_denied;
translate_error(eisdir) ->
    is_directory;
translate_error(Reason) ->
    {unknown, list_to_binary(io_lib:format("~p", [Reason]))}.
