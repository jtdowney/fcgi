-module(fcgi_ffi).

-include_lib("kernel/include/file.hrl").

-export([validate_file/1, open/1, pread/3, close/1]).

validate_file(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = directory}} ->
            {error, is_directory};
        {ok, _} ->
            {ok, nil};
        {error, Reason} ->
            {error, translate_error(Reason)}
    end.

open(Path) ->
    case file:open(Path, [read, raw, binary]) of
        {ok, IoDevice} ->
            {ok, IoDevice};
        {error, Reason} ->
            {error, translate_error(Reason)}
    end.

pread(IoDevice, Position, Size) ->
    case file:pread(IoDevice, Position, Size) of
        {ok, Data} when is_binary(Data) ->
            {ok, Data};
        eof ->
            {ok, <<>>};
        {error, Reason} ->
            {error, translate_error(Reason)}
    end.

close(IoDevice) ->
    file:close(IoDevice),
    nil.

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
