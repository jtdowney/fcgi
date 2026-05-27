-module(fcgi_test_ffi).

-export([connect_unix/1, connect_tcp/2, socket_port/1]).

-define(CONNECT_OPTS, [binary, {active, false}, {packet, raw}]).

connect_tcp(Host, Port) ->
    case inet:parse_address(binary_to_list(Host)) of
        {ok, Ip} ->
            case gen_tcp:connect(Ip, Port, ?CONNECT_OPTS, 1000) of
                {ok, _} = Reply ->
                    Reply;
                {error, Reason} ->
                    {error, {posix, Reason}}
            end;
        {error, _} ->
            {error, {posix, einval}}
    end.

connect_unix(PathBin) ->
    case gen_tcp:connect({local, PathBin}, 0, ?CONNECT_OPTS, 1000) of
        {ok, _} = Reply ->
            Reply;
        {error, Reason} ->
            {error, {posix, Reason}}
    end.

socket_port(Socket) ->
    case inet:port(Socket) of
        {ok, Port} ->
            {ok, Port};
        {error, Reason} ->
            {error, {posix, Reason}}
    end.
