-module(fcgi_test_ffi).

-export([connect/1]).

-define(CONNECT_OPTS, [binary, {active, false}, {packet, raw}]).

connect(PathBin) ->
    case gen_tcp:connect({local, PathBin}, 0, ?CONNECT_OPTS, 1000) of
        {ok, _} = Reply -> Reply;
        {error, R} -> {error, {posix, R}}
    end.
