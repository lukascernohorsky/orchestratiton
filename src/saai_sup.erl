-module(saai_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    RestartStrategy = {one_for_one, 5, 10},
    Children = [
        saai_config:child_spec(),
        saai_repo:child_spec(),
        saai_amqp:child_spec(),
        saai_mqtt:child_spec(),
        saai_scheduler:child_spec(),
        saai_http:child_spec()
    ],
    {ok, {RestartStrategy, Children}}.
