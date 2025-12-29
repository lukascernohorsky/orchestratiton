-module(saai_http).
-behaviour(gen_server).

-export([start_link/0, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {listener}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{id => ?MODULE,
      start => {?MODULE, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

init([]) ->
    Port = maps:get(port, saai_config:get(http, #{}), 8080),
    Dispatch = cowboy_router:compile([
        {'_', [
            {<<"/projects">>, saai_http_handler, #{action => create_project}},
            {<<"/projects/:id/runs">>, saai_http_handler, #{action => create_run}},
            {<<"/runs/:run_id">>, saai_http_handler, #{action => get_run}},
            {<<"/runs/:run_id/graph">>, saai_http_handler, #{action => get_graph}},
            {<<"/runs/:run_id/pause">>, saai_http_handler, #{action => pause_run}},
            {<<"/runs/:run_id/resume">>, saai_http_handler, #{action => resume_run}},
            {<<"/runs/:run_id/abort">>, saai_http_handler, #{action => abort_run}},
            {<<"/tasks/:task_id/retry">>, saai_http_handler, #{action => retry_task}},
            {<<"/tasks/:task_id/artifacts">>, saai_http_handler, #{action => list_artifacts}},
            {<<"/tasks/:task_id/events">>, saai_http_handler, #{action => list_events}}
        ]}
    ]),
    {ok, Listener} = cowboy:start_clear(http_listener, [{port, Port}], #{env => #{dispatch => Dispatch}}),
    {ok, #state{listener = Listener}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
