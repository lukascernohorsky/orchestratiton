-module(saai_config).
-behaviour(gen_server).

-export([start_link/0, child_spec/0, get/1, get/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {config = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{id => ?MODULE,
      start => {?MODULE, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

get(Key, Default) ->
    case get(Key) of
        undefined -> Default;
        Value -> Value
    end.

init([]) ->
    {ok, Config} = application:get_env(saai),
    {ok, #state{config = maps:from_list(Config)}}.

handle_call({get, Key}, _From, State = #state{config = Config}) ->
    {reply, maps:get(Key, Config, undefined), State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
