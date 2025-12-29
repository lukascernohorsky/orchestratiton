-module(saai_repo_worker).
-behaviour(gen_server).

-export([start_link/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start_link(Host, Port, User, Password, Database) ->
    gen_server:start_link(?MODULE, [Host, Port, User, Password, Database], []).

init([Host, Port, User, Password, Database]) ->
    {ok, Conn} = epgsql:connect(Host, User, Password, [{port, Port}, {database, Database}]),
    {ok, #state{conn = Conn}}.

handle_call({query, Sql, Params}, _From, State = #state{conn = Conn}) ->
    {reply, epgsql:equery(Conn, Sql, Params), State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, #state{conn = Conn}) ->
    catch epgsql:close(Conn),
    ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
