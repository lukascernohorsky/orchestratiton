-module(saai_repo).
-behaviour(gen_server).

-export([start_link/0, child_spec/0, transaction/1, query/2, query/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(POOL, saai_db_pool).
-record(state, {db_config = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{id => ?MODULE,
      start => {?MODULE, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

transaction(Fun) when is_function(Fun, 1) ->
    poolboy:transaction(?POOL, fun(Conn) ->
        epgsql:squery(Conn, "BEGIN"),
        try
            Result = Fun(Conn),
            epgsql:squery(Conn, "COMMIT"),
            Result
        catch
            Class:Reason:Stack ->
                epgsql:squery(Conn, "ROLLBACK"),
                erlang:raise(Class, Reason, Stack)
        end
    end).

query(Sql, Params) -> query(Sql, Params, []).

query(Sql, Params, Options) ->
    poolboy:transaction(?POOL, fun(Conn) -> epgsql:equery(Conn, Sql, Params, Options) end).

init([]) ->
    DB = saai_config:get(db, #{}),
    Host = maps:get(host, DB, "localhost"),
    Port = maps:get(port, DB, 5432),
    User = maps:get(user, DB, "saai"),
    Password = maps:get(password, DB, "saai"),
    Database = maps:get(database, DB, "saai"),
    PoolConfig = [
        {name, {local, ?POOL}},
        {worker_module, saai_repo_worker},
        {size, 5},
        {max_overflow, 2},
        {worker_args, [Host, Port, User, Password, Database]}
    ],
    {ok, _} = poolboy:start_link(PoolConfig, []),
    {ok, #state{db_config = DB}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
