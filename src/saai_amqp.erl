-module(saai_amqp).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0, child_spec/0, publish_dispatch/2, publish_result/2, subscribe_results/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {channel}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{id => ?MODULE,
      start => {?MODULE, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

publish_dispatch(RoutingKey, Payload) ->
    gen_server:call(?MODULE, {publish, RoutingKey, Payload}).

publish_result(RoutingKey, Payload) ->
    gen_server:call(?MODULE, {publish, RoutingKey, Payload}).

subscribe_results(HandlerPid) ->
    gen_server:cast(?MODULE, {subscribe_results, HandlerPid}).

init([]) ->
    Amqp = saai_config:get(amqp, #{}),
    Uri = maps:get(uri, Amqp, "amqp://guest:guest@localhost:5672"),
    {ok, Params} = amqp_uri:parse(Uri),
    {ok, Connection} = amqp_connection:start(Params),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ok = setup_topology(Channel, Amqp),
    {ok, #state{channel = Channel}}.

handle_call({publish, RoutingKey, Payload}, _From, State = #state{channel = Channel}) ->
    AmqpCfg = saai_config:get(amqp, #{}),
    ExchangeName = maps:get(dispatch_exchange, AmqpCfg, "sys.core"),
    amqp_channel:cast(Channel, #'basic.publish'{exchange = ExchangeName, routing_key = RoutingKey}, #amqp_msg{payload = jsx:encode(Payload)}),
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({subscribe_results, HandlerPid}, State = #state{channel = Channel}) ->
    ResultQueue = maps:get(result_queue, saai_config:get(amqp, #{}), "task_result_q"),
    {ok, _ConsumerTag} = amqp_basic:consume(Channel, ResultQueue, HandlerPid, #{no_ack => true}),
    {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, #state{channel = Channel}) ->
    catch amqp_channel:close(Channel),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

setup_topology(Channel, AmqpCfg) ->
    Exchange = maps:get(dispatch_exchange, AmqpCfg, "sys.core"),
    ok = amqp_channel:call(Channel, #'exchange.declare'{exchange = Exchange, type = <<"topic">>, durable = true}),
    DispatchRouting = maps:get(dispatch_routing_key, AmqpCfg, "sys.core.task.dispatch"),
    ResultQueue = maps:get(result_queue, AmqpCfg, "task_result_q"),
    ok = amqp_channel:call(Channel, #'queue.declare'{queue = ResultQueue, durable = true}),
    ok = amqp_channel:call(Channel, #'queue.bind'{queue = ResultQueue, exchange = Exchange, routing_key = maps:get(result_routing_key, AmqpCfg, "sys.core.task.result")}),
    DispatchQueue = maps:get(dispatch_queue, AmqpCfg, "task_dispatch_q"),
    ok = amqp_channel:call(Channel, #'queue.declare'{queue = DispatchQueue, durable = true}),
    ok = amqp_channel:call(Channel, #'queue.bind'{queue = DispatchQueue, exchange = Exchange, routing_key = DispatchRouting}),
    ok.
