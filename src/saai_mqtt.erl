-module(saai_mqtt).
-behaviour(gen_server).

-export([start_link/0, child_spec/0, publish/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {client, topic}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{id => ?MODULE,
      start => {?MODULE, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

publish(Message) ->
    gen_server:cast(?MODULE, {publish, Message}).

init([]) ->
    Mqtt = saai_config:get(mqtt, #{}),
    Host = maps:get(host, Mqtt, "localhost"),
    Port = maps:get(port, Mqtt, 1883),
    Topic = maps:get(topic, Mqtt, "sys.ui.updates"),
    {ok, Client} = emqtt:start_link([{host, Host}, {port, Port}, {clientid, maps:get(clientid, Mqtt, "saai-core")}, {clean_start, true}]),
    {ok, _} = emqtt:connect(Client),
    {ok, #state{client = Client, topic = Topic}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_cast({publish, Message}, State = #state{client = Client, topic = Topic}) ->
    Payload = jsx:encode(Message),
    _ = emqtt:publish(Client, Topic, Payload, 0, false),
    {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, #state{client = Client}) ->
    catch emqtt:disconnect(Client),
    ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
