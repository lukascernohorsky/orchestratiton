-module(saai_result_handler).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

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
    saai_amqp:subscribe_results(self()),
    {ok, #state{}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.

handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}}, State) ->
    catch handle_payload(Payload),
    saai_amqp:ack(Tag),
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

handle_payload(Payload) when is_binary(Payload) ->
    case catch jsx:decode(Payload, [return_maps]) of
        {'EXIT', _} -> ok;
        Map when is_map(Map) -> handle_result(Map)
    end.

handle_result(Result = #{}) ->
    RunId = maps:get(<<"run_id">>, Result, undefined),
    TaskId = maps:get(<<"task_id">>, Result, undefined),
    ClaimToken = maps:get(<<"claim_token">>, Result, undefined),
    Status = maps:get(<<"status">>, Result, <<"ERROR">>),
    saai_db:append_event(RunId, TaskId, #{type => "TASK_RESULT_RECEIVED", payload => Result}),
    case Status of
        <<"OK">> ->
            store_artifacts(RunId, TaskId, Result),
            _ = saai_db:mark_task_done(RunId, TaskId),
            saai_db:release_file_locks(TaskId, ClaimToken),
            saai_mqtt:publish(#{run_id => RunId, task_id => TaskId, status => "DONE"}),
            saai_db:append_event(RunId, TaskId, #{type => "TASK_STATUS_CHANGED", payload => #{status => "DONE"}});
        _ ->
            saai_db:update_task_status(TaskId, "FAILED"),
            saai_db:release_file_locks(TaskId, ClaimToken),
            saai_db:append_event(RunId, TaskId, #{type => "TASK_STATUS_CHANGED", payload => #{status => "FAILED"}}),
            saai_mqtt:publish(#{run_id => RunId, task_id => TaskId, status => "FAILED"})
    end.

store_artifacts(RunId, TaskId, Result) ->
    Artifacts = maps:get(<<"artifacts">>, Result, []),
    lists:foreach(fun(Artifact) -> store_artifact(RunId, TaskId, Artifact) end, Artifacts).

store_artifact(RunId, TaskId, Artifact) when is_map(Artifact) ->
    Type = maps:get(<<"artifact_type">>, Artifact, <<"LOG">>),
    FilePath = maps:get(<<"file_path">>, Artifact, null),
    ContentText = maps:get(<<"content_text">>, Artifact, null),
    DiffContent = maps:get(<<"diff_content">>, Artifact, null),
    Sha = case maps:get(<<"sha256">>, Artifact, undefined) of
        undefined -> saai_util:sha256(ensure_binary(ContentText, <<>>));
        V -> ensure_binary(V, <<>>)
    end,
    _ = saai_db:store_artifact(#{
        run_id => RunId,
        task_id => TaskId,
        artifact_type => to_list(Type),
        file_path => to_optional(FilePath),
        content_text => to_optional(ContentText),
        diff_content => to_optional(DiffContent),
        sha256 => Sha
    }),
    ok.

ensure_binary(null, Default) -> Default;
ensure_binary(undefined, Default) -> Default;
ensure_binary(Value, _Default) when is_binary(Value) -> Value;
ensure_binary(Value, _Default) when is_list(Value) -> list_to_binary(Value);
ensure_binary(Value, _Default) -> iolist_to_binary(io_lib:format("~p", [Value])).

to_list(Value) when is_binary(Value) -> binary_to_list(Value);
to_list(Value) when is_list(Value) -> Value;
to_list(Value) -> io_lib:format("~p", [Value]).

to_optional(null) -> null;
to_optional(undefined) -> null;
to_optional(Value) when is_binary(Value) -> binary_to_list(Value);
to_optional(Value) when is_list(Value) -> Value;
to_optional(Value) -> io_lib:format("~p", [Value]).
