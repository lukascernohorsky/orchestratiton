-module(saai_scheduler).
-behaviour(gen_server).

-export([start_link/0, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {timer}).

-define(POLL_MSG, poll).

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
    Timer = schedule(),
    {ok, #state{timer = Timer}}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(?POLL_MSG, State) ->
    dispatch_ready_tasks(),
    {noreply, State#state{timer = schedule()}};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule() ->
    Interval = maps:get(poll_interval_ms, saai_config:get(scheduler, #{}), 2000),
    erlang:send_after(Interval, self(), ?POLL_MSG).

dispatch_ready_tasks() ->
    Runs = saai_db:active_runs(),
    lists:foreach(fun dispatch_run/1, Runs).

dispatch_run(RunId) ->
    case saai_db:claim_ready_tasks(RunId, 10) of
        [] -> ok;
        Rows -> lists:foreach(fun(Row) -> dispatch_task(RunId, Row) end, Rows)
    end.

dispatch_task(RunId, {TaskId, ClaimToken, LockedFiles, WorkerId, TaskType, Objective, DoD, TokenBudget, TimeBudget, IdempotencyKey}) ->
    case catch saai_db:acquire_file_locks(RunId, TaskId, ClaimToken, LockedFiles) of
        ok ->
            AttemptNo = saai_db:next_attempt_no(TaskId),
            CorrelationId = saai_util:gen_uuid(),
            AttemptId = saai_db:insert_attempt(#{
                task_id => TaskId,
                run_id => RunId,
                attempt_no => AttemptNo,
                status => "STARTED",
                worker_id => WorkerId,
                provider => null,
                model => null,
                correlation_id => CorrelationId,
                usage_stats => #{}
            }),
            Payload = #{
                project_id => null,
                run_id => RunId,
                task_id => TaskId,
                attempt_id => AttemptId,
                correlation_id => CorrelationId,
                idempotency_key => IdempotencyKey,
                claim_token => ClaimToken,
                deadline_ts => saai_time:deadline(TimeBudget),
                budget => #{token_budget => TokenBudget, time_budget_seconds => TimeBudget, cost_limit_remaining => null},
                task_type => TaskType,
                instruction => Objective,
                context => #{capsule => null},
                model => null,
                files_hint => LockedFiles,
                output_contract => DoD
            },
            saai_amqp:publish_dispatch(maps:get(dispatch_routing_key, saai_config:get(amqp, #{}), "sys.core.task.dispatch"), Payload),
            saai_db:append_event(RunId, TaskId, #{type => "TASK_DISPATCHED", payload => Payload}),
            saai_mqtt:publish(#{run_id => RunId, task_id => TaskId, status => "RUNNING"});
        {lock_conflict, File} ->
            saai_db:update_task_status(TaskId, "BLOCKED"),
            saai_db:append_event(RunId, TaskId, #{type => "TASK_STATUS_CHANGED", payload => #{status => "BLOCKED", file => File}});
        Error ->
            saai_db:update_task_status(TaskId, "FAILED"),
            saai_db:append_event(RunId, TaskId, #{type => "TASK_STATUS_CHANGED", payload => #{status => "FAILED", error => Error}})
    end.
