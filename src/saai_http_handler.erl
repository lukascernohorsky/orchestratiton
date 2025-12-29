-module(saai_http_handler).
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, State = #{action := Action}) ->
    {Reply, Req1} = handle(Action, Req0),
    {ok, ReplyReq} = respond(Reply, Req1),
    {ok, ReplyReq, State}.

handle(create_project, Req) ->
    {Body, Req1} = decode_body(Req),
    Name = maps:get(<<"name">>, Body, <<>>),
    Settings = maps:get(<<"global_settings">>, Body, #{}),
    ProjectId = saai_db:create_project(binary_to_list(Name), Settings),
    {#{status => 201, body => #{id => ProjectId}}, Req1};
handle(create_run, Req) ->
    {Body, Req1} = decode_body(Req),
    ProjectId = cowboy_req:binding(id, Req),
    Goal = maps:get(<<"goal_text">>, Body, <<>>),
    Settings = maps:get(<<"settings">>, Body, #{}),
    RunId = saai_db:create_run(ProjectId, binary_to_list(Goal), Settings),
    {#{status => 201, body => #{id => RunId}}, Req1};
handle(get_run, Req) ->
    RunId = cowboy_req:binding(run_id, Req),
    case saai_db:get_run(RunId) of
        not_found -> {#{status => 404, body => #{error => <<"not_found">>}}, Req};
        Run -> {#{status => 200, body => format_run(Run)}, Req}
    end;
handle(get_graph, Req) ->
    RunId = cowboy_req:binding(run_id, Req),
    {#{status => 200, body => #{tasks => saai_db:list_tasks(RunId), edges => saai_db:list_edges(RunId)}}, Req};
handle(pause_run, Req) -> update_run_status(Req, "PAUSED");
handle(resume_run, Req) -> update_run_status(Req, "RUNNING");
handle(abort_run, Req) -> update_run_status(Req, "ABORTED");
handle(retry_task, Req) ->
    TaskId = cowboy_req:binding(task_id, Req),
    RunId = saai_db:task_run(TaskId),
    saai_db:append_event(RunId, TaskId, #{type => "TASK_STATUS_CHANGED", payload => #{status => "READY"}}),
    {#{status => 202, body => #{task_id => TaskId, status => <<"queued">>}}, Req};
handle(list_artifacts, Req) ->
    TaskId = cowboy_req:binding(task_id, Req),
    {#{status => 200, body => #{artifacts => saai_db:list_artifacts(TaskId)}}, Req};
handle(list_events, Req) ->
    TaskId = cowboy_req:binding(task_id, Req),
    {#{status => 200, body => #{events => saai_db:list_events(TaskId)}}, Req};
handle(_, Req) -> {#{status => 404, body => #{error => <<"not_found">>}}, Req}.

update_run_status(Req, Status) ->
    RunId = cowboy_req:binding(run_id, Req),
    saai_db:update_run_status(RunId, Status),
    {#{status => 202, body => #{run_id => RunId, status => Status}}, Req}.

respond(#{status := Status, body := Body}, Req) ->
    Json = jsx:encode(Body),
    Req1 = cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>}, Json, Req),
    {ok, Req1}.

decode_body(Req) ->
    case cowboy_req:has_body(Req) of
        true ->
            {ok, Body, Req1} = cowboy_req:read_body(Req),
            {jsx:decode(Body, [return_maps]), Req1};
        false -> {#{}, Req}
    end.

format_run({Id, ProjectId, Status, Goal, Settings, Cost, Tokens, StartedAt, FinishedAt}) ->
    #{id => Id, project_id => ProjectId, status => Status, goal_text => Goal, settings => Settings, current_cost => Cost,
      current_tokens => Tokens, started_at => StartedAt, finished_at => FinishedAt}.
