-module(saai_db).

-export([
    create_project/2,
    create_run/3,
    active_runs/0,
    insert_task/1,
    insert_edges/1,
    claim_ready_tasks/2,
    acquire_file_locks/4,
    release_file_locks/2,
    insert_attempt/1,
    finish_attempt/2,
    next_attempt_no/1,
    append_event/3,
    store_artifact/1,
    store_capsule/1,
    insert_fingerprint/1,
    update_run_status/2,
    get_run/1,
    list_tasks/1,
    list_edges/1,
    list_artifacts/1,
    list_events/1,
    task_run/1
]).

-define(JSONB(Data), {json, jsx:encode(Data)}).

create_project(Name, GlobalSettings) ->
    Sql = "INSERT INTO projects (id, name, global_settings) VALUES (gen_random_uuid(), $1, $2::jsonb) RETURNING id",
    {ok, _, [{Id}]} = saai_repo:query(Sql, [Name, ?JSONB(GlobalSettings)]),
    Id.

create_run(ProjectId, GoalText, Settings) ->
    Sql = "INSERT INTO runs (id, project_id, status, goal_text, settings, cost_limit, started_at) "
          "VALUES (gen_random_uuid(), $1, 'NEW', $2, $3::jsonb, $4, now()) RETURNING id",
    CostLimit = maps:get(cost_limit, Settings, null),
    {ok, _, [{Id}]} = saai_repo:query(Sql, [ProjectId, GoalText, ?JSONB(Settings), CostLimit]),
    Id.

active_runs() ->
    Sql = "SELECT id FROM runs WHERE status IN ('NEW','RUNNING')",
    case saai_repo:query(Sql, []) of
        {ok, _, Rows} -> [Id || {Id} <- Rows];
        _ -> []
    end.

insert_task(TaskMap) ->
    Sql = "INSERT INTO tasks (id, run_id, parent_id, task_type, title, objective, definition_of_done, status, priority, depth_level, "
          "depends_remaining, assigned_worker_id, retry_count, max_retries, token_budget, time_budget_seconds, idempotency_key, claim_token, "
          "locked_files, context_refs) VALUES (coalesce($1, gen_random_uuid()), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::jsonb)" 
          "ON CONFLICT (run_id, idempotency_key) DO UPDATE SET title = EXCLUDED.title RETURNING id",
    Params = [
        maps:get(id, TaskMap, null),
        maps:get(run_id, TaskMap),
        maps:get(parent_id, TaskMap, null),
        maps:get(task_type, TaskMap),
        maps:get(title, TaskMap),
        maps:get(objective, TaskMap),
        maps:get(definition_of_done, TaskMap),
        maps:get(status, TaskMap),
        maps:get(priority, TaskMap, 0),
        maps:get(depth_level, TaskMap, 0),
        maps:get(depends_remaining, TaskMap, 0),
        maps:get(assigned_worker_id, TaskMap, null),
        maps:get(retry_count, TaskMap, 0),
        maps:get(max_retries, TaskMap, 2),
        maps:get(token_budget, TaskMap, 2000),
        maps:get(time_budget_seconds, TaskMap, 600),
        maps:get(idempotency_key, TaskMap),
        maps:get(claim_token, TaskMap, null),
        maps:get(locked_files, TaskMap, []),
        ?JSONB(maps:get(context_refs, TaskMap, #{}))
    ],
    {ok, _, [{Id}]} = saai_repo:query(Sql, Params),
    Id.

insert_edges(Edges) when is_list(Edges) ->
    Sql = "INSERT INTO task_edges (run_id, from_task_id, to_task_id, edge_type) VALUES ($1, $2, $3, $4) "
          "ON CONFLICT DO NOTHING",
    [saai_repo:query(Sql, [RunId, From, To, maps:get(edge_type, Edge, "blocks")]) ||
        Edge = #{run_id := RunId, from_task_id := From, to_task_id := To} <- Edges],
    ok.

claim_ready_tasks(RunId, Limit) ->
    Sql = "WITH cte AS ("
          "  SELECT id FROM tasks WHERE run_id=$1 AND status='READY' AND lease_expires_at IS NULL "
          "  ORDER BY priority DESC, created_at ASC LIMIT $2 FOR UPDATE SKIP LOCKED"
          ") UPDATE tasks t SET status='RUNNING', claim_token=gen_random_uuid(), claimed_at=now(), "
          "lease_expires_at = now() + interval '5 minutes' "
          "FROM cte WHERE t.id = cte.id RETURNING t.id, t.claim_token, t.locked_files, t.assigned_worker_id, "
          "t.task_type, t.objective, t.definition_of_done, t.token_budget, t.time_budget_seconds, t.idempotency_key",
    case saai_repo:query(Sql, [RunId, Limit]) of
        {ok, _, Rows} -> Rows;
        Error -> erlang:error({claim_ready_tasks_failed, Error})
    end.

acquire_file_locks(RunId, TaskId, ClaimToken, Files) when is_list(Files) ->
    saai_repo:transaction(fun(Conn) ->
        lists:foreach(
          fun(FilePath) ->
              LockSql = "INSERT INTO file_locks (run_id, file_path, task_id, claim_token, lease_expires_at) "
                        "VALUES ($1, $2, $3, $4, now() + interval '5 minutes')",
              case epgsql:equery(Conn, LockSql, [RunId, FilePath, TaskId, ClaimToken]) of
                  {ok, _Columns, _Count} -> ok;
                  {error, {error, {constraint_violation, _}}} -> throw({lock_conflict, FilePath});
                  {error, Reason} -> throw(Reason)
              end
          end, Files),
        ok
    end).

release_file_locks(TaskId, ClaimToken) ->
    Sql = "DELETE FROM file_locks WHERE task_id=$1 AND claim_token=$2",
    saai_repo:query(Sql, [TaskId, ClaimToken]),
    ok.

insert_attempt(AttemptMap) ->
    Sql = "INSERT INTO task_attempts (id, task_id, run_id, attempt_no, status, worker_id, provider, model, correlation_id, prompt_hash, "
          "result_fingerprint, error_signature, usage_stats) VALUES (coalesce($1, gen_random_uuid()), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb) "
          "RETURNING id",
    Params = [
        maps:get(id, AttemptMap, null),
        maps:get(task_id, AttemptMap),
        maps:get(run_id, AttemptMap),
        maps:get(attempt_no, AttemptMap),
        maps:get(status, AttemptMap),
        maps:get(worker_id, AttemptMap, null),
        maps:get(provider, AttemptMap, null),
        maps:get(model, AttemptMap, null),
        maps:get(correlation_id, AttemptMap),
        maps:get(prompt_hash, AttemptMap, null),
        maps:get(result_fingerprint, AttemptMap, null),
        maps:get(error_signature, AttemptMap, null),
        ?JSONB(maps:get(usage_stats, AttemptMap, #{}))
    ],
    {ok, _, [{Id}]} = saai_repo:query(Sql, Params),
    Id.

finish_attempt(AttemptId, Status) ->
    Sql = "UPDATE task_attempts SET status=$1, finished_at=now() WHERE id=$2",
    saai_repo:query(Sql, [Status, AttemptId]),
    ok.

next_attempt_no(TaskId) ->
    Sql = "SELECT coalesce(max(attempt_no),0)+1 FROM task_attempts WHERE task_id=$1",
    {ok, _, [{Next}]} = saai_repo:query(Sql, [TaskId]),
    Next.

append_event(RunId, TaskId, Event) ->
    Sql = "INSERT INTO task_events (id, run_id, task_id, type, payload) VALUES (gen_random_uuid(), $1, $2, $3, $4::jsonb)",
    saai_repo:query(Sql, [RunId, TaskId, maps:get(type, Event), ?JSONB(maps:get(payload, Event, #{}))]),
    ok.

store_artifact(Artifact) ->
    Sql = "INSERT INTO artifacts (id, run_id, task_id, artifact_type, file_path, content_text, diff_content, sha256) "
          "VALUES (coalesce($1, gen_random_uuid()), $2, $3, $4, $5, $6, $7, $8) RETURNING id",
    Params = [
        maps:get(id, Artifact, null),
        maps:get(run_id, Artifact),
        maps:get(task_id, Artifact),
        maps:get(artifact_type, Artifact),
        maps:get(file_path, Artifact, null),
        maps:get(content_text, Artifact, null),
        maps:get(diff_content, Artifact, null),
        maps:get(sha256, Artifact)
    ],
    {ok, _, [{Id}]} = saai_repo:query(Sql, Params),
    Id.

store_capsule(Capsule) ->
    Sql = "INSERT INTO capsules (task_id, run_id, summary_text, decision_logic, assumptions, verification) "
          "VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb) "
          "ON CONFLICT (task_id) DO UPDATE SET summary_text=EXCLUDED.summary_text RETURNING task_id",
    Params = [
        maps:get(task_id, Capsule),
        maps:get(run_id, Capsule),
        maps:get(summary_text, Capsule),
        ?JSONB(maps:get(decision_logic, Capsule, #{})),
        ?JSONB(maps:get(assumptions, Capsule, #{})),
        ?JSONB(maps:get(verification, Capsule, #{}))
    ],
    {ok, _, [{TaskId}]} = saai_repo:query(Sql, Params),
    TaskId.

insert_fingerprint(Fingerprint) ->
    Sql = "INSERT INTO fingerprints (id, run_id, task_id, fingerprint_hash, fingerprint_payload) VALUES "
          "(coalesce($1, gen_random_uuid()), $2, $3, $4, $5::jsonb)",
    Params = [
        maps:get(id, Fingerprint, null),
        maps:get(run_id, Fingerprint),
        maps:get(task_id, Fingerprint),
        maps:get(fingerprint_hash, Fingerprint),
        ?JSONB(maps:get(fingerprint_payload, Fingerprint, #{}))
    ],
    saai_repo:query(Sql, Params).

update_run_status(RunId, Status) ->
    Sql = "UPDATE runs SET status=$1 WHERE id=$2",
    saai_repo:query(Sql, [Status, RunId]),
    ok.

get_run(RunId) ->
    Sql = "SELECT id, project_id, status, goal_text, settings, current_cost, current_tokens, started_at, finished_at FROM runs WHERE id=$1",
    case saai_repo:query(Sql, [RunId]) of
        {ok, _, [Row]} -> Row;
        _ -> not_found
    end.

list_tasks(RunId) ->
    Sql = "SELECT id, parent_id, task_type, title, status, priority, depth_level, depends_remaining FROM tasks WHERE run_id=$1",
    case saai_repo:query(Sql, [RunId]) of
        {ok, _, Rows} -> Rows;
        _ -> []
    end.

list_edges(RunId) ->
    Sql = "SELECT from_task_id, to_task_id, edge_type FROM task_edges WHERE run_id=$1",
    case saai_repo:query(Sql, [RunId]) of
        {ok, _, Rows} -> Rows;
        _ -> []
    end.

list_artifacts(TaskId) ->
    Sql = "SELECT id, artifact_type, file_path, sha256, created_at FROM artifacts WHERE task_id=$1",
    case saai_repo:query(Sql, [TaskId]) of
        {ok, _, Rows} -> Rows;
        _ -> []
    end.

list_events(TaskId) ->
    Sql = "SELECT id, type, payload, ts FROM task_events WHERE task_id=$1 ORDER BY ts",
    case saai_repo:query(Sql, [TaskId]) of
        {ok, _, Rows} -> Rows;
        _ -> []
    end.

task_run(TaskId) ->
    Sql = "SELECT run_id FROM tasks WHERE id=$1",
    case saai_repo:query(Sql, [TaskId]) of
        {ok, _, [{RunId}]} -> RunId;
        _ -> undefined
    end.
