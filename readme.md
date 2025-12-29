# PROJECT SPECIFICATION (Codex‑Ready): AI Development Orchestrator (S.A.A.I.)

> Purpose: This document is designed to be pasted directly into Codex as an implementation blueprint.
> Priority: **Speed-first**, without sacrificing correctness under concurrency, retries, duplicate messages, and worker crashes.

---

## 0. Goal and Non‑Goals

### Goal
Build a high-parallel, distributed system for autonomous software development. The system (“Orchestrator”) decomposes a large goal into a **DAG** (Directed Acyclic Graph) of small tasks that stay under LLM token context limits. It schedules tasks to AI workers, verifies outputs (build/tests/lint), persists state in DB, streams realtime updates to UI, and prevents runaway loops.

### Non‑Goals (MVP)
- No multi-tenant billing or enterprise SSO.
- No full IDE plugin integration.
- No advanced graph layout beyond simple Canvas rendering.
- No full secret manager integration beyond environment variables (MVP).

---

## 1. Core Stack and Constraints

- Backend: **Erlang/OTP** (rebar3), use `gen_statem` for task FSM. **No Elixir or other BEAM languages**; the orchestrator must be implemented purely in Erlang.
- State DB (SSOT): **PostgreSQL** (UUID PKs, JSONB for flexible fields).
- Messaging: **RabbitMQ**
  - **AMQP** for reliable work dispatch (Core <-> Workers)
  - **MQTT** for realtime UI stream (Core -> UI)
- Desktop Client: **Tcl/Tk**
- AI Providers (via adapters): **Mistral / Claude / OpenAI** (Python wrappers for MVP)
- Timezone: **Europe/Prague**

### Hard Requirements
- **Idempotent** scheduling and result ingestion.
- Safe under **at-least-once** message delivery and worker crashes.
- All critical state in DB (not only in memory).

---

## 2. Architecture Overview (Layers)

1) **Orchestrator Core (Erlang node)**
- Plans tasks, maintains DAG, schedules READY tasks, tracks leases/claims, aggregates cost, writes events and artifacts.

2) **State Layer (PostgreSQL)**
- SSOT for projects/runs/tasks/edges/attempts/artifacts/capsules/fingerprints/events/file locks.

3) **Messaging Layer (RabbitMQ)**
- AMQP exchanges/queues for tasks and results, retry + DLQ.
- MQTT topics for UI updates (event projections).

4) **Client Layer**
- Tcl/Tk desktop app subscribes to MQTT, renders DAG, shows logs/diffs, allows pause/abort/retry/inject instructions.

---

## 3. Data Model (PostgreSQL) — REQUIRED TABLES

> Codex MUST generate full DDL with constraints + indexes. Use `uuid` PKs and `jsonb`.

### 3.1 Enumerations
- `run_status`: `NEW, RUNNING, PAUSED, ABORTED, COMPLETED, FAILED`
- `task_status`: `NEW, PLANNED, READY, RUNNING, VERIFYING, DONE, FAILED, BLOCKED, CANCELLED`
- `attempt_status`: `STARTED, SUCCEEDED, FAILED, TIMED_OUT, CANCELLED`
- `event_type`:
  - `RUN_CREATED, RUN_STATUS_CHANGED`
  - `TASK_CREATED, TASK_STATUS_CHANGED, TASK_CLAIMED, TASK_DISPATCHED, TASK_RESULT_RECEIVED`
  - `TASK_VERIFICATION_STARTED, TASK_VERIFICATION_FINISHED`
  - `ARTIFACT_STORED, CAPSULE_STORED`
  - `LOOP_DETECTED, COST_LIMIT_REACHED`
  - `LOCK_ACQUIRED, LOCK_RELEASED`
  - `WORKER_HEARTBEAT`

### 3.2 Core Tables

#### `projects`
- `id UUID PK`
- `name TEXT NOT NULL`
- `global_settings JSONB NOT NULL DEFAULT '{}'`
  - must support: `preferred_models`, `provider_profiles`, `max_budget`, `max_depth`, `max_retries`, `max_concurrency_per_provider`

#### `runs`
- `id UUID PK`
- `project_id UUID FK projects(id)`
- `status run_status NOT NULL`
- `goal_text TEXT NOT NULL`
- `settings JSONB NOT NULL DEFAULT '{}'` (run overrides)
- `current_cost NUMERIC NOT NULL DEFAULT 0`
- `current_tokens BIGINT NOT NULL DEFAULT 0`
- `cost_limit NUMERIC NULL` (if null, use project.max_budget)
- `started_at TIMESTAMPTZ`
- `finished_at TIMESTAMPTZ`

Indexes:
- `(project_id, status)`

#### `tasks`
Fields:
- `id UUID PK`
- `run_id UUID FK runs(id)`
- `parent_id UUID NULL FK tasks(id)`
- `task_type TEXT NOT NULL` (PLAN / DECOMPOSE / EXECUTE / REVIEW / VERIFY / DIAG)
- `title TEXT NOT NULL`
- `objective TEXT NOT NULL`
- `definition_of_done TEXT NOT NULL`
- `status task_status NOT NULL`
- `priority INT NOT NULL DEFAULT 0`
- `depth_level INT NOT NULL DEFAULT 0`
- `depends_remaining INT NOT NULL DEFAULT 0` *(denormalized for fast scheduling)*
- `assigned_worker_id TEXT NULL`
- `retry_count INT NOT NULL DEFAULT 0`
- `max_retries INT NOT NULL DEFAULT 2`
- `token_budget INT NOT NULL DEFAULT 2000`
- `time_budget_seconds INT NOT NULL DEFAULT 600`
- `idempotency_key TEXT NOT NULL` *(unique per run)*
- `claim_token UUID NULL`
- `claimed_at TIMESTAMPTZ NULL`
- `lease_expires_at TIMESTAMPTZ NULL`
- `last_heartbeat_at TIMESTAMPTZ NULL`
- `locked_files TEXT[] NOT NULL DEFAULT '{}'` *(debug/visibility only)*
- `context_refs JSONB NOT NULL DEFAULT '{}'` *(capsule ids, artifact refs, retrieval queries)*
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`

Constraints / Indexes:
- UNIQUE `(run_id, idempotency_key)`
- INDEX `(run_id, status, priority)`
- INDEX `(run_id, depends_remaining, status)`
- INDEX `(run_id, parent_id)`

#### `task_edges`
Dependency edges (**from blocks to**):
- `run_id UUID`
- `from_task_id UUID`
- `to_task_id UUID`
- `edge_type TEXT NOT NULL DEFAULT 'blocks'`
- PK `(run_id, from_task_id, to_task_id)`

Indexes:
- `(run_id, to_task_id)` for decrementing `depends_remaining`

Rule:
- Must remain acyclic (MVP: enforce in application logic).

#### `task_attempts`
- `id UUID PK`
- `task_id UUID FK tasks(id)`
- `run_id UUID FK runs(id)`
- `attempt_no INT NOT NULL`
- `status attempt_status NOT NULL`
- `worker_id TEXT NULL`
- `provider TEXT NULL`
- `model TEXT NULL`
- `correlation_id UUID NOT NULL`
- `prompt_hash TEXT NULL`
- `result_fingerprint TEXT NULL`
- `error_signature TEXT NULL`
- `usage_stats JSONB NOT NULL DEFAULT '{}'` (tokens, cost, latency)
- `started_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- `finished_at TIMESTAMPTZ NULL`

Constraints:
- UNIQUE `(task_id, attempt_no)`

Indexes:
- `(run_id, status)`
- `(task_id, started_at)`

#### `file_locks`
Atomic file-level locks:
- `run_id UUID`
- `file_path TEXT`
- `task_id UUID`
- `claim_token UUID`
- `lease_expires_at TIMESTAMPTZ`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- PK `(run_id, file_path)`

Notes:
- Acquire locks via `INSERT` (unique violation = lock conflict).
- Release locks via `DELETE` for `(task_id, claim_token)`.

#### `artifacts`
- `id UUID PK`
- `run_id UUID FK runs(id)`
- `task_id UUID FK tasks(id)`
- `artifact_type TEXT NOT NULL` (PATCH, DIFF, FILE, LOG, REPORT)
- `file_path TEXT NULL`
- `content_blob BYTEA NULL` *(optional MVP; can use TEXT only)*
- `content_text TEXT NULL`
- `diff_content TEXT NULL`
- `sha256 TEXT NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`

Indexes:
- `(run_id, task_id)`
- `(sha256)`

#### `capsules`
Context capsules (“anti‑forgetting”):
- `task_id UUID PK FK tasks(id)`
- `run_id UUID FK runs(id)`
- `summary_text TEXT NOT NULL`
- `decision_logic JSONB NOT NULL DEFAULT '{}'`
- `assumptions JSONB NOT NULL DEFAULT '{}'`
- `verification JSONB NOT NULL DEFAULT '{}'`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`

#### `fingerprints`
Loop detection:
- `id UUID PK`
- `run_id UUID FK runs(id)`
- `task_id UUID FK tasks(id)`
- `fingerprint_hash TEXT NOT NULL`
- `fingerprint_payload JSONB NOT NULL` (intent, key_files, error_signature, diff_summary)
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`

Constraints / Indexes:
- UNIQUE `(run_id, fingerprint_hash)`
- INDEX `(run_id, created_at)`

#### `task_events`
Append‑only audit log:
- `id UUID PK`
- `run_id UUID FK runs(id)`
- `task_id UUID NULL`
- `ts TIMESTAMPTZ NOT NULL DEFAULT now()`
- `type event_type NOT NULL`
- `payload JSONB NOT NULL DEFAULT '{}'`

Indexes:
- `(run_id, ts)`
- `(run_id, task_id, ts)`
- `(type, ts)`

---

## 4. Task Lifecycle (FSM) — REQUIRED BEHAVIOR

### 4.1 Task Status FSM (gen_statem)
Use `gen_statem` for active task controllers. DB is SSOT; FSM orchestrates transitions.

States:
- `NEW` → `PLANNED` (planner created decomposition/execution plan)
- `PLANNED` → `READY` (dependencies resolved; `depends_remaining==0`)
- `READY` → `RUNNING` (scheduler claims + acquires file locks + dispatches attempt)
- `RUNNING` → `VERIFYING` (result received)
- `VERIFYING` → `DONE` or `READY` (retry) or `FAILED` (hard stop)

Rules:
- Persist transitions inside transactions when races are possible.
- Write a `task_events` record for every state change.
- Scheduler MUST NOT dispatch without a valid `claim_token` + active lease.

### 4.2 Leasing / Claiming (Idempotent Scheduling)
Scheduler claims tasks using **`SELECT ... FOR UPDATE SKIP LOCKED`** and updates:
- `claim_token`, `claimed_at`, `lease_expires_at`, `assigned_worker_id`

Reclaim policy:
- If `status=RUNNING` and `lease_expires_at < now()`:
  - mark attempt `TIMED_OUT`
  - release locks
  - set task to `READY` with incremented `retry_count`, or `FAILED` if over limit

### 4.3 Retry Policy
- On verification fail:
  - increment `retry_count`
  - if `retry_count <= max_retries` → `READY` with error context in `context_refs`
  - else → `FAILED`
- Each retry creates a new `task_attempts` row.

### 4.4 Circuit Breakers
- **Cost breaker:** if `runs.current_cost >= cost_limit` → stop dispatching, set run `PAUSED`, emit `COST_LIMIT_REACHED`.
- **Depth breaker:** if `tasks.depth_level > runs.settings.max_depth` → hard fail.
- **Loop breaker:** if fingerprint repeats (unique constraint hit) → hard fail with reason `Loop detected`.

---

## 5. Key Algorithms (MVP)

### 5.1 Dependency Handling
When a task becomes `DONE`:
- For each `task_edges` with `from_task_id = done_task`:
  - decrement `depends_remaining` for `to_task_id`
  - if it becomes 0 and status in `PLANNED/BLOCKED` → set `READY`

### 5.2 File Locking
Before dispatching:
- Determine `locked_files` (planner sets it or task definition includes it).
- Acquire locks by inserting rows into `file_locks` with the current `claim_token` and `lease_expires_at`.
- If any insert conflicts (unique violation):
  - do not dispatch
  - keep task `READY` or set `BLOCKED` briefly; retry later

On completion/fail:
- Release locks: delete `file_locks` where `(task_id, claim_token)`.

### 5.3 Fingerprinting (Loop Detection)
Compute `fingerprint_payload` from:
- intent/objective
- key_files (`locked_files`)
- `error_signature` (normalized verification log, if any)
- `diff_summary` (from worker)
Hash canonical JSON → `fingerprint_hash` (SHA256).
Insert into `fingerprints` with UNIQUE `(run_id, fingerprint_hash)`; conflict => loop detected.

---

## 6. Messaging (RabbitMQ) — REQUIRED CONTRACTS

### 6.1 AMQP Exchanges / Queues
Define:
- Exchange `sys.core` (topic)
- Queue `task_dispatch_q` bound to routing key `sys.core.task.dispatch`
- Queue `task_result_q` bound to routing key `sys.core.task.result`
- DLQ `task_dispatch_dlq`, `task_result_dlq`
- Retry queues with TTL (e.g. `task_result_retry_5s`, `task_result_retry_30s`) dead‑lettering back to main

### 6.2 Message Schemas (JSON)
All messages MUST include:
- `project_id`, `run_id`, `task_id`, `attempt_id`
- `correlation_id` (UUID)
- `idempotency_key`
- `claim_token`
- `deadline_ts` (ISO8601)
- `budget: {token_budget, time_budget_seconds, cost_limit_remaining}`

Dispatch message (`sys.core.task.dispatch`):
- `task_type`
- `instruction` (what to do)
- `context` (minimal: capsule summary + relevant file snippets + retrieval refs)
- `model` (provider/model string)
- `files_hint` (`locked_files`)
- `output_contract` (expected artifacts)

Result message (`sys.core.task.result`):
- `status` (OK/ERROR)
- `artifacts[]` (patch/diff/log + sha256)
- `diff_summary`
- `usage_stats` (tokens, cost, latency)
- `error` (message + stack + `error_signature`)

AMQP semantics:
- At‑least‑once delivery must not cause duplicate execution due to DB idempotency.
- Use DLQ + retry TTL for transient failures.

### 6.3 MQTT Topic (UI)
- Topic: `sys.ui.updates`
- Payload: `run_id, task_id, status, log_message, ts, progress(optional)`

UI is read‑only; correctness is enforced by Core + DB.

---

## 7. Worker Adapter (Python) — MVP Responsibilities

Implement `worker.py`:
1) Consume dispatch queue.
2) Call AI provider (OpenAI API for MVP, pluggable).
3) Publish result message with artifacts + usage stats.
4) Optionally publish heartbeats or rely on leases.

Provider interface:
- `execute(context, instruction, model, budget) -> {artifacts, diff_summary, usage_stats}`
- `decompose(context, instruction, model, budget) -> {subtasks_json}`

Worker is stateless; Core handles idempotency using `attempt_id` and `idempotency_key`.

---

## 8. Verifier (Sandbox Runner) — REQUIRED SEPARATION

Verifier is part of Core but runs commands in a controlled environment:
- MVP: local workspace per run (future: container/jail/VM)
- Inputs: patch/diff artifacts + repo checkout
- Outputs: build/test logs as artifacts; verification summary in capsule

Verifier actions:
- Apply patch (`git apply` / 3‑way)
- Run `lint/tests` per `runs.settings.verification_plan`
- Normalize compiler/test output → `error_signature`
- Decide: `DONE` vs `READY` (retry) vs `FAILED`

---

## 9. Tcl/Tk Desktop Client (MVP)

### 9.1 Protocol
Subscribe to MQTT (`sys.ui.updates`) via TCP or WebSockets.

### 9.2 UI Requirements
- Canvas DAG renderer: nodes=tasks, color=status, edges=task_edges.
- Click node: detail panel (events/logs, artifacts list, diff preview, capsule summary).
- Controls: Pause Run, Abort Run, Retry Task, Inject Instruction (creates DIAG/PLAN task).

MVP can use dummy MQTT data first.

---

## 10. Minimal REST API (Erlang Cowboy)

Endpoints:
- `POST /projects`
- `POST /projects/:id/runs` (goal_text, settings)
- `GET /runs/:run_id`
- `GET /runs/:run_id/graph` (tasks + edges)
- `POST /runs/:run_id/pause`
- `POST /runs/:run_id/resume`
- `POST /runs/:run_id/abort`
- `POST /tasks/:task_id/retry`
- `GET /tasks/:task_id/artifacts`
- `GET /tasks/:task_id/events`

Auth (MVP): static API key header.

---

## 11. Implementation Plan (Codex MUST FOLLOW ORDER)

### Step 0 — Repo + Dev Environment
- Create rebar3 app `saai`
- Add docker‑compose (Postgres + RabbitMQ + management)
- Add config templates (DB + AMQP + MQTT)
- Add SQL migrations under `priv/migrations` (versioned)

### Step 1 — PostgreSQL DDL
Generate full DDL for all enums/tables/indexes/constraints above.

### Step 2 — Erlang DB Layer
Implement DB modules:
- transactional helpers
- `create_project/2`, `create_run/3`
- `insert_task/1`, `insert_edges/1`
- `claim_ready_tasks/2` (SKIP LOCKED; set lease)
- `acquire_file_locks/3`, `release_file_locks/2`
- `insert_attempt/…`, `finish_attempt/…`
- `append_event/…`
- `store_artifact/…`, `store_capsule/…`
- `insert_fingerprint/…` (unique constraint => loop)

### Step 3 — Scheduler / Dispatcher
Implement `scheduler.erl`:
- poll READY tasks; respect run status and cost breaker
- claim tasks via DB
- acquire file locks
- create attempt row
- publish AMQP dispatch
- emit events

### Step 4 — Task FSM (gen_statem)
Implement `task_fsm.erl`:
- transition logic with DB updates + events
- start verifier on results
- on DONE: release locks, store capsule, decrement dependents

### Step 5 — AMQP + MQTT Clients
Implement:
- `mq_amqp.erl` (publish/consume, DLQ/retry)
- `mq_mqtt.erl` (publish UI updates from events)

### Step 6 — Python Worker
Implement `worker.py`:
- consumes dispatch
- calls OpenAI API (MVP)
- publishes result message

### Step 7 — Tcl/Tk Prototype
Implement `ui.tcl`:
- connects to MQTT
- renders simple DAG
- node click opens details (may call REST)

---

## 12. Acceptance Criteria (MVP)

1) Create project and run; run creates initial PLAN task.
2) Planner/Decompose creates DAG tasks and edges; tasks persisted in DB.
3) Scheduler dispatches READY tasks with lease + file locks; no double-dispatch under concurrency.
4) Worker returns patch; verifier runs; task becomes DONE or retries with correct policy.
5) Fingerprinting detects repeated cycles and hard-stops a task.
6) Cost limit pauses run.
7) UI shows realtime status updates via MQTT.

---

## 13. Notes for Codex (Quality Constraints)

- Erlang: keep modules small and focused; critical state in DB.
- Use DB transactions for claim/locks and any race-prone transitions.
- Every AMQP message includes correlation/idempotency fields.
- Supervision trees must be correct; use timeouts and backpressure.
- Do not implement multi-tenancy in MVP.

---

# Appendix A — Alternative Backend Option: Mnesia (Erlang)

> Use this appendix ONLY if you explicitly decide to replace PostgreSQL with Mnesia.
> Recommendation: Mnesia is best for **single-node MVP** or as **cache/index**; PostgreSQL remains preferable as SSOT for long‑term robustness and reporting.

## A.1 When Mnesia is Acceptable
- Single orchestrator node (or fixed small cluster) with stable membership.
- You accept limited ad-hoc querying and stronger reliance on event sourcing for audit.

## A.2 Mandatory Additions if Using Mnesia
- Keep `task_attempts`, `fingerprints`, `events` as disc_copies.
- Implement leasing/claiming and idempotency strictly inside `mnesia:transaction/1`.
- Implement file locks as a dedicated table with `{run_id,file_path}` key and lease expiry.
- Implement periodic reclaim of expired leases and locks.

## A.3 Data Model Mapping (Conceptual)
- `projects`, `runs`, `tasks`, `edges`, `attempts`, `file_locks`, `fingerprints`, `events`, `artifacts_meta`
- Store large artifact blobs outside Mnesia (filesystem/object store), keep only hashes + metadata.

## A.4 Key Risk
Network partitions can cause correctness issues in distributed mode. Prefer fail‑stop strategy: pause dispatch on cluster instability.

---

End of document.
