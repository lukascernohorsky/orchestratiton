-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Enumerations
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'run_status') THEN
        CREATE TYPE run_status AS ENUM ('NEW','RUNNING','PAUSED','ABORTED','COMPLETED','FAILED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_status') THEN
        CREATE TYPE task_status AS ENUM ('NEW','PLANNED','READY','RUNNING','VERIFYING','DONE','FAILED','BLOCKED','CANCELLED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'attempt_status') THEN
        CREATE TYPE attempt_status AS ENUM ('STARTED','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_type') THEN
        CREATE TYPE event_type AS ENUM (
            'RUN_CREATED','RUN_STATUS_CHANGED',
            'TASK_CREATED','TASK_STATUS_CHANGED','TASK_CLAIMED','TASK_DISPATCHED','TASK_RESULT_RECEIVED',
            'TASK_VERIFICATION_STARTED','TASK_VERIFICATION_FINISHED',
            'ARTIFACT_STORED','CAPSULE_STORED',
            'LOOP_DETECTED','COST_LIMIT_REACHED',
            'LOCK_ACQUIRED','LOCK_RELEASED',
            'WORKER_HEARTBEAT'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    global_settings JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY,
    project_id UUID REFERENCES projects(id),
    status run_status NOT NULL,
    goal_text TEXT NOT NULL,
    settings JSONB NOT NULL DEFAULT '{}',
    current_cost NUMERIC NOT NULL DEFAULT 0,
    current_tokens BIGINT NOT NULL DEFAULT 0,
    cost_limit NUMERIC NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS runs_project_status_idx ON runs (project_id, status);

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs(id),
    parent_id UUID NULL REFERENCES tasks(id),
    task_type TEXT NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    definition_of_done TEXT NOT NULL,
    status task_status NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    depth_level INT NOT NULL DEFAULT 0,
    depends_remaining INT NOT NULL DEFAULT 0,
    assigned_worker_id TEXT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 2,
    token_budget INT NOT NULL DEFAULT 2000,
    time_budget_seconds INT NOT NULL DEFAULT 600,
    idempotency_key TEXT NOT NULL,
    claim_token UUID NULL,
    claimed_at TIMESTAMPTZ NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    last_heartbeat_at TIMESTAMPTZ NULL,
    locked_files TEXT[] NOT NULL DEFAULT '{}',
    context_refs JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE tasks ADD CONSTRAINT tasks_idempotency UNIQUE (run_id, idempotency_key);
CREATE INDEX IF NOT EXISTS tasks_status_priority_idx ON tasks (run_id, status, priority);
CREATE INDEX IF NOT EXISTS tasks_depends_idx ON tasks (run_id, depends_remaining, status);
CREATE INDEX IF NOT EXISTS tasks_parent_idx ON tasks (run_id, parent_id);

CREATE TABLE IF NOT EXISTS task_edges (
    run_id UUID,
    from_task_id UUID,
    to_task_id UUID,
    edge_type TEXT NOT NULL DEFAULT 'blocks',
    PRIMARY KEY (run_id, from_task_id, to_task_id)
);
CREATE INDEX IF NOT EXISTS task_edges_to_idx ON task_edges (run_id, to_task_id);

CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY,
    task_id UUID REFERENCES tasks(id),
    run_id UUID REFERENCES runs(id),
    attempt_no INT NOT NULL,
    status attempt_status NOT NULL,
    worker_id TEXT NULL,
    provider TEXT NULL,
    model TEXT NULL,
    correlation_id UUID NOT NULL,
    prompt_hash TEXT NULL,
    result_fingerprint TEXT NULL,
    error_signature TEXT NULL,
    usage_stats JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL
);
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_unique_attempt UNIQUE (task_id, attempt_no);
CREATE INDEX IF NOT EXISTS task_attempts_run_status_idx ON task_attempts (run_id, status);
CREATE INDEX IF NOT EXISTS task_attempts_started_idx ON task_attempts (task_id, started_at);

CREATE TABLE IF NOT EXISTS file_locks (
    run_id UUID,
    file_path TEXT,
    task_id UUID,
    claim_token UUID,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, file_path)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs(id),
    task_id UUID REFERENCES tasks(id),
    artifact_type TEXT NOT NULL,
    file_path TEXT NULL,
    content_blob BYTEA NULL,
    content_text TEXT NULL,
    diff_content TEXT NULL,
    sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS artifacts_task_idx ON artifacts (run_id, task_id);
CREATE INDEX IF NOT EXISTS artifacts_sha_idx ON artifacts (sha256);

CREATE TABLE IF NOT EXISTS capsules (
    task_id UUID PRIMARY KEY REFERENCES tasks(id),
    run_id UUID REFERENCES runs(id),
    summary_text TEXT NOT NULL,
    decision_logic JSONB NOT NULL DEFAULT '{}',
    assumptions JSONB NOT NULL DEFAULT '{}',
    verification JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fingerprints (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs(id),
    task_id UUID REFERENCES tasks(id),
    fingerprint_hash TEXT NOT NULL,
    fingerprint_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE fingerprints ADD CONSTRAINT fingerprints_unique_hash UNIQUE (run_id, fingerprint_hash);
CREATE INDEX IF NOT EXISTS fingerprints_run_idx ON fingerprints (run_id, created_at);

CREATE TABLE IF NOT EXISTS task_events (
    id UUID PRIMARY KEY,
    run_id UUID REFERENCES runs(id),
    task_id UUID NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    type event_type NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS task_events_run_ts_idx ON task_events (run_id, ts);
CREATE INDEX IF NOT EXISTS task_events_task_ts_idx ON task_events (run_id, task_id, ts);
CREATE INDEX IF NOT EXISTS task_events_type_ts_idx ON task_events (type, ts);
