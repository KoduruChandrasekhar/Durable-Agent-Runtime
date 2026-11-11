-- +goose Up
-- +goose StatementBegin

-- Tenants carry their own scheduling counters; the runs trigger below keeps
-- `running` and `pending` exact inside the same transaction as every status
-- change, so fair scheduling never reads a stale count.
CREATE TABLE tenants (
  tenant_id        TEXT PRIMARY KEY,
  weight           INT NOT NULL DEFAULT 1,
  max_running      INT,
  max_pending      INT NOT NULL DEFAULT 50000,
  daily_budget_usd NUMERIC(12,2),
  allowed_models   TEXT[],
  running          INT NOT NULL DEFAULT 0,
  pending          INT NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE runs (
  run_id           UUID PRIMARY KEY,
  tenant_id        TEXT NOT NULL,
  workflow_name    TEXT NOT NULL,
  workflow_version TEXT NOT NULL,
  workflow_key     TEXT GENERATED ALWAYS AS (workflow_name || '@' || workflow_version) STORED,
  status           TEXT NOT NULL,   -- pending | running | waiting | completed | failed | cancelled
  input            JSONB NOT NULL,
  result           JSONB,
  error            JSONB,
  next_seq         BIGINT NOT NULL DEFAULT 1,
  lease_owner      TEXT,
  lease_epoch      BIGINT NOT NULL DEFAULT 0,      -- fencing token: +1 on every claim
  lease_expires_at TIMESTAMPTZ,
  available_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- retry backoff
  wake_at          TIMESTAMPTZ,                     -- durable timers
  waiting_on       JSONB,                           -- wait conditions while waiting
  wait_reason      TEXT,
  last_release     TEXT,                            -- why the run last left a worker
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  priority         SMALLINT NOT NULL DEFAULT 0,
  budget_usd       NUMERIC(12,6),
  spent_usd        NUMERIC(12,6) NOT NULL DEFAULT 0,
  reserved_usd     NUMERIC(12,6) NOT NULL DEFAULT 0,   -- held by in-flight LLM calls
  tainted          BOOLEAN NOT NULL DEFAULT false,
  facts            JSONB NOT NULL DEFAULT '{}'::jsonb,
  dry_run          BOOLEAN NOT NULL DEFAULT false,
  overrides        JSONB,
  forked_from      UUID,
  start_key        TEXT,
  parent_run_id    UUID REFERENCES runs,
  chain_root       UUID NOT NULL,                   -- first run of a continue-as-new chain
  continued_as     UUID,
  first_claimed_at TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, start_key)
);

CREATE INDEX runs_ready        ON runs (priority DESC, available_at) WHERE status = 'pending';
CREATE INDEX runs_ready_tenant ON runs (tenant_id, priority DESC, available_at) WHERE status = 'pending';
CREATE INDEX runs_waking       ON runs (wake_at) WHERE status = 'waiting';
CREATE INDEX runs_leased       ON runs (lease_expires_at) WHERE status = 'running';
CREATE INDEX runs_tenant_created ON runs (tenant_id, created_at DESC);
CREATE INDEX runs_parent       ON runs (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX runs_chain        ON runs (chain_root);

-- Frequent UPDATEs on runs leave dead tuples; vacuum this table aggressively.
ALTER TABLE runs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02,
                      fillfactor = 80);

-- The event log. Partitioned by the month the run was created, so every event
-- of a run lives in one partition and (run_id, seq) stays unique; old
-- partitions of finished runs can be archived or dropped.
CREATE TABLE events (
  run_id     UUID NOT NULL,
  seq        BIGINT NOT NULL,       -- 1, 2, 3 ... per run, no gaps
  type       TEXT NOT NULL,
  step_id    TEXT,
  payload    JSONB NOT NULL,        -- or {"blob": "sha256:..."} when large
  epoch      BIGINT NOT NULL,       -- lease epoch of the writer
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  run_month  DATE NOT NULL,
  PRIMARY KEY (run_id, seq, run_month)
) PARTITION BY RANGE (run_month);

CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE TABLE signals (
  id          BIGSERIAL PRIMARY KEY,
  run_id      UUID NOT NULL REFERENCES runs,
  name        TEXT NOT NULL,
  payload     JSONB,
  signal_id   TEXT NOT NULL,              -- client-chosen id, deduplicated
  consumed_at TIMESTAMPTZ,
  consumed_by TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, signal_id)
);
CREATE INDEX signals_unconsumed ON signals (run_id, name, id) WHERE consumed_at IS NULL;

CREATE TABLE approvals (
  approval_id UUID PRIMARY KEY,
  run_id      UUID NOT NULL REFERENCES runs,
  tenant_id   TEXT NOT NULL,
  step_id     TEXT NOT NULL,
  action      JSONB NOT NULL,
  reason      TEXT NOT NULL,
  rule_ids    TEXT[] NOT NULL DEFAULT '{}',
  approvers   TEXT[] NOT NULL DEFAULT '{}',
  summary     JSONB,
  status      TEXT NOT NULL,             -- pending | approved | rejected | expired
  on_expiry   TEXT NOT NULL DEFAULT 'reject',
  edited_args JSONB,
  comment     TEXT,
  decided_by  TEXT,
  decided_at  TIMESTAMPTZ,
  expires_at  TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX approvals_pending ON approvals (tenant_id, created_at) WHERE status = 'pending';
CREATE INDEX approvals_expiring ON approvals (expires_at) WHERE status = 'pending';

CREATE TABLE outbox (
  id              BIGSERIAL PRIMARY KEY,
  run_id          UUID,
  step_id         TEXT,
  destination     TEXT NOT NULL,
  payload         JSONB NOT NULL,
  dedupe_key      TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL DEFAULT 'pending',  -- pending | delivered | dead
  attempts        INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at    TIMESTAMPTZ
);
CREATE INDEX outbox_due ON outbox (next_attempt_at) WHERE status = 'pending';

CREATE TABLE workers (
  worker_id  TEXT PRIMARY KEY,
  workflows  TEXT[] NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workflows (
  workflow_name    TEXT NOT NULL,
  workflow_version TEXT NOT NULL,
  input_schema     JSONB,
  last_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_name, workflow_version)
);

CREATE TABLE api_keys (
  key_hash   TEXT PRIMARY KEY,           -- sha256 of the key; the key itself is never stored
  tenant_id  TEXT NOT NULL,
  principal  TEXT NOT NULL,
  scopes     TEXT[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at TIMESTAMPTZ
);

-- Per-tenant daily LLM spend, reserved then settled by the model gateway.
CREATE TABLE tenant_spend (
  tenant_id    TEXT NOT NULL,
  day          DATE NOT NULL,
  spent_usd    NUMERIC(12,6) NOT NULL DEFAULT 0,
  reserved_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, day)
);

CREATE FUNCTION keel_runs_count() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO tenants (tenant_id) VALUES (NEW.tenant_id) ON CONFLICT DO NOTHING;
    IF NEW.status IN ('pending', 'running') THEN
      UPDATE tenants
         SET pending = pending + (NEW.status = 'pending')::int,
             running = running + (NEW.status = 'running')::int
       WHERE tenant_id = NEW.tenant_id;
    END IF;
  ELSIF NEW.status IS DISTINCT FROM OLD.status
        AND (NEW.status IN ('pending', 'running') OR OLD.status IN ('pending', 'running')) THEN
    UPDATE tenants
       SET pending = pending + (NEW.status = 'pending')::int - (OLD.status = 'pending')::int,
           running = running + (NEW.status = 'running')::int - (OLD.status = 'running')::int
     WHERE tenant_id = NEW.tenant_id;
  END IF;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER runs_count AFTER INSERT OR UPDATE OF status ON runs
  FOR EACH ROW EXECUTE FUNCTION keel_runs_count();

INSERT INTO tenants (tenant_id) VALUES ('default') ON CONFLICT DO NOTHING;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS tenant_spend, api_keys, workflows, workers, outbox, approvals, signals, events, runs, tenants CASCADE;
DROP FUNCTION IF EXISTS keel_runs_count();
-- +goose StatementEnd
