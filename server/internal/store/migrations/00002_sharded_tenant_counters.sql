-- +goose Up
-- +goose StatementBegin

-- The first benchmark showed every status change of one tenant serializing on
-- that tenant's single counter row. Counters move to 16 shards per tenant:
-- a transition updates the shard of its run (hash of the run id), and readers
-- sum the shards. Tenants with a running cap still get a strict check: the
-- claim locks the tenant row and re-reads the committed sum.
CREATE TABLE tenant_counters (
  tenant_id TEXT NOT NULL,
  shard     SMALLINT NOT NULL,
  running   INT NOT NULL DEFAULT 0,
  pending   INT NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, shard)
);

INSERT INTO tenant_counters (tenant_id, shard, running, pending)
SELECT tenant_id, (abs(hashtext(run_id::text)) % 16)::smallint,
       count(*) FILTER (WHERE status = 'running'), count(*) FILTER (WHERE status = 'pending')
  FROM runs WHERE status IN ('pending', 'running')
 GROUP BY 1, 2;

CREATE OR REPLACE FUNCTION keel_runs_count() RETURNS trigger AS $$
DECLARE
  dp INT := 0;
  dr INT := 0;
  sh SMALLINT;
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO tenants (tenant_id) VALUES (NEW.tenant_id) ON CONFLICT DO NOTHING;
    dp := (NEW.status = 'pending')::int;
    dr := (NEW.status = 'running')::int;
  ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
    dp := (NEW.status = 'pending')::int - (OLD.status = 'pending')::int;
    dr := (NEW.status = 'running')::int - (OLD.status = 'running')::int;
  END IF;
  IF dp <> 0 OR dr <> 0 THEN
    sh := (abs(hashtext(NEW.run_id::text)) % 16)::smallint;
    INSERT INTO tenant_counters AS c (tenant_id, shard, running, pending) VALUES (NEW.tenant_id, sh, dr, dp)
    ON CONFLICT (tenant_id, shard) DO UPDATE SET running = c.running + dr, pending = c.pending + dp;
  END IF;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE VIEW tenant_load AS
SELECT tenant_id, coalesce(sum(running), 0)::int AS running, coalesce(sum(pending), 0)::int AS pending
  FROM tenant_counters GROUP BY tenant_id;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS tenant_load;
DROP TABLE IF EXISTS tenant_counters;
-- +goose StatementEnd
