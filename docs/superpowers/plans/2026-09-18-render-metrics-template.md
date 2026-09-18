# Render Metrics Template Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Deploy to Render button in the sql-flow README that creates a Postgres, a webhook pipeline aggregating a generic metric by minute, and an HTTP API that reads it back.

**Architecture:** `render.yaml` at the repository root points into `render/`, which holds one Docker image used by two Render web services: `sqlflow run` for ingest and `sqlflow serve` for the API. The pipeline upserts closed minutes into `metrics_1m`. Postgres triggers keep a `series` table and a generated six-grain rollup ladder current in the writer's transaction. The API's `metric` and `metric_total` datasets are written by hand, one statement per grain.

**Tech Stack:** sqlflow (webhook source, `StructuredBatch`, tumbling window, `postgres` sink, `serve`, `rollup`), DuckDB SQL, Postgres 18, Docker Compose, bash, jq, GitHub Actions, Render Blueprints.

**Spec:** `docs/superpowers/specs/2026-09-18-render-metrics-template-design.md`

This plan covers build-order rows 3 and 4 of the spec: the template without telemetry, and its first deploy. The telemetry client is a later plan on the same branch. The branch merges to `main` after that plan lands, so the button on `main` never exists without its telemetry disclosure.

## Global Constraints

- **Precondition:** a published `turbolytics/sql-flow` tag that contains `source.webhook.addr`, rollup `numeric: double` and `last`, and the `StructuredBatch` escape fix. This plan calls it `SQLFLOW_TAG`. Replace every `SQLFLOW_TAG` in the files below with the real tag, such as `v2026.09.20`. It appears in `render/Dockerfile`, `render/Makefile` and `render/docker-compose.yml`, and the three must match.
- Every file of the template is under `render/`, except `render.yaml` at the repository root and the CI workflow under `.github/workflows/`.
- The sqlflow binary is not modified by this plan.
- Unsigned mode is chosen, never reached by omission: `SQLFLOW_WEBHOOK_AUTH=none`. A blank secret under `hmac` exits 2.
- `late_rows: drop`. `sqlflow validate` refuses `reemit` with an upserting sink.
- DuckDB reserves `AT`. The handler's timestamp column is `ts`.
- Wire format: one metric in the body's top-level keys, or several under `metrics`. A bare JSON array is not accepted.
- Every file below was written and checked before this plan was: the pipeline, the series trigger and the `metric` dataset ran end to end against `turbolytics/sql-flow:v2026.09.17.3` and Postgres 18 on 2026-09-18, without `addr` and the rollup tables, which need `SQLFLOW_TAG`; `serve.yml` passed `sqlflow validate`; `render.yaml` passed Render's published schema. Type them as written. If one fails, the difference is in the environment or the release, so find it before editing the file.
- Prose follows `CLAUDE.md`. Comments explain why. Commit messages name the defect, the fix, and the evidence.
- Branch from `main`: `feat/render-metrics-template`.

## File Structure

| File | Responsibility |
|---|---|
| `render.yaml` | The Blueprint: one database, two web services, their environment. |
| `render/Dockerfile` | `FROM` the pinned sqlflow image, plus `psql` and `curl`, the configs, migrations and scripts. |
| `render/pipeline.yml` | Webhook source, request schema, handler SQL, window, sink. |
| `render/serve.yml` | The `series`, `metric` and `metric_total` datasets. |
| `render/rollups.yml` | The ladder's declaration. Generates `0003_rollups.sql`. |
| `render/migrations/0001_metrics_1m.sql` | The minute table. |
| `render/migrations/0002_series.sql` | The `series` table and its triggers. |
| `render/migrations/0003_rollups.sql` | Generated. Never edited. |
| `render/bin/migrate.sh` | Applies migrations once, behind an advisory lock, after waiting for the database. |
| `render/bin/entrypoint.sh` | Ingest: environment checks, migrate, `exec sqlflow run`. |
| `render/bin/serve.sh` | API: environment checks, migrate, `exec sqlflow serve`. |
| `render/bin/send.sh` | Signs and posts a body. The README's first request. |
| `render/bin/validate-render.py` | `render.yaml` against Render's published schema. |
| `render/docker-compose.yml` | The local run and the CI run, with a window that closes in seconds. |
| `render/test/e2e.sh` | Posts metrics, reads them back at every grain, checks the auth matrix. |
| `render/Makefile` | `validate`, `rollups`, `up`, `test`, `psql`, `clean`. |
| `render/README.md` | The contract: deploy, send, read, the rules, the API, the settings. |
| `.github/workflows/render-template.yml` | Runs `validate` and `test` when the template changes. |
| `README.md`, `CHANGELOG.md` | The button, and an Unreleased entry. |

---

### Task 1: the schema, and a runner that applies it once

**Files:**
- Create: `render/migrations/0001_metrics_1m.sql`
- Create: `render/migrations/0002_series.sql`
- Create: `render/bin/migrate.sh`

**Interfaces:**
- Produces: tables `metrics_1m` and `series`; `bin/migrate.sh`, which reads `SQLFLOW_POSTGRES_URI`, applies `migrations/*.sql` in filename order, and prints `applied <version>` or `skipped <version>` per file. It resolves `migrations/` relative to the working directory, so callers run it from `render/` or from `/app` in the image.

- [ ] **Step 1: Start a throwaway Postgres**

```bash
docker run -d --rm --name render-tpl-pg -e POSTGRES_PASSWORD=t -p 127.0.0.1:5461:5432 postgres:18
export SQLFLOW_POSTGRES_URI=postgresql://postgres:t@127.0.0.1:5461/postgres
until pg_isready -q -d "$SQLFLOW_POSTGRES_URI"; do sleep 1; done
```

- [ ] **Step 2: Copy the migration runner**

`bin/migrate.sh` is the Bluesky demo's, which already waits for a database that is not up yet and already survives two services starting at once. Copy it at a pinned commit and change the lock's name, so this template and the demo could share a database without sharing a lock:

```bash
mkdir -p render/bin render/migrations
curl -fsSL https://raw.githubusercontent.com/turbolytics/sql-flow-bluesky-demo/6ec4966/bin/migrate.sh -o render/bin/migrate.sh
sed -i.bak "s/sqlflow_bluesky_migrations/sqlflow_render_metrics_migrations/g" render/bin/migrate.sh && rm render/bin/migrate.sh.bak
chmod +x render/bin/migrate.sh
grep -c sqlflow_render_metrics_migrations render/bin/migrate.sh
```

Expected: `2`. Read the file. Its comment about "a Blueprint's first deploy" describes the failure the wait exists for, and stays. Change the one phrase "which is how the worker uses it" to "which is how both services use it".

- [ ] **Step 3: Verify the runner fails with nothing to apply to**

```bash
cd render && bin/migrate.sh; cd ..
psql "$SQLFLOW_POSTGRES_URI" -Atc "SELECT count(*) FROM metrics_1m"
```

Expected: `migrate.sh` fails. With no migration the glob stays literal, and `psql` reports that it cannot open `migrations/*.sql`. The second command fails with `relation "metrics_1m" does not exist`.

- [ ] **Step 4: Write the minute table**

Create `render/migrations/0001_metrics_1m.sql`:

```sql
-- One row per minute per series. A series is a name, a type and a set of
-- dimensions, and dimensions_key is that set in canonical form: keys sorted,
-- every value a string, compact JSON. The pipeline builds it.
--
-- The five values are what every coarser grain can be merged from exactly:
-- sums add, min and max nest, and last is the latest bucket's. An average is
-- value_sum / value_count at any grain. An average stored per minute could
-- not be merged: a minute of one sample would weigh as much as one of 1,000.
CREATE TABLE metrics_1m (
  bucket         TIMESTAMPTZ      NOT NULL,
  name           TEXT             NOT NULL,
  type           TEXT             NOT NULL CHECK (type IN ('count', 'gauge')),
  dimensions_key TEXT             NOT NULL,
  value_sum      DOUBLE PRECISION NOT NULL,
  value_count    BIGINT           NOT NULL,
  value_min      DOUBLE PRECISION NOT NULL,
  value_max      DOUBLE PRECISION NOT NULL,
  value_last     DOUBLE PRECISION NOT NULL,
  -- The event time of value_last.
  last_at        TIMESTAMPTZ      NOT NULL,
  -- The Postgres clock at the first write. A republished minute keeps it.
  updated_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, name, type, dimensions_key)
);

-- The API reads one name over a range.
CREATE INDEX metrics_1m_name_bucket ON metrics_1m (name, bucket);
```

- [ ] **Step 5: Write the series table and its triggers**

Create `render/migrations/0002_series.sql`:

```sql
-- Every series seen, and where a key turns back into dimensions. The rollup
-- tables carry dimensions_key and never jsonb, so the API joins here to
-- filter by dimension and to return dimensions.
CREATE TABLE series (
  name           TEXT        NOT NULL,
  type           TEXT        NOT NULL,
  dimensions_key TEXT        NOT NULL,
  -- dimensions_key is JSON text, so the cast is the whole conversion.
  dimensions     JSONB       NOT NULL,
  first_bucket   TIMESTAMPTZ NOT NULL,
  last_bucket    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (name, type, dimensions_key)
);

-- Kept in the writer's transaction, as the rollups are. least and greatest
-- because a republished minute may be older than what is recorded.
CREATE FUNCTION metrics_series() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  INSERT INTO series (name, type, dimensions_key, dimensions, first_bucket, last_bucket)
  SELECT name, type, dimensions_key, dimensions_key::jsonb, min(bucket), max(bucket)
  FROM changed
  GROUP BY name, type, dimensions_key
  ON CONFLICT (name, type, dimensions_key) DO UPDATE SET
    first_bucket = least(series.first_bucket, excluded.first_bucket),
    last_bucket  = greatest(series.last_bucket, excluded.last_bucket);
  RETURN NULL;
END $fn$;

-- Two triggers: Postgres refuses a transition table on a trigger with more
-- than one event. An upsert fires both, each with its own rows.
CREATE TRIGGER metrics_series_ins AFTER INSERT ON metrics_1m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_series();
CREATE TRIGGER metrics_series_upd AFTER UPDATE ON metrics_1m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_series();
```

- [ ] **Step 6: Apply twice, and check the trigger**

```bash
cd render && bin/migrate.sh && bin/migrate.sh; cd ..
```

Expected: the first run prints `applied 0001_metrics_1m` and `applied 0002_series`. The second prints `skipped` for both.

```bash
psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last, last_at)
VALUES ('2026-09-18 10:05+00', 'cpu', 'gauge', '{"host":"a"}', 1, 1, 1, 1, 1, '2026-09-18 10:05+00');
-- An upsert of an older minute moves first_bucket back and leaves last_bucket.
INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last, last_at)
VALUES ('2026-09-18 10:01+00', 'cpu', 'gauge', '{"host":"a"}', 1, 1, 1, 1, 1, '2026-09-18 10:01+00');
SELECT dimensions->>'host' AS host, first_bucket, last_bucket FROM series;
SQL
```

Expected: one row, `a | 2026-09-18 10:01:00+00 | 2026-09-18 10:05:00+00`.

```bash
psql "$SQLFLOW_POSTGRES_URI" -c "INSERT INTO metrics_1m VALUES (now(), 'x', 'histogram', '{}', 1, 1, 1, 1, 1, now())"
```

Expected: refused, `violates check constraint`.

- [ ] **Step 7: Commit**

```bash
docker stop render-tpl-pg
git add render/migrations/0001_metrics_1m.sql render/migrations/0002_series.sql render/bin/migrate.sh
git commit -m "render: the minute table, the series table, and a runner that applies them once

The template's schema. metrics_1m holds five values per minute per
series, the five every coarser grain merges from exactly. series turns
a dimensions_key back into dimensions and is kept by statement triggers
in the writer's transaction.

migrate.sh is the Bluesky demo's at 6ec4966, under its own lock name.
Applied twice against Postgres 18: the second run skips both. An upsert
of an older minute moves first_bucket back and leaves last_bucket."
```

---

### Task 2: the rollup ladder

**Files:**
- Create: `render/rollups.yml`
- Create: `render/migrations/0003_rollups.sql` (generated)
- Create: `render/Makefile`

**Interfaces:**
- Consumes: `metrics_1m` from Task 1; `sqlflow rollup ddl` and `rollup check` from `SQLFLOW_TAG`.
- Produces: tables `metrics_5m` … `metrics_1d` with `(bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last)`, and `metrics_total_5m` … `metrics_total_1d` with `(bucket, name, type, value_sum, value_count, value_min, value_max)`. Task 3's SQL reads them by these names. `make rollups`, `make validate`.

- [ ] **Step 1: Write the declaration**

Create `render/rollups.yml`. It is `dev/config/rollups/metrics.yml` from the rollup plan with this template's comments:

```yaml
# The rollups Postgres keeps from the pipeline's minute table.
#
#   make rollups    regenerates migrations/0003_rollups.sql
#
# CI runs `sqlflow rollup check`, which fails when the migration differs from
# what this file generates.
#
# There is no serve block. sqlflow rollup generates a dataset of at most one
# dimension, folded. A series here is three columns that cannot fold, so
# serve.yml's metric and metric_total grains are written by hand.
rollups:
  - name: metrics
    source:
      table: metrics_1m
      time_column: bucket
      grain: 1m
      dimensions: [name, type, dimensions_key]

    # Each grain a few times the one below it, so a re-merge reads few rows.
    grains:
      5m: {from: 1m}
      15m: {from: 5m}
      1h: {from: 15m}
      6h: {from: 1h}
      1d: {from: 6h}

    dimension_sets:
      - name: metrics
        dimensions: [name, type, dimensions_key]
        measures:
          # Summed as an integer, a gauge of 0.73 is 1 and one of 0.25 is 0.
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_min: {type: min, column: value_min}
          value_max: {type: max, column: value_max}
          # A gauge's headline value: queue depth now, not its sum.
          value_last: {type: last, column: value_last}
      # A name across every set of dimensions, for a name with a series per
      # user. No last: two series share a bucket here, and neither is later.
      - name: metrics_total
        dimensions: [name, type]
        measures:
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_min: {type: min, column: value_min}
          value_max: {type: max, column: value_max}
```

- [ ] **Step 2: Write the Makefile**

Create `render/Makefile`. Recipes are indented with tabs:

```make
# Must match the Dockerfile and docker-compose.yml.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:SQLFLOW_TAG

.PHONY: validate rollups up test psql clean

# render.yaml is checked against Render's published schema, fetched each run:
# Render renames plans and deprecates keys, and a vendored copy would pass a
# file Render had started to refuse. Needs uv: https://docs.astral.sh/uv/
validate:
	./bin/validate-render.py ../render.yaml
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_WEBHOOK_HMAC_SECRET=x \
		$(SQLFLOW_IMAGE) validate pipeline.yml
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
		$(SQLFLOW_IMAGE) validate serve.yml
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) validate rollups.yml
	@# rollup check requires --serve. rollups.yml generates no dataset, so the
	@# serve file is only loaded, and the migration is what is compared.
	docker run --rm -v $(CURDIR):/w -w /w \
		-e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
		$(SQLFLOW_IMAGE) rollup check \
		-c rollups.yml --migration migrations/0003_rollups.sql --serve serve.yml

rollups:
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) rollup ddl \
		-c rollups.yml > migrations/0003_rollups.sql
	@echo "# regenerated migrations/0003_rollups.sql"

up:
	docker compose up --build

test:
	SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) test/e2e.sh

psql:
	docker compose exec postgres psql -U metrics -d metrics

clean:
	docker compose down -v
```

- [ ] **Step 3: Verify the check fails without a migration**

Run: `make -C render validate`
Expected: FAIL. `validate-render.py` does not exist yet, so the first recipe line fails. Run the check alone to see the failure this task fixes:

```bash
cd render && docker run --rm -v "$PWD":/w -w /w -e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
  turbolytics/sql-flow:SQLFLOW_TAG rollup check -c rollups.yml --migration migrations/0003_rollups.sql --serve serve.yml; cd ..
```

Expected: `migration not found: migrations/0003_rollups.sql`.

- [ ] **Step 4: Generate the migration**

Run: `make -C render rollups`
Expected: `# regenerated migrations/0003_rollups.sql`.

Read `render/migrations/0003_rollups.sql`. Confirm `"metrics_5m"` selects `sum(f."value_sum")::double precision`, `"metrics_total_5m"` has no `value_last`, and there are ten `CREATE TABLE` statements: `grep -c '^CREATE TABLE' render/migrations/0003_rollups.sql` prints `10`.

- [ ] **Step 5: Apply it and watch a minute climb the ladder**

```bash
docker run -d --rm --name render-tpl-pg -e POSTGRES_PASSWORD=t -p 127.0.0.1:5461:5432 postgres:18
export SQLFLOW_POSTGRES_URI=postgresql://postgres:t@127.0.0.1:5461/postgres
until pg_isready -q -d "$SQLFLOW_POSTGRES_URI"; do sleep 1; done
cd render && bin/migrate.sh; cd ..
psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last, last_at) VALUES
 ('2026-09-18 10:01+00', 'cpu', 'gauge', '{"host":"a"}', 0.25, 1, 0.25, 0.25, 0.25, '2026-09-18 10:01+00'),
 ('2026-09-18 10:02+00', 'cpu', 'gauge', '{"host":"a"}', 0.5, 1, 0.5, 0.5, 0.5, '2026-09-18 10:02+00'),
 ('2026-09-18 10:02+00', 'cpu', 'gauge', '{"host":"b"}', 4, 2, 1, 3, 3, '2026-09-18 10:02+00');
SELECT dimensions_key, value_sum, value_count, value_last FROM metrics_1d ORDER BY 1;
SELECT value_sum, value_count, value_min, value_max FROM metrics_total_1d;
SQL
docker stop render-tpl-pg
```

Expected: `{"host":"a"} | 0.75 | 2 | 0.5` and `{"host":"b"} | 4 | 2 | 3`, then `4.75 | 4 | 0.25 | 3`.

- [ ] **Step 6: Commit**

```bash
git add render/rollups.yml render/Makefile render/migrations/0003_rollups.sql
git commit -m "render: five coarser grains, generated, and a total across dimensions

rollups.yml declares the ladder over metrics_1m. metrics keeps every
dimension and all five values. metrics_total drops dimensions_key for a
name with a series per user, and has no last.

0003_rollups.sql is sqlflow rollup ddl's output. Applied to Postgres 18:
0.25 and 0.5 reach the day as 0.75 with last 0.5, and three minutes of
two series total 4.75."
```

---

### Task 3: the API's datasets

**Files:**
- Create: `render/serve.yml`
- Create: `render/bin/serve.sh`

**Interfaces:**
- Consumes: `series`, `metrics_1m`, `metrics_<grain>`, `metrics_total_<grain>` from Tasks 1 and 2.
- Produces: datasets `series`, `metric(name, dimensions, since, until, grain)` and `metric_total(name, since, until, grain)`; environment `SQLFLOW_POSTGRES_URI`, `SQLFLOW_SERVE_CLIENT_ID`, `SQLFLOW_SERVE_PORT`. Task 4's test calls all three datasets with client id `local-dev`.

Twelve grain blocks follow, six per dataset. Within a dataset they differ only in `bucket`, `max_range`, the cache time of the coarse three, and the table. `metric_total` at `1m` is the exception: no table holds a minute's total, so DuckDB sums the series of at most six hours.

Each grain's `$since` and `$until` bind as timestamps, which is what lets DuckDB push the range into Postgres. Do not cast them in the SQL. The containment and the join to `series` run in DuckDB.

- [ ] **Step 1: Write `serve.yml`**

Create `render/serve.yml`:

```yaml
# Serves what the pipeline writes, read-only, from the same Postgres.
#
#   sqlflow serve -c serve.yml
#
# SQLFLOW_POSTGRES_URI     required
# SQLFLOW_SERVE_CLIENT_ID  required. An identifier, not a secret: it names the
#                          caller in the log. Render generates it.
# SQLFLOW_SERVE_PORT       default 8080
#
# The metric and metric_total grains are written by hand. sqlflow rollup
# generates a dataset of at most one dimension, folded, and a series here is
# three columns that cannot fold. Every grain of a dataset is the same
# statement over another table. CI queries each one.
commands:
  - name: pin the session timezone
    sql: |
      SET TimeZone='UTC';

  - name: load postgres extension
    sql: |
      INSTALL postgres;
      LOAD postgres;

  # One scan fans out across ctid ranges, each on its own Postgres connection.
  # The default cap is 64. The Render plan allows far fewer.
  - name: bound the postgres connections one scan may open
    sql: |
      SET pg_connection_limit = 4;

  # READ_ONLY is the guard. A write in a dataset's SQL fails at startup.
  - name: attach postgres
    sql: |
      ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES, READ_ONLY);

serve:
  name: render-metrics-api

  http:
    addr: "0.0.0.0:{{ SQLFLOW_SERVE_PORT|default('8080') }}"

  clients:
    - name: default
      id: "{{ SQLFLOW_SERVE_CLIENT_ID }}"

  pool:
    size: 4

  metrics:
    enabled: true

  limits:
    max_rows: 10000
    timeout_seconds: 10

  datasets:
    - name: series
      description: Every series seen. A series is a name, a type and a set of dimensions. dimensions is a JSON object, as a string.
      cache: {ttl_seconds: 15}
      sql: |
        SELECT name, type, dimensions, first_bucket, last_bucket
        FROM pg.series
        ORDER BY name, type, dimensions_key

    - name: metric
      description: One metric per bucket per series. name is required; without it the answer is empty. dimensions is a JSON object, and the answer holds the series whose dimensions contain every pair in it; a dimensions that is not JSON matches nothing. since and until bound the range, default the last hour, and pick the grain. Read value_sum for a count and value_last for a gauge; value_sum / value_count is the average.
      params:
        - name: name
          type: string
        - name: dimensions
          type: string
        - name: since
          type: timestamp
        - name: until
          type: timestamp
      cache:
        ttl_seconds: 30
      grains:
        1m:
          bucket: 1m
          max_range: 6h
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_1m AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
        5m:
          bucket: 5m
          max_range: 1d
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_5m AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
        15m:
          bucket: 15m
          max_range: 3d
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_15m AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
        1h:
          bucket: 1h
          cache:
            ttl_seconds: 120
          max_range: 14d
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_1h AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
        6h:
          bucket: 6h
          cache:
            ttl_seconds: 300
          max_range: 90d
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_6h AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
        1d:
          bucket: 1d
          cache:
            ttl_seconds: 600
          max_range: 365d
          sql: |
            SELECT m.bucket, m.type, s.dimensions,
                   m.value_sum, m.value_count, m.value_min, m.value_max, m.value_last
            FROM pg.metrics_1d AS m
            JOIN pg.series AS s USING (name, type, dimensions_key)
            WHERE m.name = $name AND s.name = $name
              AND m.bucket >= $since AND m.bucket < $until
              AND json_contains(s.dimensions::JSON,
                    CASE WHEN $dimensions IS NULL THEN '{}'::JSON
                         ELSE TRY_CAST($dimensions AS JSON) END)
            ORDER BY m.bucket, m.type, m.dimensions_key
      range:
        since: since
        until: until
        default: 1h

    - name: metric_total
      description: One metric per bucket, across every series of the name. The answer for a name with many series, such as one per user. name is required. It has no value_last, because the last value of many series is no series' value.
      params:
        - name: name
          type: string
        - name: since
          type: timestamp
        - name: until
          type: timestamp
      cache:
        ttl_seconds: 30
      grains:
        1m:
          bucket: 1m
          max_range: 6h
          sql: |
            SELECT bucket, type,
                   sum(value_sum)::DOUBLE AS value_sum,
                   sum(value_count)::BIGINT AS value_count,
                   min(value_min) AS value_min,
                   max(value_max) AS value_max
            FROM pg.metrics_1m
            WHERE name = $name AND bucket >= $since AND bucket < $until
            GROUP BY bucket, type
            ORDER BY bucket, type
        5m:
          bucket: 5m
          max_range: 1d
          sql: |
            SELECT bucket, type, value_sum, value_count, value_min, value_max
            FROM pg.metrics_total_5m
            WHERE name = $name AND bucket >= $since AND bucket < $until
            ORDER BY bucket, type
        15m:
          bucket: 15m
          max_range: 3d
          sql: |
            SELECT bucket, type, value_sum, value_count, value_min, value_max
            FROM pg.metrics_total_15m
            WHERE name = $name AND bucket >= $since AND bucket < $until
            ORDER BY bucket, type
        1h:
          bucket: 1h
          cache:
            ttl_seconds: 120
          max_range: 14d
          sql: |
            SELECT bucket, type, value_sum, value_count, value_min, value_max
            FROM pg.metrics_total_1h
            WHERE name = $name AND bucket >= $since AND bucket < $until
            ORDER BY bucket, type
        6h:
          bucket: 6h
          cache:
            ttl_seconds: 300
          max_range: 90d
          sql: |
            SELECT bucket, type, value_sum, value_count, value_min, value_max
            FROM pg.metrics_total_6h
            WHERE name = $name AND bucket >= $since AND bucket < $until
            ORDER BY bucket, type
        1d:
          bucket: 1d
          cache:
            ttl_seconds: 600
          max_range: 365d
          sql: |
            SELECT bucket, type, value_sum, value_count, value_min, value_max
            FROM pg.metrics_total_1d
            WHERE name = $name AND bucket >= $since AND bucket < $until
            ORDER BY bucket, type
      range:
        since: since
        until: until
        default: 1h
```

- [ ] **Step 2: Write `serve.sh`**

Create `render/bin/serve.sh`, mode 755:

```bash
#!/usr/bin/env bash
# The API service's entrypoint.
set -euo pipefail

for var in SQLFLOW_POSTGRES_URI SQLFLOW_SERVE_CLIENT_ID; do
  if [ -z "${!var:-}" ]; then
    echo "$var is not set" >&2
    exit 2
  fi
done

# Both services migrate. Whichever starts first applies the schema, and the
# advisory lock in migrate.sh makes the other skip. Without it the API would
# fail its first deploy whenever it won the race: serve prepares every
# dataset at startup, against tables that do not exist yet.
/app/bin/migrate.sh

exec sqlflow serve -c /app/serve.yml "$@"
```

- [ ] **Step 3: Validate**

```bash
cd render && docker run --rm -v "$PWD":/w -w /w -e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_SERVE_CLIENT_ID=x \
  turbolytics/sql-flow:SQLFLOW_TAG validate serve.yml; cd ..
```

Expected: `serve.yml: valid`.

- [ ] **Step 4: Commit**

```bash
chmod +x render/bin/serve.sh
git add render/serve.yml render/bin/serve.sh
git commit -m "render: series, metric and metric_total over HTTP

metric filters by containment: the series whose dimensions include
every pair sent. It joins series in DuckDB, after name and the range
have pushed down to Postgres, so a filtered request scans what an
unfiltered one does. A filter that is not JSON matches nothing rather
than everything. metric_total reads one row per bucket for a name with
many series.

The grains are written by hand because sqlflow rollup generates a
dataset of at most one dimension, folded. The next commit's test
queries every one."
```

---

### Task 4: the pipeline, the image, and a test that reads back what it posts

**Files:**
- Create: `render/test/e2e.sh`
- Create: `render/pipeline.yml`
- Create: `render/bin/entrypoint.sh`
- Create: `render/bin/send.sh`
- Create: `render/Dockerfile`
- Create: `render/docker-compose.yml`

**Interfaces:**
- Consumes: everything from Tasks 1 to 3.
- Produces: the image; `POST /events` on `SQLFLOW_WEBHOOK_PORT`; `make -C render test`.

- [ ] **Step 1: Write the failing test**

Create `render/test/e2e.sh`, mode 755. It asserts, at every one of the six grains: two key orders are one series; one pair of `dimensions` returns the two series that contain it and both pairs return one; a value that is a prefix of another matches nothing; a filter that is not JSON matches nothing; a gauge's `0.25 + 0.5` reads `0.75`; `metric_total` sums three series. It also asserts that seven malformed bodies answer 200 and store nothing, that a dimension value with a quote in it survives, that the entrypoint exits 2 on a blank secret, an unknown auth mode and an unsafe prefix, that a restart skips the migrations, and that `none` with a prefix accepts an unsigned request and drops a name outside the prefix.

```bash
#!/usr/bin/env bash
# Brings the template up, posts metrics, and reads them back at every grain.
# Run from render/: test/e2e.sh. Needs docker, curl, jq and openssl.
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST="http://127.0.0.1:${INGEST_HOST_PORT:-10000}"
API="http://127.0.0.1:${API_HOST_PORT:-8080}"
SECRET=local-secret
GRAINS="1m 5m 15m 1h 6h 1d"

fail() { echo "FAIL: $*" >&2; docker compose logs --tail 40 ingest api >&2 || true; exit 1; }
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# status <expected> <signed|unsigned> <body>
status() {
  local want="$1" mode="$2" body="$3" got sig
  local args=(-s -o /dev/null -w '%{http_code}' -X POST "$INGEST/events" --data-binary "$body")
  if [ "$mode" = signed ]; then
    sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')"
    args+=(-H "X-Signature-256: sha256=$sig")
  fi
  got="$(curl "${args[@]}")"
  [ "$got" = "$want" ] || fail "POST ($mode) $body: status $got, want $want"
}

# get <dataset> [curl --data-urlencode args...] -> response JSON
get() {
  local ds="$1"; shift
  local args=(-s -G "$API/v1/datasets/$ds" --data-urlencode client_id=local-dev)
  for kv in "$@"; do args+=(--data-urlencode "$kv"); done
  curl "${args[@]}"
}

# expect <description> <jq filter that must print true> <json>
expect() {
  [ "$(jq -r "$2" <<<"$3")" = true ] || fail "$1: $(jq -c . <<<"$3")"
  echo "ok   $1"
}

wait_for() {
  local what="$1" url="$2" i
  for i in $(seq 1 60); do
    if curl -s -o /dev/null "$url"; then return 0; fi
    sleep 1
  done
  fail "$what did not come up"
}

echo "== the entrypoint refuses a blank secret and an unknown auth mode"
cleanup
docker compose build -q
for env in "SQLFLOW_WEBHOOK_HMAC_SECRET=" "SQLFLOW_WEBHOOK_AUTH=open" "SQLFLOW_METRIC_NAME_PREFIX=a'b"; do
  code=0
  docker compose run --rm -T --no-deps -e "$env" ingest >/dev/null 2>&1 || code=$?
  [ "$code" = 2 ] || fail "ingest with $env exited $code, want 2"
  echo "ok   $env exits 2"
done

echo "== signed requests land"
docker compose up -d --wait postgres
docker compose up -d ingest api
wait_for ingest "$INGEST/events"
wait_for api "$API/healthz"

# One timestamp for every metric. Without it the posts could straddle a
# minute, and the 1m assertions below would see two buckets.
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

status 400 unsigned '{"name":"unsigned","type":"count"}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"pro"}}'
status 200 signed '{"name":"checkout","type":"count","value":2,"timestamp":"'"$TS"'","dimensions":{"plan":"pro","region":"us-east"}}'
status 200 signed '{"name":"checkout","type":"count","value":5,"timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"free"}}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"eu","plan":"pro"}}'
status 200 signed '{"name":"quote","type":"count","timestamp":"'"$TS"'","dimensions":{"q":"say \"hi\""}}'
status 200 signed '{"metrics":[{"name":"cpu","type":"gauge","value":0.25,"timestamp":"'"$TS"'"},{"name":"cpu","type":"gauge","value":0.5,"timestamp":"'"$TS"'"}]}'
# Each is answered 200 and stores nothing.
status 200 signed '{"name":"bad-type","type":"histogram"}'
status 200 signed '{"name":"bad-value","type":"count","value":"abc"}'
status 200 signed '{"name":"bad-dims","type":"count","dimensions":[1,2]}'
status 200 signed '{"name":"future","type":"count","timestamp":"2030-01-01T00:00:00Z"}'
status 200 signed '{"name":"bad-ts","type":"count","timestamp":"yesterday"}'
status 200 signed '[{"name":"bare-array","type":"count"}]'
status 200 signed 'not json'

echo "== waiting for the minute to close"
for i in $(seq 1 60); do
  n="$(get series | jq '.rows | length')"
  [ "$n" -ge 5 ] && break
  sleep 1
done

series="$(get series)"
expect "series holds the five that were valid, and no bad one" \
  '[.rows[].name] | sort == ["checkout","checkout","checkout","cpu","quote"]' "$series"
expect "a dimension value with a quote in it survives" \
  '[.rows[] | select(.name == "quote") | .dimensions | fromjson | .q] == ["say \"hi\""]' "$series"

for g in $GRAINS; do
  echo "== grain $g"
  # The default range, one hour, is inside every grain's max_range.
  all="$(get metric name=checkout grain="$g")"
  expect "$g: three checkout series" '.grain == "'"$g"'" and (.rows | length) == 3' "$all"
  expect "$g: two key orders were one series, summed" \
    '[.rows[] | select((.dimensions | fromjson) == {"plan":"pro","region":"us-east"}) | [.value_sum, .value_count, .value_min, .value_max]] == [[3,2,1,2]]' "$all"

  one="$(get metric name=checkout grain="$g" 'dimensions={"region":"us-east"}')"
  expect "$g: one pair returns the two series that contain it" '(.rows | length) == 2' "$one"
  both="$(get metric name=checkout grain="$g" 'dimensions={"region":"us-east","plan":"free"}')"
  expect "$g: both pairs return one series" '[.rows[].value_sum] == [5]' "$both"
  none="$(get metric name=checkout grain="$g" 'dimensions={"region":"us"}')"
  expect "$g: a value that is a prefix of another matches nothing" '(.rows | type) == "array" and (.rows | length) == 0' "$none"
  bad="$(get metric name=checkout grain="$g" 'dimensions={not json')"
  expect "$g: a filter that is not JSON matches nothing, and is not an error" '(.rows | type) == "array" and (.rows | length) == 0' "$bad"

  cpu="$(get metric name=cpu grain="$g")"
  expect "$g: a gauge keeps its fraction" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[0.75,2,0.25,0.5]]' "$cpu"

  total="$(get metric_total name=checkout grain="$g")"
  expect "$g: the total sums every series" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[9,4,1,5]]' "$total"
done

noname="$(get metric)"
expect "no name answers empty" '(.rows | type) == "array" and (.rows | length) == 0' "$noname"

echo "== a restart applies no migration twice"
docker compose restart ingest >/dev/null
sleep 3
docker compose logs ingest | grep -q 'skipped 0001_metrics_1m' || fail "the restart did not skip migration 0001"
echo "ok   restart skipped the migrations"

echo "== unsigned mode, and the name prefix"
docker compose stop ingest >/dev/null
SQLFLOW_WEBHOOK_AUTH=none SQLFLOW_WEBHOOK_HMAC_SECRET= SQLFLOW_METRIC_NAME_PREFIX=install. docker compose up -d ingest
wait_for ingest "$INGEST/events"
status 200 unsigned '{"name":"install.deployed","type":"count","dimensions":{"install_id":"abc"}}'
status 200 unsigned '{"name":"other.thing","type":"count"}'
for i in $(seq 1 60); do
  n="$(get series | jq '[.rows[] | select(.name == "install.deployed")] | length')"
  [ "$n" -ge 1 ] && break
  sleep 1
done
series="$(get series)"
expect "the prefixed name landed unsigned" '[.rows[] | select(.name == "install.deployed")] | length == 1' "$series"
expect "the name outside the prefix was dropped" '[.rows[] | select(.name == "other.thing")] | length == 0' "$series"

echo "PASS"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `chmod +x render/test/e2e.sh && make -C render test`
Expected: FAIL at `docker compose build`: there is no `docker-compose.yml`.

- [ ] **Step 3: Write `pipeline.yml`**

Create `render/pipeline.yml`:

```yaml
# Accepts metrics over a webhook, aggregates them in a 1-minute tumbling
# window, and upserts each closed minute into Postgres.
#
# SQLFLOW_POSTGRES_URI            required
# SQLFLOW_WEBHOOK_AUTH            hmac (default) or none. bin/entrypoint.sh
#                                 refuses anything else, and refuses hmac
#                                 without a secret.
# SQLFLOW_WEBHOOK_HMAC_SECRET     required under hmac
# SQLFLOW_WEBHOOK_PORT            default 8001
# SQLFLOW_WEBHOOK_MAX_BODY_BYTES  default 26214400, 25 MiB
# SQLFLOW_METRIC_NAME_PREFIX      default empty. When set, a metric whose name
#                                 does not start with it is dropped.
#
# The four window and flush variables exist so the test closes a minute in
# seconds. Leave them unset in a deploy.
commands:
  - name: pin the session timezone
    sql: |
      SET TimeZone='UTC';

  # One request is one row. A request carries one metric in its top-level
  # keys, or several under metrics. Every scalar is TEXT: StructuredBatch
  # fails the whole batch on a value that does not fit its column, and one
  # sender's "value": "abc" must not cost the others their metrics. The
  # handler casts, and drops what does not cast.
  - name: declare the request schema
    sql: |
      CREATE TABLE IF NOT EXISTS events (
        name TEXT, type TEXT, value TEXT, dimensions TEXT, timestamp TEXT,
        metrics STRUCT(name TEXT, type TEXT, value TEXT, dimensions TEXT, timestamp TEXT)[]
      );

tables:
  sql:
    - name: metrics_1m
      # No unique index, on purpose: in-memory DuckDB never frees rows deleted
      # from an indexed table, and the engine deletes every closed minute. Each
      # batch appends its own rows and emit_sql merges them per minute.
      # See turbolytics/sql-flow#268.
      sql: |
        CREATE TABLE IF NOT EXISTS metrics_1m (
          bucket TIMESTAMPTZ, name TEXT, type TEXT, dimensions_key TEXT,
          value_sum DOUBLE, value_count BIGINT, value_min DOUBLE, value_max DOUBLE,
          value_last DOUBLE, last_at TIMESTAMPTZ
        );

      # late_rows is drop, not reemit. The sink replaces a minute, it does not
      # merge into one, so a late row republished alone would replace the
      # minute's values with its own. sqlflow validate refuses reemit with an
      # upserting sink. A metric whose timestamp is older than the open minutes
      # is dropped and counted in window_late_rows_total.
      window:
        time_column: bucket
        size_seconds: 60
        grace_seconds: {{ SQLFLOW_WINDOW_GRACE_SECONDS|default('60') }}
        idle_close_seconds: {{ SQLFLOW_WINDOW_IDLE_SECONDS|default('60') }}
        late_rows: drop
        poll_interval_seconds: {{ SQLFLOW_WINDOW_POLL_SECONDS|default('10') }}
        # value_last is the value with the latest last_at, not the last row
        # appended: batches of one minute can arrive out of order.
        emit_sql: |
          SELECT bucket, name, type, dimensions_key,
                 sum(value_sum) AS value_sum,
                 sum(value_count)::BIGINT AS value_count,
                 min(value_min) AS value_min,
                 max(value_max) AS value_max,
                 arg_max(value_last, last_at) AS value_last,
                 max(last_at) AS last_at
          FROM closed
          GROUP BY bucket, name, type, dimensions_key

        sink:
          # upsert on the primary key makes at-least-once delivery survivable:
          # a minute written twice replaces itself. updated_at is not in the
          # batch, so a new minute takes the column's DEFAULT now().
          type: postgres
          postgres:
            dsn: "{{ SQLFLOW_POSTGRES_URI }}"
            table: metrics_1m
            mode: upsert
            key: [bucket, name, type, dimensions_key]

pipeline:
  name: render-metrics
  batch_size: 500
  # A lone curl is a partial batch. It waits this long, then the minute has to
  # close, so a first metric is readable after one to two minutes.
  flush_interval_seconds: {{ SQLFLOW_FLUSH_INTERVAL_SECONDS|default('5') }}

  source:
    type: webhook
    webhook:
      addr: "0.0.0.0:{{ SQLFLOW_WEBHOOK_PORT|default('8001') }}"
      max_body_bytes: {{ SQLFLOW_WEBHOOK_MAX_BODY_BYTES|default('26214400') }}
{% if SQLFLOW_WEBHOOK_AUTH|default('hmac') == 'hmac' %}
      signature_type: hmac
      hmac:
        header: X-Signature-256
        sig_key: sha256
        secret: "{{ SQLFLOW_WEBHOOK_HMAC_SECRET }}"
{% endif %}

  handler:
    type: handlers.StructuredBatch
    table: events
    # A row is kept when every rule holds and dropped otherwise. The request
    # was answered 200 before this ran, so nothing here can refuse one.
    #
    # dimensions_key is the series' identity: keys sorted, every value a
    # string, compact JSON. This is its only author. Nothing reads a key back
    # into this form.
    #
    # A timestamp more than a minute ahead is dropped. The window closes
    # minutes against the newest event time it has seen, so one metric dated
    # 2030 would close every minute until 2030 and drop all that follows as
    # late.
    #
    # The column is ts, not at: AT is reserved in DuckDB.
    sql: |
      INSERT INTO metrics_1m
      WITH raw AS (
        SELECT name, type, value, dimensions, timestamp FROM events WHERE metrics IS NULL
        UNION ALL
        SELECT m.name, m.type, m.value, m.dimensions, m.timestamp
        FROM (SELECT unnest(metrics) AS m FROM events WHERE metrics IS NOT NULL)
      ),
      parsed AS (
        SELECT name, type,
               CASE WHEN value IS NULL THEN 1.0 ELSE TRY_CAST(value AS DOUBLE) END AS value,
               TRY(from_json(coalesce(dimensions, '{}'), '"MAP(VARCHAR, VARCHAR)"')) AS dims,
               CASE WHEN timestamp IS NULL THEN now() ELSE TRY_CAST(timestamp AS TIMESTAMPTZ) END AS ts
        FROM raw
      )
      SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket, name, type,
             to_json(map_from_entries(list_sort(map_entries(dims))))::TEXT AS dimensions_key,
             sum(value), count(*), min(value), max(value), arg_max(value, ts), max(ts)
      FROM parsed
      WHERE name IS NOT NULL AND name <> '' AND strlen(name) <= 200
        AND starts_with(name, '{{ SQLFLOW_METRIC_NAME_PREFIX|default('') }}')
        AND type IN ('count', 'gauge')
        AND value IS NOT NULL AND isfinite(value)
        AND dims IS NOT NULL AND cardinality(dims) <= 16
        AND ts IS NOT NULL AND ts <= now() + INTERVAL '60 seconds'
      GROUP BY ALL

  # The window's sink does the writing. A pipeline sink would write every
  # batch, which is the opposite of aggregating.
  sink:
    type: noop
```

- [ ] **Step 4: Write `entrypoint.sh`**

Create `render/bin/entrypoint.sh`, mode 755:

```bash
#!/usr/bin/env bash
# The ingest service's entrypoint: check the environment, provision the
# schema, then run the pipeline.
set -euo pipefail

# Checked here rather than left to sqlflow. An unset template variable renders
# as an empty string, and the error for an empty DSN does not name the cause.
if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

# Unsigned mode is chosen, never reached by omission. Render lets a deploy
# leave the secret's prompt blank, and a blank secret under hmac would be a
# public write endpoint into this database.
case "${SQLFLOW_WEBHOOK_AUTH:-hmac}" in
  hmac)
    if [ -z "${SQLFLOW_WEBHOOK_HMAC_SECRET:-}" ]; then
      echo "SQLFLOW_WEBHOOK_HMAC_SECRET is not set. Set it, or set SQLFLOW_WEBHOOK_AUTH=none to accept unsigned requests." >&2
      exit 2
    fi
    ;;
  none)
    echo "webhook accepts unsigned requests: SQLFLOW_WEBHOOK_AUTH=none" >&2
    ;;
  *)
    echo "SQLFLOW_WEBHOOK_AUTH is '${SQLFLOW_WEBHOOK_AUTH}'. Use hmac or none." >&2
    exit 2
    ;;
esac

# The prefix is rendered into the handler's SQL inside single quotes.
case "${SQLFLOW_METRIC_NAME_PREFIX:-}" in
  *[!A-Za-z0-9._-]*)
    echo "SQLFLOW_METRIC_NAME_PREFIX may hold only letters, digits, '.', '_' and '-'" >&2
    exit 2
    ;;
esac

/app/bin/migrate.sh

# exec keeps sqlflow as PID 1, so the platform's SIGTERM reaches it and the
# graceful drain runs. Without exec, bash holds PID 1 and forwards nothing.
exec sqlflow run -c /app/pipeline.yml "$@"
```

- [ ] **Step 5: Write `send.sh`**

Create `render/bin/send.sh`, mode 755:

```bash
#!/usr/bin/env bash
# Signs a body and posts it to the pipeline.
#
#   URL=https://<ingest>.onrender.com SECRET=<secret> bin/send.sh '{"name":"hello","type":"count"}'
#
# Leave SECRET unset for a pipeline running with SQLFLOW_WEBHOOK_AUTH=none.
set -euo pipefail

body="${1:?usage: URL=... [SECRET=...] send.sh '<json>'}"
url="${URL:?URL is not set}"

args=(-sS -X POST "$url/events" -H 'Content-Type: application/json' --data-binary "$body")
if [ -n "${SECRET:-}" ]; then
  # The signature covers the exact bytes sent. printf '%s' adds no newline,
  # and --data-binary sends the body as is, where -d would strip newlines.
  sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')"
  args+=(-H "X-Signature-256: sha256=$sig")
fi
curl "${args[@]}"
echo
```

- [ ] **Step 6: Write the `Dockerfile`**

Create `render/Dockerfile`:

```dockerfile
# The published sqlflow image carries the matching libduckdb and sets
# SQLFLOW_DUCKDB_LIB. Pinned: a later release could change the config schema
# under this template, and a copy deployed from the button never rebuilds on
# its own. Must match the Makefile and docker-compose.yml.
#
# An argument so an unreleased build can be tried locally:
#   docker build --build-arg SQLFLOW_IMAGE=turbolytics/sql-flow:<tag> render
ARG SQLFLOW_IMAGE=turbolytics/sql-flow:SQLFLOW_TAG
FROM ${SQLFLOW_IMAGE}

# psql and pg_isready apply the migrations. curl sends the install events.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pipeline.yml serve.yml /app/
COPY migrations /app/migrations
COPY bin /app/bin

RUN chmod +x /app/bin/*.sh

# Logs and window timestamps are UTC regardless of the host.
ENV TZ=UTC

# The ingest service's entrypoint. The API service overrides it with
# bin/serve.sh.
ENTRYPOINT ["/app/bin/entrypoint.sh"]
```

- [ ] **Step 7: Write `docker-compose.yml`**

Create `render/docker-compose.yml`. `${SQLFLOW_WEBHOOK_HMAC_SECRET-local-secret}` has no colon on purpose: a variable set to empty stays empty, which is how the test asks for a blank secret.

```yaml
# The template on one machine: the local run, and what CI tests.
#
# The window and flush settings close a minute in seconds, so a test does not
# wait two minutes for its metric. A deploy leaves them at their defaults.
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: metrics
      POSTGRES_PASSWORD: metrics
      POSTGRES_DB: metrics
    ports:
      - "127.0.0.1:${POSTGRES_HOST_PORT:-5442}:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U metrics -d metrics"]
      interval: 2s
      timeout: 3s
      retries: 15

  ingest:
    build:
      context: .
      args:
        SQLFLOW_IMAGE: ${SQLFLOW_IMAGE:-turbolytics/sql-flow:SQLFLOW_TAG}
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SQLFLOW_POSTGRES_URI: postgresql://metrics:metrics@postgres:5432/metrics
      SQLFLOW_WEBHOOK_AUTH: ${SQLFLOW_WEBHOOK_AUTH:-hmac}
      SQLFLOW_WEBHOOK_HMAC_SECRET: ${SQLFLOW_WEBHOOK_HMAC_SECRET-local-secret}
      SQLFLOW_WEBHOOK_PORT: "10000"
      SQLFLOW_METRIC_NAME_PREFIX: ${SQLFLOW_METRIC_NAME_PREFIX:-}
      SQLFLOW_WINDOW_GRACE_SECONDS: "3"
      SQLFLOW_WINDOW_IDLE_SECONDS: "5"
      SQLFLOW_WINDOW_POLL_SECONDS: "1"
      SQLFLOW_FLUSH_INTERVAL_SECONDS: "1"
      SQLFLOW_LOG_LEVEL: INFO
    ports:
      - "127.0.0.1:${INGEST_HOST_PORT:-10000}:10000"

  api:
    build:
      context: .
      args:
        SQLFLOW_IMAGE: ${SQLFLOW_IMAGE:-turbolytics/sql-flow:SQLFLOW_TAG}
    entrypoint: /app/bin/serve.sh
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SQLFLOW_POSTGRES_URI: postgresql://metrics:metrics@postgres:5432/metrics
      SQLFLOW_SERVE_CLIENT_ID: local-dev
      SQLFLOW_LOG_LEVEL: INFO
    ports:
      - "127.0.0.1:${API_HOST_PORT:-8080}:8080"
```

- [ ] **Step 8: Validate the pipeline**

```bash
chmod +x render/bin/*.sh
cd render && docker run --rm -v "$PWD":/w -w /w -e SQLFLOW_POSTGRES_URI=x -e SQLFLOW_WEBHOOK_HMAC_SECRET=x \
  turbolytics/sql-flow:SQLFLOW_TAG validate pipeline.yml; cd ..
```

Expected: `pipeline.yml: valid`, after a line saying the unused-variable check was skipped because the template has control structures.

- [ ] **Step 9: Run the test to verify it passes**

Run: `make -C render test`
Expected: about sixty `ok` lines, then `PASS`. It takes about a minute.

If `a dimension value with a quote in it survives` fails, `SQLFLOW_TAG` lacks the `StructuredBatch` escape fix. If a grain other than `1m` fails with a missing table, it lacks the rollup change or `0003_rollups.sql` was not regenerated with it.

- [ ] **Step 10: Commit**

```bash
git add render/test/e2e.sh render/pipeline.yml render/bin/entrypoint.sh render/bin/send.sh render/Dockerfile render/docker-compose.yml
git commit -m "render: a webhook pipeline that aggregates a metric by minute, tested end to end

pipeline.yml reads one metric from a request's top-level keys or
several from metrics, casts every field in SQL so one sender's bad
value cannot fail a batch, builds the series key, and merges each
closed minute into five values. A timestamp more than a minute ahead is
dropped: the window closes against the newest event time, and one
metric dated 2030 would close every minute until then.

The entrypoint exits 2 on a blank secret, an auth mode that is neither
hmac nor none, and a name prefix that could leave its quotes.

test/e2e.sh posts signed metrics and reads them back at all six grains
of both datasets, and checks that seven malformed bodies answer 200 and
store nothing."
```

---

### Task 5: the Blueprint, its check, and CI

**Files:**
- Create: `render.yaml`
- Create: `render/bin/validate-render.py`
- Create: `.github/workflows/render-template.yml`

**Interfaces:**
- Consumes: `render/Dockerfile`, `bin/serve.sh`, the environment variable names from Tasks 3 and 4, `make validate` and `make test` from Tasks 2 and 4.
- Produces: the Blueprint the button applies.

- [ ] **Step 1: Copy the schema check**

```bash
curl -fsSL https://raw.githubusercontent.com/turbolytics/sql-flow-bluesky-demo/6ec4966/bin/validate-render.py -o render/bin/validate-render.py
sed -i.bak 's/sqlflow-bluesky-demo render.yaml validator/sql-flow render.yaml validator/' render/bin/validate-render.py && rm render/bin/validate-render.py.bak
chmod +x render/bin/validate-render.py
```

It fetches Render's schema on each run, needs `uv`, and exits 0 valid, 1 invalid, 2 when the schema could not be read.

- [ ] **Step 2: Verify the check fails**

Run: `make -C render validate`
Expected: FAIL, `could not read ../render.yaml`.

- [ ] **Step 3: Write `render.yaml`**

Create `render.yaml` at the repository root. It has no `healthCheckPath` on the ingest service: the webhook source answers only `POST /events`. Task 7 finds out whether Render accepts that.

```yaml
# Render Blueprint for the Deploy to Render button in the README: a Postgres,
# a web service that accepts metrics over a signed webhook and aggregates them
# by minute, and a web service that reads them back over HTTP.
#
# This file is at the repository root because the button reads it from there.
# Everything it deploys is under render/.
#
# The deploy asks for one value:
#   SQLFLOW_WEBHOOK_HMAC_SECRET  any long random string. A blank one fails the
#                                deploy: the pipeline will not start unsigned
#                                unless SQLFLOW_WEBHOOK_AUTH is set to none.
databases:
  - name: sqlflow-metrics-db
    plan: 0.1c-256mb
    # Same region as both services: connectionString is the private-network
    # URL, which only resolves within one region.
    region: virginia
    postgresMajorVersion: "18"
    diskSizeGB: 5
    # Off, so storage is watched rather than grown silently. Nothing deletes
    # from the minute table yet.
    storageAutoscalingEnabled: false

services:
  # The pipeline. A web service, not a worker, because only a web service
  # gets a public URL, and senders need one.
  - type: web
    name: sqlflow-metrics-ingest
    runtime: docker
    dockerfilePath: ./render/Dockerfile
    dockerContext: ./render
    plan: 0.5c-512mb
    region: virginia
    # Off: a copy of this file in someone else's workspace would otherwise
    # redeploy whenever this repository's main moves. Quoted: unquoted, YAML
    # reads off as false, and Render's schema wants a trigger name.
    autoDeployTrigger: "off"
    envVars:
      - key: SQLFLOW_POSTGRES_URI
        fromDatabase:
          name: sqlflow-metrics-db
          property: connectionString
      # Prompted for during the deploy.
      - key: SQLFLOW_WEBHOOK_HMAC_SECRET
        sync: false
      # hmac or none. none accepts unsigned requests from anyone.
      - key: SQLFLOW_WEBHOOK_AUTH
        value: hmac
      # pipeline.yml listens on SQLFLOW_WEBHOOK_PORT. PORT tells Render the
      # same port, so it routes there rather than scanning for one.
      - key: SQLFLOW_WEBHOOK_PORT
        value: "10000"
      - key: PORT
        value: "10000"
      - key: SQLFLOW_LOG_LEVEL
        value: INFO

  # The API, from the same image.
  - type: web
    name: sqlflow-metrics-api
    runtime: docker
    dockerfilePath: ./render/Dockerfile
    dockerContext: ./render
    dockerCommand: /app/bin/serve.sh
    plan: 0.5c-512mb
    region: virginia
    healthCheckPath: /healthz
    autoDeployTrigger: "off"
    envVars:
      - key: SQLFLOW_POSTGRES_URI
        fromDatabase:
          name: sqlflow-metrics-db
          property: connectionString
      # An identifier sent as ?client_id=, not a secret. Render mints it once.
      # Copy it from the dashboard.
      - key: SQLFLOW_SERVE_CLIENT_ID
        generateValue: true
      - key: SQLFLOW_SERVE_PORT
        value: "8080"
      - key: PORT
        value: "8080"
      - key: SQLFLOW_LOG_LEVEL
        value: INFO
```

- [ ] **Step 4: Run the check**

Run: `make -C render validate`
Expected: `render.yaml: valid against https://render.com/schema/render.yaml.json`, three `valid` lines, and `rollups.yml, migrations/0003_rollups.sql and serve.yml agree`.

- [ ] **Step 5: Write the workflow**

Create `.github/workflows/render-template.yml`:

```yaml
name: render-template

# The Deploy to Render template under render/. Runs when it, the Blueprint or
# this file changes.
on:
  push:
    branches: [main]
    paths: ["render/**", "render.yaml", ".github/workflows/render-template.yml"]
  pull_request:
    paths: ["render/**", "render.yaml", ".github/workflows/render-template.yml"]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - name: Configs, the generated migration, and render.yaml against Render's schema
        run: make -C render validate
      - name: A render.yaml that Render would refuse is refused
        run: |
          sed 's/plan: 0.5c-512mb/plan: not-a-plan/' render.yaml > "$RUNNER_TEMP/bad.yaml"
          status=0
          render/bin/validate-render.py "$RUNNER_TEMP/bad.yaml" || status=$?
          if [ "$status" != 1 ]; then
            echo "expected status 1 for an invalid plan, got $status" >&2
            exit 1
          fi

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Post metrics and read them back at every grain
        run: make -C render test
```

- [ ] **Step 6: Commit and watch CI**

```bash
git add render.yaml render/bin/validate-render.py .github/workflows/render-template.yml
git commit -m "render: the Blueprint, checked against Render's schema, and CI for the template

render.yaml declares a Postgres and two web services built from
render/. The HMAC secret is sync: false, so the deploy asks for it.
autoDeployTrigger is off, so a copy in someone's workspace does not
redeploy when this repository's main moves.

CI validates the configs, the generated migration and render.yaml, holds
that an invalid plan is refused, and runs the end-to-end test."
git push -u origin feat/render-metrics-template
gh run watch
```

Expected: both jobs pass.

---

### Task 6: the READMEs

**Files:**
- Create: `render/README.md`
- Modify: `README.md` (after the first paragraph)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Write `render/README.md`**

````markdown
# Deploy to Render: a metrics pipeline

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow)

One click creates a Postgres, a sqlflow pipeline that accepts metrics over a
signed webhook and aggregates them by minute, and an HTTP API that reads them
back. Everything it deploys is in this directory, and
[`render.yaml`](../render.yaml) at the repository root declares it.

It is a paid deploy: a `0.1c-256mb` Postgres and two `0.5c-512mb` web
services, in `virginia`. Render shows the price before you confirm.

## Deploy

Render asks for one value:

| Prompt | Type |
|---|---|
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | Any long random string, such as the output of `openssl rand -hex 32`. Keep it: you sign requests with it. |

A blank secret fails the deploy, on purpose. The pipeline writes to your
database, and it will not start unsigned unless you set
`SQLFLOW_WEBHOOK_AUTH=none` yourself.

When the deploy finishes, copy two things from the Render dashboard: the URL
of `sqlflow-metrics-ingest` and of `sqlflow-metrics-api`, and the value of
`SQLFLOW_SERVE_CLIENT_ID` on the API service.

## Send a metric

```sh
export URL=https://sqlflow-metrics-ingest-xxxx.onrender.com
export SECRET=<the secret you typed>
bin/send.sh '{"name":"hello","type":"count","dimensions":{"from":"readme"}}'
```

```
{"status":"received"}
```

`bin/send.sh` is short. The signature is an HMAC-SHA256 of the exact
bytes of the body, hex-encoded, in `X-Signature-256` with a `sha256=` prefix:

```sh
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')
curl -X POST "$URL/events" -H "X-Signature-256: sha256=$sig" --data-binary "$body"
```

## Read it back

A metric is readable once its minute closes, one to two minutes after you
send it. Ask which series exist:

```sh
export API=https://sqlflow-metrics-api-xxxx.onrender.com
export CLIENT_ID=<SQLFLOW_SERVE_CLIENT_ID>
curl -sG "$API/v1/datasets/series" --data-urlencode "client_id=$CLIENT_ID"
```

Then read one:

```sh
curl -sG "$API/v1/datasets/metric" --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode 'name=hello'
```

**The pipeline answers 200 to anything signed.** It acknowledges a request
before it reads it, so a metric with no `name`, a body that is not JSON, and
a bare JSON array all answer `{"status":"received"}` and store nothing. If a
metric does not appear in `series` after two minutes, check it against the
rules below.

## The metric

One metric:

```json
{
  "name": "checkout.completed",
  "type": "count",
  "value": 1,
  "dimensions": {"region": "us-east", "plan": "pro"},
  "timestamp": "2026-09-18T14:03:22Z"
}
```

Several, in one signed request:

```json
{"metrics": [{"name": "a", "type": "count"}, {"name": "b", "type": "gauge", "value": 0.73}]}
```

| Field | Required | Default | Rule |
|---|---|---|---|
| `name` | yes | | 1 to 200 bytes. |
| `type` | yes | | `count` or `gauge`. |
| `value` | no | `1` | A finite number. |
| `dimensions` | no | `{}` | A JSON object of at most 16 keys. Every value is stored as a string: `1` and `"1"` are the same. |
| `timestamp` | no | arrival | RFC 3339. More than a minute in the future is dropped. Older than the open minutes, about two, is dropped: there is no backfill. |

A series is a name, a type and a set of dimensions. Key order does not
matter.

Send `timestamp` when order matters. Metrics without one that arrive within
the same five seconds share an arrival time, and which of them is a gauge's
`value_last` is not defined.

## What is stored

Every minute of every series stores five values: `value_sum`, `value_count`,
`value_min`, `value_max` and `value_last`. `type` does not change what is
stored. It says which value to read: `value_sum` for a count, `value_last`
for a gauge. An average is `value_sum / value_count`, at any grain.

Postgres keeps the same five values at 5m, 15m, 1h, 6h and 1d, by triggers
that run inside the pipeline's own write. [`rollups.yml`](rollups.yml)
declares them, `migrations/0003_rollups.sql` is generated from it with
`make rollups`, and `make validate` fails when it has drifted.

## The API

Every request except `/healthz` and `/metrics` sends `?client_id=`. It names
the caller in the log. It is an identifier, not a secret.

| Route | Returns |
|---|---|
| `GET /v1/datasets/series` | Every series: `name`, `type`, `dimensions`, `first_bucket`, `last_bucket`. |
| `GET /v1/datasets/metric?name=…` | Per bucket per series: the five values, `type`, and `dimensions`. |
| `GET /v1/datasets/metric_total?name=…` | Per bucket, across every series of the name: `value_sum`, `value_count`, `value_min`, `value_max`. |

`dimensions` in a response is a JSON object encoded as a string. Parse it.

`metric` and `metric_total` take:

| Param | Meaning |
|---|---|
| `name` | Required. Without it the answer is empty. |
| `since`, `until` | RFC 3339 with an offset. Encode `+` as `%2B`. Default: the last hour. |
| `grain` | Optional. Without it the API picks the finest grain that covers the range. |
| `dimensions` | `metric` only. A JSON object. The answer holds the series whose dimensions contain every pair in it. |

`dimensions={"region":"us-east"}` returns every plan in us-east, each its own
series. `dimensions={"region":"us-east","plan":"pro"}` returns one series. A
`dimensions` that is not JSON matches nothing.

Use `metric_total` for a name with many series, such as one per user. It
reads one row per bucket. `metric` reads a row per bucket per series, and a
response stops at 10,000 rows and says `"truncated": true`. There is no
filter-then-sum: sum the series of a `metric` response yourself. Every value
but `value_last` adds.

| Grain | Widest range |
|---|---|
| `1m` | 6 hours |
| `5m` | 1 day |
| `15m` | 3 days |
| `1h` | 14 days |
| `6h` | 90 days |
| `1d` | 365 days |

## Settings

Set on the `sqlflow-metrics-ingest` service.

| Variable | Default | Meaning |
|---|---|---|
| `SQLFLOW_WEBHOOK_AUTH` | `hmac` | `none` accepts unsigned requests from anyone who has the URL. |
| `SQLFLOW_WEBHOOK_HMAC_SECRET` | | Required under `hmac`. |
| `SQLFLOW_METRIC_NAME_PREFIX` | empty | When set, a metric whose name does not start with it is dropped. Letters, digits, `.`, `_` and `-`. |
| `SQLFLOW_WEBHOOK_MAX_BODY_BYTES` | 26214400 | A larger body is refused with 413. |

## Run it locally

```sh
make up        # Postgres, the pipeline on :10000, the API on :8080
make test      # posts metrics and reads them back at every grain
make validate  # the configs, the generated migration, and render.yaml
```

Locally the secret is `local-secret`, the client id is `local-dev`, and a
minute closes in seconds.
````

- [ ] **Step 2: Check every command in it against the local run**

```bash
make -C render up
```

In a second shell, from `render/`:

```bash
URL=http://127.0.0.1:10000 SECRET=local-secret bin/send.sh '{"name":"hello","type":"count","dimensions":{"from":"readme"}}'
sleep 15
curl -sG http://127.0.0.1:8080/v1/datasets/series --data-urlencode client_id=local-dev | jq '.rows'
curl -sG http://127.0.0.1:8080/v1/datasets/metric --data-urlencode client_id=local-dev --data-urlencode name=hello | jq '.rows'
```

Expected: `{"status":"received"}`, then one series named `hello`, then one row with `value_sum` 1. Then `make -C render clean`.

- [ ] **Step 3: Add the button to the root README**

In `README.md`, after the opening description and before the first `##` heading, add:

```markdown
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow)

One click runs sqlflow on Render: a webhook that accepts metrics, a one-minute
aggregation into Postgres, and an HTTP API that reads them back. It asks for
one secret and is a paid deploy. See [`render/`](render/README.md).
```

- [ ] **Step 4: Changelog**

Under `## Unreleased`, `### Added`:

```markdown
- A Deploy to Render button. `render.yaml` and `render/` deploy a Postgres, a
  webhook pipeline that aggregates a generic metric by minute, and a
  `sqlflow serve` API over a six-grain rollup ladder. See `render/README.md`.
```

- [ ] **Step 5: Commit**

```bash
git add render/README.md README.md CHANGELOG.md
git commit -m "docs: the Deploy to Render button, and the template's contract

render/README.md is what a deployer reads: the one prompt, the first
signed request, the rules a metric must meet, and the three datasets.
It says plainly that the pipeline answers 200 to anything signed and
stores only what meets the rules. Every command in it was run against
the local compose."
```

---

### Task 7: the first deploy, which becomes our collector

This task is manual and needs a Render workspace with billing. It settles the two checks the spec left open, and the service it creates is the install-telemetry collector.

- [ ] **Step 1: Does the button honor a subdirectory?**

Push a scratch branch that moves `render.yaml` to `render/render.yaml`, with `dockerfilePath` and `dockerContext` unchanged. Open `https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow/tree/<scratch-branch>`.

- If Render finds the Blueprint: move `render.yaml` into `render/` on the feature branch, update the Makefile's `../render.yaml` to `render.yaml`, the workflow's `paths` and `sed`, and both READMEs. Record the result in the spec's Decisions table.
- If Render reports no `render.yaml`: leave it at the root. Record that in the spec. Delete the scratch branch.

- [ ] **Step 2: Deploy from the feature branch**

Open `https://render.com/deploy?repo=https://github.com/turbolytics/sql-flow/tree/feat/render-metrics-template`. Leave `SQLFLOW_WEBHOOK_HMAC_SECRET` blank.

Expected: `sqlflow-metrics-ingest` fails its deploy, and its log ends with `SQLFLOW_WEBHOOK_HMAC_SECRET is not set. Set it, or set SQLFLOW_WEBHOOK_AUTH=none to accept unsigned requests.` `sqlflow-metrics-api` deploys and answers `/healthz`.

- [ ] **Step 3: Make it the collector**

On `sqlflow-metrics-ingest`, set `SQLFLOW_WEBHOOK_AUTH=none`, `SQLFLOW_METRIC_NAME_PREFIX=install.` and `SQLFLOW_WEBHOOK_MAX_BODY_BYTES=4096`. Deploy.

- [ ] **Step 4: Does Render need a health route on the ingest service?**

Watch the deploy. Render marks a web service live when it detects the open port.

- If the deploy goes live: no change.
- If Render reports no open port or a failing health check: the webhook source needs `GET /healthz` on its own listener. Add it to the webhook address spec, plan it, release it, and return here.

- [ ] **Step 5: Send it an install event and read it back**

```bash
URL=https://<ingest>.onrender.com render/bin/send.sh '{"name":"install.deployed","type":"count","dimensions":{"install_id":"manual-check","template":"render-metrics"}}'
URL=https://<ingest>.onrender.com render/bin/send.sh '{"name":"not.an.install","type":"count"}'
sleep 150
curl -sG https://<api>.onrender.com/v1/datasets/series --data-urlencode client_id=<id> | jq '.rows'
curl -sG https://<api>.onrender.com/v1/datasets/metric_total --data-urlencode client_id=<id> --data-urlencode name=install.deployed | jq '.rows'
```

Expected: one series, `install.deployed`, and no `not.an.install`. `metric_total` returns one row with `value_sum` 1.

- [ ] **Step 6: Record what the README needs**

From the Render dashboard, read the ingest service's memory at idle and after the requests above. Add one sentence to `render/README.md` under "Deploy" with the measured number and the date. Read the price Render quoted for the Blueprint and replace "Render shows the price before you confirm" with the quoted figure and its date.

- [ ] **Step 7: Commit, and record the collector's URL**

```bash
git add render/README.md docs/superpowers/specs/2026-09-18-render-metrics-template-design.md
git commit -m "docs: what the first deploy measured, and the two checks it settled"
```

Point the telemetry subdomain, such as `telemetry.turbolytics.io`, at the ingest service: add it as a custom domain on `sqlflow-metrics-ingest` and create the DNS record Render names. Send the Step 5 request to the subdomain and confirm it answers. Write the subdomain into issue #331. The telemetry plan ships it as the constant in `bin/telemetry.sh`, and every deployed copy calls it for as long as it runs, so it must be a hostname we own and not the `onrender.com` URL.

---

## Verification

- [ ] `make -C render validate` and `make -C render test` pass locally and in CI.
- [ ] The collector is live, accepts an unsigned `install.*` metric, drops any other name, and answers `metric_total`.
- [ ] `git grep -n SQLFLOW_TAG` prints nothing: every placeholder is the real tag.
- [ ] The branch is not merged. The telemetry plan lands on it first.
