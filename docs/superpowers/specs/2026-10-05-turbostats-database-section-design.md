# TurboStats `database` section: dbhealth reports to control

**Status:** design, awaiting review.
**Producer:** `turbolytics/dbhealth`, a new binary. Postgres first.
**Consumer:** `turbolytics/sql-flow-control`, a new section of the site.
**Builds on:** `2026-09-19-turbostats-contract-amendment-design.md`. This spec
adds one section to the v1 bundle and changes nothing in the sections that
exist.

## The question

"Is my database healthy?" Someone points `dbhealth` at a connection string,
and within a minute control shows them whether the database is serving, how
close it is to its limits, and whether their data is current. That is the free
tier. Keeping that history and judging it over time is what the paid tiers add.

The pipeline sections answer "is my pipeline healthy?" and stay as they are.
Control gets a second kind of instance, a database, with its own pages and
its own verdicts. The two share the bundle format, the signing, the ingest
path and retention.

## Decisions

1. **One new section, `database`. No new bundle version.** Sections are
   additive under the v1 rules. A bundle carries `database` and no `pipeline`.
2. **Control decides the kind from the sections present.** `pipeline` or
   `serve` means a pipeline. `database` means a database. A bundle with
   neither is a process that reports no work, as today.
3. **The reporter sends facts. Control judges them.** `dbhealth` never sends
   "stale" or "degraded". It sends timestamps, counts and limits. Thresholds
   live in control, so a verdict can change without a reporter redeploy, and
   the free tier can tighten them per org later.
4. **No credentials in the bundle, ever.** The target is a host and a database
   name.
5. **Postgres first, generic config.** The config names a `kind`, and the
   section's shape is the same for every kind. Fields a kind cannot provide
   are absent, not zero.
6. **Honest resources.** Postgres reports connections against `max_connections`
   and sizes on disk. It does not know the host's RAM or free disk from SQL.
   v1 reports what the database knows; host memory and disk come from a later
   host probe, and the spec says so rather than promise a number it cannot get.
7. **Row counts are estimates by default, exact by choice.** Postgres keeps
   `n_live_tup`, free and about 10% off until the next vacuum. `count(*)` on a
   large table is a minute of load. A table opts into exact in config, and the
   bundle says which it sent.

## The section

```jsonc
{
  "v": 1,
  "sent_at": "2026-10-05T12:00:00Z",
  "interval_seconds": 60,
  "last_activity_at": "2026-10-05T11:59:59Z",   // the last successful probe
  "instance": { "id": "dbhealth-billing-pg", "name": "billing", "version": "0.1.0",
                "commit": "…", "arch": "linux/arm64", "config_hash": "sha256:…" },
  "process":  { "started_at": "…", "rss_bytes": 18000000 },

  "database": {
    "kind": "postgres",                       // postgres | mysql | mongo | snowflake | redshift
    "target": "pg-2.internal:5432/billing",   // host:port/database; never a DSN
    "cluster": "billing",                     // groups a primary with its replicas in control
    "server_version": "18.0",

    // Is it serving, and how fast? One round trip per interval.
    "probe": {
      "ok": true,
      "latency_ms": 3,
      "last_ok_at": "2026-10-05T11:59:59Z",
      "consecutive_failures": 0,
      "error": null                           // the error's class when not ok: "refused", "timeout", "auth", "other"
    },

    // How close to its limits.
    "resources": {
      "connections": { "used": 18, "max": 100, "waiting": 0 },
      "size_bytes": 14104255,                 // this database on disk
      "oldest_transaction_seconds": 2,        // an idle-in-transaction session holds vacuum back
      "memory": { "shared_buffers_bytes": 134217728 }   // a setting; usage is a host fact
    },

    // Is the data current and complete? One entry per watched table.
    "tables": [
      {
        "name": "public.usage_per_minute",
        "freshness_column": "minute",
        "newest_at": "2026-10-05T11:59:00Z",   // max(freshness_column); absent when no column
        "rows": 7832,
        "rows_exact": false,                    // true when count(*) ran
        "size_bytes": 1826816,
        "last_vacuum_at": "2026-10-05T03:10:00Z",
        "checked_at": "2026-10-05T11:59:58Z"
      }
    ],

    // Present only when the target replicates. Measured from this
    // endpoint's side: a replica reports how far behind what it serves is;
    // a primary reports what it knows about each replica.
    "replication": {
      "role": "replica",                        // primary | replica
      "lag_seconds": 0.4,                       // replica: now - last replayed
      "last_replayed_at": "2026-10-05T11:59:59Z",
      "upstream": "pg-1.internal:5432"
      // a primary instead carries: "replicas": [{"name": "pg-2", "lag_seconds": 0.4, "state": "streaming"}]
    },

    // What ran this interval, so control can show the cost of watching.
    "collection": { "queries": 9, "duration_ms": 41, "errors": [] }
  }
}
```

### Field rules

- `probe` is always present. It is the one thing a bundle with an unreachable
  database still carries: `ok: false`, the error class, and the failure count.
  Everything else is absent when the probe failed, because nothing else could
  run.
- `newest_at` is an absolute time, not an age. Control computes staleness on
  its own clock and keeps the history. An age would be one reading and useless
  a minute later.
- `rows_exact` says what `rows` is. A graph of estimates that jumps at each
  vacuum is honest only if it is labelled.
- `collection.errors` carries a table's failure without failing the bundle:
  `{"table": "public.x", "error": "permission denied"}`. One table the role
  cannot read does not hide the other nine.
- Every number is a reading at `sent_at`. Nothing is a rate; control derives
  rates from consecutive bundles as it does for pipelines.

## Replicas

One instance per endpoint `dbhealth` connects to, replicas included. "Is it
serving?" is a question about an endpoint: a primary that answers says
nothing about the replica an application reads from. Lag is measured from
the replica's side, how far behind what it serves is, and table freshness on
a replica is a different number by definition. So a primary and two replicas
are three instances in control, each with its own probe. `cluster` groups
them, so control can show the three together and line up the primary's view
of a replica's lag with that replica's own.

One process can watch all three, and several databases on one server. Each
entry in `databases` is one instance with its own id. A replica is a
database being watched, and a unit, as a pipeline is.

## The config

```yaml
# dbhealth.yml
databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_PRIMARY_DSN }}"   # from the environment; the bundle carries host:port/db only
    name: billing-primary               # the instance name control shows
    cluster: billing
  - kind: postgres
    dsn: "{{ DBHEALTH_REPLICA_A_DSN }}"
    name: billing-replica-a
    cluster: billing

probe:
  interval_seconds: 60                # one probe and one bundle per database per interval
  timeout_seconds: 5

tables:
  # Which tables to watch. Exactly one of discover or static.
  discover:
    schemas: [public]                 # default: every schema that is not the kind's own catalog
    exclude: ["sqlflow_*", "*_tmp"]   # globs on the table name
    freshness_columns: [updated_at, created_at, minute, ts]   # the first that exists is the table's
    max_tables: 50                    # a schema with 400 tables is a config decision, not a surprise
  # static:
  #   - name: public.usage_per_minute
  #     freshness_column: minute
  #     rows: exact                   # exact | estimate (default)
  #   - name: public.events
  #     freshness_column: created_at

  rows: estimate                      # the default for every table; a static entry overrides it
  freshness_interval_seconds: 60      # max() on a timestamp column, cheap with an index, not without
  rows_exact_interval_seconds: 3600   # count(*) where a table asked for it

report:
  to: https://control.turbolytics.io
  credential: "{{ TURBOSTATS_CREDENTIAL }}"
  # statsd: localhost:8125            # the second output; the same facts as gauges
```

Config rules:

- **`discover` is the one-minute path.** Point it at a database and every
  table with a known timestamp column is watched. A table with none is still
  watched for rows and size, with no `newest_at`.
- **`static` is the production path.** The tables are named, so a schema
  change cannot add a 2-billion-row table to the watch list by accident.
- **Costly work has its own interval.** `count(*)` runs at
  `rows_exact_interval_seconds`, never every probe. The bundle in between
  carries the last exact count with its `checked_at`.
- **The same file shape for every kind.** `kind: mongo` reads `tables` as
  collections and `freshness_column` as a field. A key a kind cannot use is a
  validation error, named, at start.

## What each kind can provide

v1 ships Postgres. The section is designed so the others fill the same fields:

| Field | Postgres | MySQL | Mongo | Snowflake | Redshift |
|---|---|---|---|---|---|
| `probe` | `SELECT 1` | `SELECT 1` | `ping` | `SELECT 1` | `SELECT 1` |
| `connections` | `pg_stat_activity` / `max_connections` | `Threads_connected` / `max_connections` | `serverStatus.connections` | absent | `stv_sessions` |
| `size_bytes` | `pg_database_size` | `information_schema.tables` | `dbStats` | `DATABASE_STORAGE_USAGE_HISTORY` | `svv_table_info` |
| `tables[].rows` estimate | `n_live_tup` | `table_rows` | `collStats.count` | `information_schema.tables.row_count` | `svv_table_info.tbl_rows` |
| `tables[].newest_at` | `max(col)` | `max(col)` | `find().sort(-1).limit(1)` | `max(col)` | `max(col)` |
| `replication` | `pg_stat_replication`, `pg_last_wal_replay_lsn` | `SHOW REPLICA STATUS` | `rs.status` | absent | absent |

A kind reports the fields it has. Control renders what arrives.

## Control

Today control assumes every instance is a pipeline. This section needs two
things from it; everything else, the pages and the verdicts, is control's own
design and not part of this spec.

1. **Kind.** Control decides an instance's kind from the sections present:
   `pipeline` or `serve` means a pipeline, `database` means a database. A
   database instance gets its own section of the site, beside the pipelines,
   not inside them.
2. **The process status is shared.** `up`, `idle`, `unreachable` and `exited`
   are about the reporter process and apply unchanged. A `dbhealth` that stops
   reporting is `unreachable`, as a pipeline is.

The section is stored and retained as every bundle is.

## dbhealth, the binary

- Go, one binary, `dbhealth run -c dbhealth.yml`. It imports
  `github.com/turbolytics/sql-flow/turbostats/wire` for the bundle and the
  signing, like `kafka-connect-turbostats` does.
- Every interval, per database: probe, then resources, then the tables due,
  then one signed POST. A failed probe sends the bundle with `probe` alone.
- A query that fails lands in `collection.errors` and the interval continues.
- `dbhealth validate -c dbhealth.yml` checks the file and, with `--connect`,
  that the role can read every catalog view the kind needs, naming each one it
  cannot. This is the first thing a new user runs, and it must say exactly
  what grant is missing.
- StatsD stays as an output, the same facts as gauges, for the existing
  `db-insights` users and the "plays with Datadog" checkbox.

## What breaks if this is wrong

- **A heavy default.** If discovery watches a huge table with exact counts by
  default, the monitor becomes the load. Hence estimates by default and a
  separate, slow interval for exact counts.
- **A false "healthy".** A reporter that cannot read `pg_stat_activity` would
  send no `connections` and control would show nothing wrong. Hence
  `collection.errors` in every bundle and `validate --connect` before the
  first run.
- **A judgment in the reporter.** If the reporter decided staleness, every
  threshold change would be a redeploy at every customer. Hence facts only;
  control judges.

## Testing

- **Unit, dbhealth:** config validation for every rule above; the Postgres
  queries against a testcontainers Postgres, including a role without
  `pg_read_all_stats` (the errors land in `collection.errors`, the bundle
  still sends); discovery picks the first matching freshness column and
  honours `exclude` and `max_tables`; exact counts run on their own interval.
- **Unit, control:** `Kind` from sections; a bundle with `database` is a
  database instance and not a pipeline.
- **End to end:** `dbhealth` against the usage-metering stack's Postgres,
  reporting to a local control: the table list matches `\dt`, the row count
  for `usage_per_minute` is within 10% of `count(*)`, and stopping Postgres
  sends a bundle with `probe.ok` false and nothing else within two intervals.

## Out of scope

Comparisons across databases (the `db-insights` check), per-query and
slow-query statistics, index advice, host memory and disk, alerting rules,
MySQL, Mongo, Snowflake and Redshift producers. The section shape accommodates
them; v1 does not build them.
