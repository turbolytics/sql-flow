# The harness behind the ClickHouse page's claims

Read [STATUS.md](STATUS.md) first. It holds the findings and what each one
measured. This file says how to run them again.

Everything here was run against `turbolytics/sql-flow:v2026.09.17.1`,
self-hosted ClickHouse 26.8.1.2041 and ClickHouse Cloud 26.4.1.2359, on the
dev stack's `dev_default` network with the containers `kafka1` and
`clickhouse`.

These scripts are scratch, not a supported harness. They are kept because they
are the configuration that reproduces each claim, which is the bar the page
has to meet. Folding them into something the repo checks is Pending item 5 in
STATUS.md.

| File | What it proves |
| --- | --- |
| `probe.sh` | One message through the sink into a table you declare. Every NULL, Enum, Decimal, timestamp and unsupported-type finding came from this. |
| `perf.sh` | One throughput run against self-hosted, with a fresh consumer group. |
| `perfcloud.sh` | The same against Cloud, truncating over HTTP. |
| `win.yml` | The tumbling-window replay: one past bucket, read in many batches. |
| `taxi.yml`, `taxi.sql`, `publish.py` | The page's NYC taxi example, extracted verbatim from the MDX. |
| `bluesky.yml`, `bluesky.sql` | The page's WebSocket example, extracted verbatim. Not yet re-run. |
| `win_events.sample.jsonl` | The first three lines of the window fixture. Regenerate the rest with the loop below. |

## Running them

`probe.sh <name> <table-body> <handler-sql> [dsn]` creates `probe.<name>`,
sends one message through it, and prints what landed.

```bash
./probe.sh enum_nozero "e Enum8('a' = 1, 'b' = 2)" \
  "SELECT CAST(NULL AS VARCHAR) AS e FROM batch"
```

It reads from a topic named `ch-probe`, which needs at least one message:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ch-probe --partitions 1 --replication-factor 1
printf '{"x":1}\n' | docker exec -i kafka1 kafka-console-producer \
  --bootstrap-server localhost:9092 --topic ch-probe
```

`perf.sh <label> <batch_size> <dsn>` runs the taxi pipeline once.

```bash
./perf.sh s20k_a 20000 'clickhouse://default@clickhouse:8123/nyc_taxi'
```

**It takes a fresh consumer group every run, on purpose.** An offset reset
fails silently when another consumer still holds the group, and a stuck
container then starves every later run while reporting zero. That cost two
rounds of numbers.

The window fixture is 500 events inside one past one-minute bucket:

```bash
for i in $(seq 0 499); do
  printf '{"ts":"2026-09-01 12:00:%02d+00","id":%d}\n' $((i % 60)) $i
done > win_events.jsonl
```

Run `win.yml` **without** `--max-msgs`. The bucket closes on
`idle_close_seconds`, and `--max-msgs` stops the process before that fires, so
the window publishes nothing. Let it idle past 15 seconds, then stop it.

## Before a long run

The Docker VM filling up is what killed Kafka in the middle of this work.

```bash
docker exec clickhouse df -h /var/lib/clickhouse
```
