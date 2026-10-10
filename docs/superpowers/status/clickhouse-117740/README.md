# The harness behind the ClickHouse page's claims

Read [STATUS.md](STATUS.md) first. It holds the findings and what each one
measured. This file says how to run them again.

Everything here runs against `turbolytics/sql-flow:v2026.10.08` by default,
on the dev stack's `dev_default` network with the containers `kafka1` and
`clickhouse`. Set `IMG` to run another image.

These scripts are scratch, not a supported harness. They are kept because they
are the configuration that reproduces each claim, which is the bar the page
has to meet. Folding them into something the repo checks is Pending item 3 in
STATUS.md.

| File | What it proves |
| --- | --- |
| `probes.sh`, `probes.out` | Every single-message finding: NULL, Enum, Decimal, timestamps, unsupported types, a missing column, the retry contract and the flush interval. |
| `probe.sh` | One message through the sink into a table you declare. `probes.sh` calls it. |
| `dedup.sh`, `dedup.out` | A replayed block against `ReplacingMergeTree` and `MergeTree`, self-hosted. |
| `win.sh`, `win.yml`, `win.out` | The tumbling-window replay: one past bucket, read in ten batches. |
| `perf.sh`, `perf.out` | One throughput run against self-hosted, with a fresh consumer group. |
| `perfcloud.sh` | The same against Cloud, truncating over HTTP. |
| `taxi.yml`, `taxi.sql`, `publish.py` | The page's NYC taxi example, extracted verbatim from the MDX. |
| `bluesky.yml`, `bluesky.sql` | The page's WebSocket example, extracted verbatim. |
| `sqlflow.orig.mdx`, `sqlflow.mdx` | The page at the PR's head `fe238499`, and the draft with every finding applied. |

## Setup

Start the dev stack, then create the topics and the databases:

```bash
make start-backing-services
for t in ch-probe nyc-taxi-trips; do
  docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done
printf '{"x":1}\n' | docker exec -i kafka1 kafka-console-producer \
  --bootstrap-server localhost:9092 --topic ch-probe
docker exec clickhouse clickhouse-client -q 'CREATE DATABASE IF NOT EXISTS probe'
docker exec -i clickhouse clickhouse-client --multiquery < taxi.sql
```

`publish.py` reads `trips_0.gz` from its working directory and needs
`confluent-kafka`:

```bash
curl -O https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz
python publish.py
```

## Running them

```bash
./probes.sh > probes.out
./dedup.sh  > dedup.out
./win.sh    > win.out
./perf.sh s20k_a 20000 'clickhouse://default@clickhouse:8123/nyc_taxi'
```

`probe.sh <name> <table-body> <handler-sql> [dsn]` runs one case:

```bash
./probe.sh enum_nozero "e Enum8('a' = 1, 'b' = 2)" \
  "SELECT CAST(NULL AS VARCHAR) AS e FROM batch"
```

**`perf.sh` takes a fresh consumer group every run, on purpose.** An offset
reset fails silently when another consumer still holds the group, and a stuck
container then starves every later run while reporting zero.

**`win.sh` runs without `--max-msgs`.** The bucket closes on
`idle_close_seconds`, and `--max-msgs` stops the process before that fires.
The script runs the pipeline for 40 seconds, then stops it.

## Before a long run

The Docker VM filling up is what killed Kafka in the middle of this work.

```bash
docker run --rm alpine df -h /
```
