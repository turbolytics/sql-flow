# MQTT source: an edge POC for a 1 GB Raspberry Pi

Drafted 2026-09-23 against `main` at 723fe60.

## Goal

SQLFlow reads environmental sensor telemetry from MQTT on a 1 GB Raspberry Pi and loses nothing. Three processes share the Pi:

- A Python sensor collector publishes readings.
- Mosquitto brokers them.
- SQLFlow transforms them in SQL and flushes them to a sink.

This POC runs all three in one docker compose stack on a laptop, capped at 1 GB combined. The Pi run follows once the hardware arrives. The sink is a local DuckDB file written by the `sqlcommand` sink. Production points the same `ATTACH` at MotherDuck.

The POC is done when it shows four things:

1. The pipeline runs end to end: collector to Mosquitto to SQLFlow SQL to the DuckDB file.
2. Every reading the broker acknowledged reaches the sink after SQLFlow is killed mid-stream.
3. Every acknowledged reading reaches the sink after Mosquitto restarts mid-stream.
4. A throughput number and peak RSS per service, measured under the 1 GB cap.

## Delivery contract

The source uses MQTT 5 at QoS 1 with a persistent session. It sends PUBACK only when the pipeline commits.

- `clean_start: false` and a nonzero session expiry keep the session across restarts. The broker holds unacknowledged and queued messages for the session and redelivers them when SQLFlow reconnects.
- SQLFlow withholds PUBACK until the sink has flushed the batch. A crash before the flush leaves those messages unacknowledged, and the broker redelivers them.

This is at-least-once, the same guarantee as the Kafka source. Duplicates are by design. The POC judges the source on loss, never on duplication.

The guarantee starts at SQLFlow's first subscribe. MQTT drops a publish that matches no subscription, so a reading published before SQLFlow's session exists has nowhere to queue. The first smoke run lost the first 1.1 s of readings this way: the collector started before SQLFlow connected. On the Pi, this happens once, at first boot. After that the session persists across restarts of either process. The stack starts SQLFlow first and starts the collector after SQLFlow logs `mqtt subscribed`. `mqtt connected` comes too early: it logs before the subscription exists.

QoS 0 and auto-ack on receipt both lose the batch in flight when the process dies. Sparkplug B is heavier than an MVP needs. Neither is in scope.

## Architecture

```
┌──────────────── docker compose, mem cap 1 GB total ────────────────┐
│                                                                     │
│  collector (Python)  ──QoS 1──▶  mosquitto  ──QoS 1──▶  sqlflow     │
│  sensors/<dev>/<metric>          persistent    manual ack  │        │
│  acked.csv (ground truth)        session, disk             ▼        │
│                                                  sqlcommand sink    │
│                                                  ATTACH iot.duckdb  │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. MQTT source (`internal/mqtt`)

The source is a new `core.Source` with type `mqtt`. It uses `github.com/eclipse/paho.golang` (autopaho), which supports MQTT 5, manual acknowledgement, session expiry, and Receive Maximum.

It implements `MarkCommitter`, not plain `Commit`. The source buffers publishes ahead of the pipeline. A plain `Commit` would acknowledge messages the pipeline has not processed. `commitSource` in `internal/core/turbine.go` documents how that lost data in the Kafka source.

The source maps MQTT onto `core.Message` like this:

| `core.Message` field | Value |
|---|---|
| `Value` | The publish payload. |
| `Topic` | The subscription filter that matched, such as `sensors/#`. It is not the concrete topic. |
| `Partition` | 0. |
| `Offset` | A sequence number local to the source. It starts at 0 in each process and never resets on reconnect. |

`Topic` holds the filter, not the concrete topic. Marks and lag are keyed by topic, and one topic per device and metric would make that cardinality unbounded. SQL reads the device and metric from the payload.

`CommitMarks` acknowledges every held publish whose sequence number is at or below the highest mark. It acknowledges in receive order, as MQTT 5 requires. The pipeline processes messages in order, so the highest mark covers every earlier message.

On reconnect, the source discards held acknowledgements from the previous connection. The broker redelivers those messages on the resumed session with new sequence numbers. The pipeline may process a message twice, which is allowed.

The source subscribes with Retain Handling 2. A retained reading is a stale value, not a new event, so it never enters the stream.

`Delivering()` reports true while the client is connected. Reconnects back off from 1 s to 30 s, the same as the websocket source.

`SeekTo` is a no-op. With a `state_path`, `run` passes the stored offsets to `SeekTo`, and a source without the method fails at startup (`internal/cli/run/root.go:523`). The stored offsets are the previous process's local sequence numbers and name nothing on the broker. The broker's session holds the position instead.

A non-empty `Topic` has two side effects in the engine:

- `handlers.InferredMemBatch` adds `kafka_topic`, `kafka_partition`, and `kafka_offset` columns to the batch. For MQTT they hold the filter, 0, and the local sequence number. The example configs select columns by name, so these never reach the sink. Renaming them to source-neutral names is out of scope.
- With a `state_path`, the engine writes one offsets row per filter. The row is harmless, and `SeekTo` ignores it.

A window with a `state_path` can count a reading twice after a crash. The state commit lands, then the process dies before the PUBACK, and the broker redelivers. The Kafka source avoids this by seeking to the offset in the state file. The MQTT source cannot seek. This is at-least-once, and the raw config, which the loss check reads, keeps no state.

### 2. Config

```yaml
source:
  type: mqtt
  mqtt:
    broker: tcp://mosquitto:1883
    client_id: sqlflow-edge-1
    topics: ["sensors/#"]
    session_expiry_seconds: 3600
    receive_maximum: 65535
```

Validation rejects three configs at startup:

- An empty `client_id`. A generated ID starts a new session on every restart, and the broker discards the old session's messages.
- A negative `session_expiry_seconds`. An absent or zero value means the default, 3600. The source never sends 0, because with 0 the broker drops the session on disconnect.
- `receive_maximum` below `pipeline.batch_size`. The broker stops sending when that many messages are unacknowledged. The source acknowledges only on commit, so every batch would wait out `flush_interval_seconds`.

`receive_maximum` defaults to 65535, the protocol maximum. A value above 65535 fails.

The first two checks run when the builder calls `Resolved*` methods on the config, following the webhook source. The builder never sees `batch_size`, so the third check is a method on the pipeline config. `sqlflow validate` and `sqlflow run` both call it, the same way both call `TurboStats.Check`.

### 3. Coverage declaration

`TestToolingCoverageSourceRegistry_MatchesTheConstructorSwitch` requires every source kind to be declared in `docs/coverage/integrations/`. `source.mqtt` implements `Source` and `MarkCommitter`, and requires `unit` and `integration`. Its tests name their invariant with `coverage.Invariant(t, "<invariant>", "source.mqtt")`:

- `source.commit.only_processed`: a unit test against a fake acknowledger, and an integration test against Mosquitto.
- `source.marks.never_regress`: a unit test.
- `source.resume.from_committed`: an integration test against Mosquitto.
- `source.commit.on_revoke`: exempt. MQTT has no partition assignment to revoke. The `proven_by` test asserts that the source implements no revoke hook. That test must exist. The checker accepts any non-empty name, and `source.websocket.yml` names a test that does not exist.

### 4. Mosquitto (`dev/mosquitto/mosquitto.conf`)

Mosquitto's defaults lose data under this contract. The POC overrides three settings:

| Setting | Default | POC | Why |
|---|---|---|---|
| `max_queued_messages` | 1000 | 0 (unlimited) | The broker discards queued messages beyond the limit for a session. |
| `persistence` | false | true, on a volume | Without it, a restart drops every session and queue. |
| `autosave_interval` | 1800 s | 1 s | Mosquitto writes state to disk only at shutdown or on this interval. |

A graceful restart saves state. A `kill -9` of the broker loses up to one `autosave_interval` of messages. That is a Mosquitto limit, not a SQLFlow limit. The POC reports it and does not test it.

Mosquitto holds queued messages in memory. The persistence file is a snapshot of that memory, not a disk-backed queue. An unlimited queue trades silent loss for memory growth while SQLFlow is down. The crash scenario reports broker RSS per queued message. That number sizes `max_queued_bytes` for the Pi, where a bounded queue must fit the 1 GB budget.

`max_inflight_messages` needs no override. Mosquitto 2.1.2 honors an MQTT 5 client's Receive Maximum with that setting at its default of 20.

### 5. Collector (`dev/iot/collector`, Python)

The collector is the program that runs on the Pi. It uses `paho-mqtt` at QoS 1 with a persistent session. A driver interface produces readings. The `simulated` driver models several environmental sensors at a configured rate. Real sensor drivers replace it on the Pi, and the publish path stays the same.

Every reading has one narrow shape, whatever the sensor:

```json
{"device_id": "pi-1", "sensor": "bme280", "metric": "temperature",
 "value": 21.4, "unit": "C", "seq": 1042, "ts": "2026-09-23T14:02:11.120Z"}
```

A new sensor adds rows, not columns. Adding a sensor never changes the SQL or the table schema.

The collector publishes to `sensors/<device_id>/<metric>`. `seq` counts from 1, per `(device_id, metric)`, with no gaps.

The collector appends one row to `acked.csv` for each publish the broker acknowledges: `device_id,metric,seq`. Those rows are the ground truth. DuckDB reads the file directly in the loss check. A reading the broker never acknowledged may or may not reach the sink. The check makes no claim about it.

### 6. SQLFlow configs (`dev/config/examples/`)

Both configs attach the output database from a template variable:

```yaml
commands:
  - name: attach output
    sql: ATTACH '{{ SQLFLOW_OUTPUT_DB|default('/data/iot.duckdb') }}' AS iot
  - name: bound memory
    sql: SET memory_limit = '{{ SQLFLOW_DUCKDB_MEMORY|default('256MB') }}'
```

Production sets `SQLFLOW_OUTPUT_DB=md:iot`, and nothing else changes.

- `mqtt.duckdb.raw.yml` writes every reading. A `sqlcommand` sink runs `INSERT INTO iot.readings SELECT * FROM sqlflow_sink_batch`. The loss check needs every `seq`, so the proof scenarios use this config.
- `mqtt.duckdb.agg.yml` computes a 1-minute tumbling window of min, avg, max, and count per device and metric. The window's sink is `sqlcommand` into `iot.readings_1m`. It shows the edge use case: SQL on the Pi shrinks the upload by the reading rate times 60.

The stack runs one config at a time, so the memory cap covers one DuckDB instance, not two.

### 7. Stack (`dev/mqtt.yml`)

The compose file runs `mosquitto`, `collector`, and `sqlflow`. SQLFlow runs from the release image built by `make sqlflow-image`. The config is mounted. `restart: unless-stopped` stands in for systemd on the Pi.

Memory limits sum to 1 GB. SQLFlow gets the largest share. The POC measures actual use rather than assuming it.

## Proof (`scripts/mqtt-poc.sh`, `make mqtt-poc`)

Each scenario ends the same way:

1. Stop the collector. It writes its last `acked.csv` rows on exit.
2. Wait for SQLFlow to drain.
3. Stop SQLFlow. DuckDB allows one writer, so the check cannot read the file while SQLFlow holds it.
4. Query `iot.readings` with DuckDB.

The check passes when every acknowledged `(device_id, metric, seq)` appears at least once. The script reports duplicates and does not fail on them.

| # | Scenario | What it does | Reports |
|---|---|---|---|
| 1 | Steady state | 10 minutes at a fixed rate. | msgs/sec, peak RSS per service. |
| 2 | Ceiling | Raise the rate until the broker queue for SQLFlow grows or a service nears its memory limit. | Highest sustained msgs/sec. |
| 3 | SQLFlow crash | `docker kill` SQLFlow mid-stream, then start it again. | Loss check, duplicates, recovery time, broker RSS per queued message. |
| 4 | Broker restart | `docker restart` Mosquitto mid-stream. | Loss check, duplicates, recovery time. |

The agg config runs scenario 1 alone. It shows the window's output and its memory use.

The laptop numbers are a baseline for the stack, not a prediction for the Pi. The Pi run repeats the same script.

## Out of scope

- Running on the Pi. The linux/arm64 image already builds. The Pi run is the next step once the hardware arrives.
- MotherDuck and a WAN outage scenario. A remote sink lets an outage exercise the broker as the offline buffer. A local file cannot fail that way.
- TLS and authentication on the broker.
- Sparkplug B, QoS 0, and QoS 2.
- Exposing the concrete MQTT topic to SQL.
- Shared subscriptions (`$share/...`) for more than one SQLFlow instance.

## Verified before the plan

A probe against Mosquitto 2.1.2 and paho.golang v0.23.0 confirmed the following:

- **Receive Maximum:** a subscriber that held 5,000 publishes unacknowledged received all 5,000.
- **Manual acknowledgement:** the subscriber acknowledged the first 2,000. After a graceful broker restart, the broker redelivered exactly 3,000.
- **Default queue limit:** with the default `max_queued_messages`, an offline session kept 1,000 of 5,000 publishes. The broker dropped the rest without an error.

paho.golang details that constrain the source:

- The option is spelled `EnableManualAcknowledgment`.
- `Client.Ack` releases PUBACKs in receive order through its `acksTracker`.
- `Client.Ack` after its connection closes has undefined results (paho issue #160). Each held publish keeps the `*paho.Client` it arrived on, and the source acknowledges only through the current client.

The window's `sqlcommand` sink runs on its own connection from the same DuckDB instance. `ATTACH` is instance-wide, and `dev/bench/bluesky/demo-10x-sqlcommand.yml` relies on it. The agg config's end-to-end run verifies it for this POC.

## Known limits

The final review and its fixes found four engine behaviors that bear on MQTT. None is fixed in this POC:

- **IGNORE on the inferred handler drops a batch of good readings.** `handlers.InferredMemBatch` fails the whole batch when one reading's type conflicts with the others, such as `{"value": "err"}` among numbers. Under `on_error.policy: IGNORE`, the engine then commits the source, and the MQTT source acknowledges every reading in the batch. One such reading lost 999 good readings from every sensor. The raw config uses `handlers.StructuredBatch` with a declared schema instead. It writes NULL for the field that does not fit, and the same reading then cost only itself. The engine fix is to retry a failed batch one message at a time.
- **The structured handler crash-loops under a state path.** On reinitialization it truncates its table and runs `CHECKPOINT`. With a `state_path`, that runs inside the batch transaction, and DuckDB refuses it: "Cannot CHECKPOINT: the current transaction has transaction local changes". So the agg config, which needs a state file, keeps the inferred handler and its batch-drop exposure. A power cut on a Pi is routine. A mistyped reading needs a broken driver.
- **State stats log a false error with an attached database.** With a `state_path`, the stats loop lists the attached `iot` database's tables and queries them without the `iot.` prefix. The pipeline logs "Table with name readings_1m does not exist" every 5 s. The error is noise, not a failure.
- **A receive window of only rejected publishes wedges the source.** A publish the handler rejects at write time advances the marks but not the batch count. The flush tick commits the source only when the batch holds a message. If all `receive_maximum` unacknowledged publishes are rejected, the broker stops sending, and SQLFlow never acknowledges. A restart replays the same window. It needs every sensor to send only unparsable readings. The engine fix is to commit the source on the idle tick when the marks have moved past the committed position.

## Results

`make mqtt-poc` ran on 2026-09-23 on an Apple M1 Pro with Docker 29.8.0, which gives the VM 10 CPUs and 7.65 GiB. SQLFlow ran the linux/arm64 release image, and the collector published 6 metrics for one device. These are laptop numbers, not Pi numbers.

Every scenario delivered every acknowledged reading:

| Scenario | Acked | Missing | Duplicates | Peak MiB: SQLFlow / Mosquitto / collector |
|---|---|---|---|---|
| Steady, 10 min at 1,000/s | 600,000 | 0 | 0 | 78.9 / 4.8 / 14.8 |
| SQLFlow `kill -9` at 40 s, down 20 s | 120,000 | 0 | 0 | 85.3 / 15.2 / 14.5 |
| Mosquitto restart at 40 s | 120,000 | 0 | 252 | 53.0 / 4.5 / 16.1 |

The broker restart produced 252 duplicates. At-least-once allows them.

The whole stack peaked under 120 MiB against the 1 GB cap.

While SQLFlow was down, Mosquitto's queue grew from 990 to 10,983 publishes, and its memory went from a 4.5 MiB baseline to 15.2 MiB. That is about 1 KB of broker memory per queued reading. On a 1 GB Pi, 100 MB of queue holds about 100,000 readings, which is 100 s at 1,000/s or about 3 hours at 10/s. Set `max_queued_bytes` from that budget before production.

The ceiling ramp held each rate for 60 s. SQLFlow kept up whenever the broker's queue stayed flat:

| Target rate | Collector achieved | Queue at 30 s / 60 s |
|---|---|---|
| 1,000/s | 998/s | 350 / 343 |
| 2,000/s | 1,997/s | 107 / 251 |
| 5,000/s | 5,000/s | 1,098 / 1,099 |
| 10,000/s | 9,626/s | 108 / 75 |
| 20,000/s | no summary | 66 / 66 |

- **At 10,000/s,** the single-threaded Python collector topped out below its target while the queue stayed flat. The collector is the ceiling, not SQLFlow.
- **At 20,000/s,** the collector printed no summary before compose stopped it. That step is inconclusive.

A Pi run of the same script gives the real edge numbers.
