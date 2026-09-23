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

QoS 0 and auto-ack on receipt both lose the batch in flight when the process dies. Sparkplug B is heavier than an MVP needs. Neither is in scope.

## Architecture

```
┌──────────────── docker compose, mem cap 1 GB total ────────────────┐
│                                                                     │
│  collector (Python)  ──QoS 1──▶  mosquitto  ──QoS 1──▶  sqlflow     │
│  sensors/<dev>/<metric>          persistent    manual ack  │        │
│  acked.json (ground truth)       session, disk             ▼        │
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
- `session_expiry_seconds` of 0. The broker drops the session on disconnect.
- `receive_maximum` below `pipeline.batch_size`. The broker stops sending when that many messages are unacknowledged. The source acknowledges only on commit, so every batch would wait out `flush_interval_seconds`.

`receive_maximum` defaults to 65535, the protocol maximum.

### 3. Coverage declaration

`TestToolingCoverageSourceRegistry_MatchesTheConstructorSwitch` requires every source kind to be declared in `docs/coverage/integrations/`. `source.mqtt` declares four source invariants:

- `source.commit.only_processed`: a unit test against a fake client.
- `source.marks.never_regress`: a unit test.
- `source.resume.from_committed`: an integration test against Mosquitto.
- `source.commit.on_revoke`: exempt. MQTT has no partition assignment to revoke. A test proves the exemption.

### 4. Mosquitto (`dev/mosquitto/mosquitto.conf`)

Mosquitto's defaults lose data under this contract. The POC overrides four settings:

| Setting | Default | POC | Why |
|---|---|---|---|
| `max_queued_messages` | 1000 | 0 (unlimited) | The broker discards queued messages beyond the limit for a session. |
| `max_inflight_messages` | 20 | 0 (unlimited) | It caps in-flight messages below the client's Receive Maximum and throttles each batch to 20. |
| `persistence` | false | true, on a volume | Without it, a restart drops every session and queue. |
| `autosave_interval` | 1800 s | 1 s | Mosquitto writes state to disk only at shutdown or on this interval. |

A graceful restart saves state. A `kill -9` of the broker loses up to one `autosave_interval` of messages. That is a Mosquitto limit, not a SQLFlow limit. The POC reports it and does not test it.

### 5. Collector (`dev/iot/collector`, Python)

The collector is the program that runs on the Pi. It uses `paho-mqtt` at QoS 1 with a persistent session. A driver interface produces readings. The `simulated` driver models several environmental sensors at a configured rate. Real sensor drivers replace it on the Pi, and the publish path stays the same.

Every reading has one narrow shape, whatever the sensor:

```json
{"device_id": "pi-1", "sensor": "bme280", "metric": "temperature",
 "value": 21.4, "unit": "C", "seq": 1042, "ts": "2026-09-23T14:02:11.120Z"}
```

A new sensor adds rows, not columns. Adding a sensor never changes the SQL or the table schema.

The collector publishes to `sensors/<device_id>/<metric>`. `seq` counts from 1, per `(device_id, metric)`, with no gaps.

The collector writes `acked.json`: for each `(device_id, metric)`, the set of `seq` values the broker acknowledged. That set is the ground truth for the loss check. A reading the broker never acknowledged may or may not reach the sink. The check makes no claim about it.

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

1. Stop the collector and wait for `acked.json`.
2. Wait for SQLFlow to drain.
3. Stop SQLFlow. DuckDB allows one writer, so the check cannot read the file while SQLFlow holds it.
4. Query `iot.readings` with DuckDB.

The check passes when every acknowledged `(device_id, metric, seq)` appears at least once. The script reports duplicates and does not fail on them.

| # | Scenario | What it does | Reports |
|---|---|---|---|
| 1 | Steady state | 10 minutes at a fixed rate. | msgs/sec, peak RSS per service. |
| 2 | Ceiling | Raise the rate until the broker queue for SQLFlow grows or a service nears its memory limit. | Highest sustained msgs/sec. |
| 3 | SQLFlow crash | `docker kill` SQLFlow mid-stream, then start it again. | Loss check, duplicates, recovery time. |
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

## Risks to verify before the plan

- paho.golang's manual acknowledgement API behaves as described. That includes acknowledging in order and rejecting an acknowledgement from a previous connection.
- Mosquitto 2.x honors the client's Receive Maximum once `max_inflight_messages` is 0.
- The window's `sqlcommand` sink can write to an attached database.
