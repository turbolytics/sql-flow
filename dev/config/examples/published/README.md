# Configs we publish

Every config that appears in third-party documentation lives here. The pages
copy from these files; these files are the source of truth.

The reason is the version floor. A config that only exists in a vendor's docs
is checked by nobody, so it drifts with the engine and the first person to
notice is a reader following the page. Here, the three checks that already
walk `dev/config/examples` cover them: the CLI validator, the loader, and the
generated JSON schema.

| File | Published in |
| --- | --- |
| `clickhouse.taxi.yml` | ClickHouse docs, the NYC taxi walkthrough |
| `clickhouse.bluesky.yml` | ClickHouse docs, the WebSocket example |
| `motherduck.bluesky.websocket.yml` | MotherDuck cookbook, `sqlflow-streaming-websocket` |

When a page changes, change the file here first and copy it out. When the
engine changes, CI tells you which published page needs an edit before a
reader finds out.
