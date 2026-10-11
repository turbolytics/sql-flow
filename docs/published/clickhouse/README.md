# Evidence for the ClickHouse page

The measurements [`../clickhouse.mdx`](../clickhouse.mdx) cites. Its other
claims cite `clickhouse.*` invariants, proven by
`internal/sinks/clickhouse_invariants_test.go`.

| File | What it shows | Produced by |
| --- | --- | --- |
| `selfhosted-verify.out` | The taxi example's Verify counts on self-hosted 26.8.1, and the publish time | the page's steps, on 2026-10-09 |
| `cloudverify.out` | The same counts on Cloud 26.6.1 | the page's steps, then the Verify query |
| `bluesky.out` | The WebSocket example landing 2,000 posts in 40 seconds | the page's steps, on 2026-10-09 |
| `perf.out` | Self-hosted throughput at each `batch_size` | `perf.sh` |
| `perfcloud.out` | Cloud throughput, the previous release as a control, and one count read short from a lagging replica | `perfcloud.sh` |
| `throughput-bisect.out` | Why v2026.10.08 is slower than v2026.09.17.1: the Kafka fetch defaults and jsonparser 1.3.0 | host builds of each commit |
| `window-replay.out` | 1,000 events in one past bucket, read in ten batches, published as one row | `win.sh` with `win.yml` |
| `cloud-dedup.out` | Cloud dropping an identical second block at insert | the queries in its header |
| `cloud-reset.out` | `clickhouse://` against Cloud's TLS port failing at startup | the page's run step with a plain-HTTP DSN |
| `cloud-auth.out` | Cloud's answer to a wrong password: HTTP 401, code 194 | the query in its header |

The scripts expect the dev stack (`make start-backing-services`): Kafka as
`kafka1` and ClickHouse as `clickhouse` on the `dev_default` network. Set
`IMG` to run another image. `perfcloud.sh` reads Cloud credentials from
`~/.sqlflow-clickhouse` or the file `CLICKHOUSE_CREDS` names, and keeps the
password out of the argument list.

A measurement names the versions it ran on. When a release changes one of
these numbers, re-run the script, replace the file, and update the page.
