# Security Policy

## Reporting a vulnerability

Report vulnerabilities privately. Do not open a public issue.

Use one of these channels:

1. [GitHub private vulnerability reporting](https://github.com/turbolytics/sql-flow/security/advisories/new)
   for this repository. Reports stay private until a fix ships, and the
   advisory publishes from the same place.
2. Email `danny@turbolytics.io` if you cannot use GitHub.

Include what you can of the following:

- The version, from `sqlflow version`, or the image tag.
- The pipeline config that reproduces it, with credentials removed.
- What you expected, what happened, and what an attacker gains.

## What to expect

- Acknowledgement within 3 business days.
- A severity assessment and a fix plan once we have reproduced the report.
- Updates as the fix progresses.
- Coordinated disclosure when the fix ships, or 90 days after the report,
  whichever comes first. We will ask before extending that window, and we
  will not extend it without your agreement.
- Credit in the advisory, unless you ask us not to.

## Supported versions

| Version | Receives security fixes |
| ------- | ----------------------- |
| 1.2.x   | Yes                     |
| < 1.2   | No. Upgrade to 1.2.x.   |

Fixes ship as a patch release on the current minor. We do not maintain
long-term support branches for older minors.

## How a fix ships

A fix is a new tag in this repository and a matching image on Docker Hub,
`turbolytics/sql-flow:<tag>`. The advisory names the fixed version.

Build from source at the tag if you run a binary. See the README for the
build steps.

## DuckDB and other dependencies

sqlflow loads `libduckdb` at runtime through the ADBC driver manager. The
supported DuckDB version is pinned in the `DUCKDB_VERSION` file.

When DuckDB publishes a vulnerability that affects sqlflow:

- We bump `DUCKDB_VERSION`, verify, and release. The Docker image bakes in
  the matching `libduckdb`, so pulling the new tag is the whole fix.
- If you run a binary against your own `libduckdb`, you can install the
  fixed DuckDB before we release. The advisory names the DuckDB version that
  is safe.

Report DuckDB vulnerabilities to the [DuckDB project](https://github.com/duckdb/duckdb/security).
Tell us too, so we can release.

Vulnerabilities in Go module dependencies ship as a sqlflow release.

## What counts as a vulnerability

sqlflow runs the SQL in a pipeline config with DuckDB's full capability, in
process. It can read local files, attach databases, load extensions, and
reach the network. That is the product.

**The pipeline config is trusted input.** Anyone who can write it can do
anything the sqlflow process can do. A report that a config can read a file
or reach a host is not a vulnerability. Running configs from untrusted
authors is not a supported deployment.

**Data from sources is untrusted input.** Kafka messages, webhook request
bodies, and websocket frames come from outside. A message that lets its
sender do any of the following is a vulnerability:

- Crash or hang the process.
- Consume memory or disk without bound.
- Execute SQL, reach a file, or reach a host the config did not name.
- Read another pipeline's data.

**Config files hold credentials.** Broker passwords, database DSNs, and
cloud keys live in the config or in `SQLFLOW_`-prefixed environment
variables. Protect them as you would any secret. A vulnerability that
exposes them through a log line, an endpoint, or an error message is in
scope.

Out of scope:

- Anything that requires an attacker who already controls the host or the
  config.
- Throughput or latency under load. Backpressure from a slow sink is by
  design.
- Vulnerabilities in DuckDB, Kafka, or another dependency, in isolation.
  Report those upstream. If sqlflow's use of the dependency makes it worse,
  that is in scope.

## Network surfaces

sqlflow opens no ports by default. Each of these is a flag, and none of them
authenticates:

| Flag                | Address          | Serves                                          |
| ------------------- | ---------------- | ----------------------------------------------- |
| `--metrics`         | `:8000`          | `/metrics`, `/stats`, `/healthz`, `/turbostats` |
| `--pprof`           | `:6060`          | Go profiling, including heap contents            |
| `--with-http-debug` | `127.0.0.1:5000` | `GET /debug?sql=...` against the live database   |

`--metrics` and `--pprof` listen on every interface. Put them behind your
network policy. `--with-http-debug` runs any SQL it is sent, which is why it
binds loopback only. Do not forward it.

A webhook source listens where its config says. The request body is
untrusted input, as above.

## Advisories

Published advisories appear under
[Security Advisories](https://github.com/turbolytics/sql-flow/security/advisories)
for this repository, with a CVE where one applies.
