# Published pages

Pages SQLFlow publishes in third-party docs. Each page here is the source of
truth for what a vendor's docs say about SQLFlow. The vendor's repository
gets a copy, by hand, after review.

A page carries an anchor comment beside each claim. MDX renders none of them:

| Anchor | What it cites | What the check requires |
| --- | --- | --- |
| `{/* invariant: <id> */}` | a claim in `docs/coverage/invariants.yml` | the id is declared |
| `{/* measured: <path> */}` | a committed output file that names its versions | the file exists |
| `{/* config: <file> */}` | the YAML block that follows | it equals `dev/config/examples/published/<file>` |
| `{/* feature: <id> */}` | a tooling claim, proven by a named test | the id is declared in `features.yml` |
| `{/* generated: type-table */}` | the type table that follows | it equals what `sink.clickhouse.yml` renders |
| `{/* generated: unsupported-types */}` | the list that follows | it equals the rendered unsupported list |

A page named for a namespace cites every invariant in it: `clickhouse.mdx`
must cite each `clickhouse.*` invariant at least once.

The coverage generator runs the check before it reads any test report, so
`make coverage-page`, `make coverage-check` and CI's Coverage job all fail on
a page that drifted from its evidence. An invariant whose test fails fails
the Integration job, which names the page claim through the invariant's id.

## Pages

| Page | Published in | Evidence |
| --- | --- | --- |
| [`clickhouse.mdx`](clickhouse.mdx) | ClickHouse docs, `docs/integrations/connectors/data-ingestion/community-integrations/sqlflow.mdx` | [`clickhouse/`](clickhouse/README.md) |

## Changing a page

1. Change the claim here, and its invariant, test or measurement with it.
2. Run `make coverage-page` and `uv run --locked pytest tests/tooling -q`.
3. After the pull request merges, copy the page to the vendor's repository.
