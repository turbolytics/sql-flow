# Window decisions

Rendered from the truth tables in `internal/managers/decide.go` by a test, so
this page is what runs. A pass reduces what it read to one value per fact,
looks the combination up, and performs the action of the one row it
selects. The check that runs when the package loads proves each combination
selects exactly one row and every row is reachable.

The engine asserts each window's watermark, in the commit that makes the
rows it describes visible: as of that commit, every row the pipeline will
ever write for the window has `time_column` at or after the watermark. How it
is computed -- the newest event time seen per source partition, less the
grace, combined by minimum over the partitions that could still deliver --
is `internal/core/watermarks.go`. The manager reads that one value and
nothing else: no clock, no ticker, no progress row, no reading of the
table's newest bucket. It runs a pass when it starts, when the engine kicks
it after a commit that moved the watermark or admitted a late row, and when
it drains. See `docs/superpowers/specs/2026-09-24-window-watermark-design.md`
and `docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md`.

## The watermark, once per pass

| Fact | Values | Computed from |
|---|---|---|
| `asserted` | `none`, `behind`, `ahead` | The engine's watermark in `sqlflow_watermarks` against the closed watermark in `sqlflow_windows`, both event time. `none`: the engine has asserted nothing for this window. `behind`: the assertion is at or before the closed watermark, so it has been acted on. `ahead`: the assertion is past the closed watermark, or nothing has closed yet. |

3 combinations, 3 rows.

| Rule | asserted | Action | Watermark | Deciding | Claim |
|---|---|---|---|---|---|
| `hold.unasserted` | `none` | `hold` | unchanged | asserted | The engine has asserted no watermark for this window, so nothing is known to be complete and nothing closes. |
| `hold.behind` | `behind` | `hold` | unchanged | asserted | The assertion is at or before what this window has already closed, so it has been acted on; the closed watermark never moves backwards. |
| `follow` | `ahead` | `follow` | the assertion | asserted | The engine has promised that every row it will ever write ends at or after the asserted watermark, so every bucket ending at or before it is complete: the closed watermark follows it, and those buckets publish. |

## Each bucket, given the pass's watermark

| Fact | Values | Computed from |
|---|---|---|
| `bucket` | `open`, `due`, `retained`, `expired` | The bucket's end against the closed watermark, the asserted one, and `allowed_lateness_seconds`. `open`: ends after the asserted watermark. `due`: ends after the closed watermark and at or before the asserted one, so it closes in this pass. `retained`: closed in an earlier pass and its end plus the lateness is still past the assertion. `expired`: closed in an earlier pass and its end plus the lateness is at or before the assertion. |

4 combinations, 4 rows.

| Rule | bucket | Action | Deciding | Claim |
|---|---|---|---|---|
| `keep` | `open` | `keep` | bucket | The bucket ends after the asserted watermark, so its rows stay. |
| `publish` | `due` | `publish` | bucket | The bucket ends between the closed watermark and the asserted one: emit_sql runs over its rows and the result is published. With allowed_lateness_seconds the rows stay for a late row to republish it whole; without, this pass purges them too. |
| `retain` | `retained` | `retain` | bucket | The bucket has been published and its lateness has not run out: its rows stay, and a late row the engine admits republishes it whole. |
| `purge` | `expired` | `purge` | bucket | The bucket ended at or before the asserted watermark less the lateness: nothing more can arrive for it, and its rows are deleted. |

The engine decides a record's lateness at arrival by the same comparison,
before the handler sees it: a record whose bucket ended at or before the
asserted watermark less the lateness is refused and counted; one whose
bucket ended at or before the watermark but within the lateness is written,
and the bucket is republished whole on the next pass; anything else is on
time. So the window table never holds a row the engine did not admit, and
the manager never sweeps.

## What closes a bucket, by configuration

Three keys decide. `grace_seconds` is how far the stream's own clock must
pass a bucket's end before it closes (Flink's bounded out-of-orderness);
`idle_close_seconds` is how long a partition may be silent before it stops
holding the window open (Flink's idleness); `allowed_lateness_seconds` is how
long after a bucket closes a record for it is still admitted, and its rows
kept, so that the bucket can be republished whole (Flink's allowed
lateness).

| | `grace_seconds` | `idle_close_seconds` | the stream this is for | what closes a bucket | the failure mode to know |
|---|---|---|---|---|---|
| **A** | 0 | absent | an ordered stream that never stops | only event time moving past the bucket's end | a stream that stops never publishes its last bucket |
| **B** | >0 | absent | a reordered stream that never stops | event time, less the grace | the same, and the tail is `grace` further behind |
| **C** | 0 | set | ordered, intermittent | event time, or every partition silent for the bound | a bound shorter than the gap *within* a burst closes mid-burst, and the rest of the burst is late |
| **D** | >0 | set | bursty and reordered: the IoT default | either | both of the above |

Each crossed with `allowed_lateness_seconds`: 0 refuses a late record before
the handler and counts it; a positive value admits it and republishes its
bucket as a whole value, so the sink must replace by key.

## Invariants

Each is a claim with a check in the tests.

| ID | Claim |
|---|---|
| `window.never_backwards` | No reading moves the closed watermark behind the committed one, checked over ten thousand random readings here; and the engine's assertion is `max(stored, W)` by construction, checked in `internal/core`. |
| `window.asserted_in_the_commit` | The engine writes the watermark in the transaction that commits the rows it describes, so no reader can see rows without the watermark that accounts for them, or a watermark without its rows. A commit that fails leaves both where they were. Checked in `internal/core`. |
| `window.minimum_over_partitions` | The watermark is the minimum over the partitions that could still deliver: one that has not delivered holds it at -inf, a lagging one holds it, an idle one leaves it, a lost one holds at its last position, a revoked one is gone. Checked in `internal/core` and by the simulator. |
| `window.no_clock_in_the_manager` | The manager has no clock. Every close is `bucket end <= asserted watermark`, in event time; the engine's clock decides only which partitions are in its minimum. Checked by construction: the manager takes no clock and no interval. |
| `window.lateness_decided_at_arrival` | A record whose bucket ended at or before the watermark less the lateness is refused before the handler and counted; one within the lateness is written and its bucket republished whole. The window table never holds a row the engine did not admit. Checked in `internal/core`, by the model and by the simulator. |
