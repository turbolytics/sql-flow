# Window decisions

Rendered from the truth tables in `internal/managers/decide.go` by a test, so
this page is what runs. A poll reduces what it read to one value per fact,
looks the combination up, and performs the action of the one row it
selects. The check that runs when the package loads proves each combination
selects exactly one row and every row is reachable.

## The watermark, once per poll

| Fact | Values | Computed from |
|---|---|---|
| `data` | `none`, `behind`, `open`, `ripe` | The newest bucket's start, in event time, against the committed watermark. `none`: no rows. `behind`: newest + size is at or before the watermark, so everything held has closed. `open`: newest + size is after the watermark and newest - grace is not. `ripe`: newest - grace is after the watermark, or there is no watermark yet. |
| `idle` | `off`, `unconfirmed`, `confirmed` | `idle_close_seconds` and the engine's progress row. `off` when not declared. `confirmed` when `last_commit - last_arrival` is at least the bound. `unconfirmed` otherwise, including a row the engine has not written. |

12 combinations, 5 rows. A blank cell matches every value.

| Rule | data | idle | Action | Watermark | Deciding | Claim |
|---|---|---|---|---|---|---|
| `hold.empty` | `none` |  | `hold` | unchanged | data | The window holds no rows, so there is nothing to close. |
| `hold.behind` | `behind` |  | `hold` | unchanged | data | Everything the window holds ended at or before the watermark, so it has already closed; the watermark never moves backwards. |
| `hold.open` | `open` | `off`, `unconfirmed` | `hold` | unchanged | idle | A bucket is open, the stream has not moved past it by the grace, and the engine has not confirmed the stream quiet. |
| `close.idle` | `open`, `ripe` | `confirmed` | `close.idle` | newest + size | idle | The engine committed idle_close_seconds after the newest arrival with nothing else arriving, so every open bucket closes, up to the newest bucket's end. |
| `close.grace` | `ripe` | `off`, `unconfirmed` | `close.grace` | newest - grace | data | The stream has moved past the watermark by the grace, so the watermark follows it to the newest bucket's start less the grace. |

## Each bucket, given the poll's watermark

| Fact | Values | Computed from |
|---|---|---|
| `bucket` | `late`, `due`, `open` | The bucket's end against the previous watermark and the one this poll decided. `late`: at or before the previous, so its rows arrived after it closed. `due`: after the previous and at or before the next. `open`: after the next. |
| `policy` | `drop`, `reemit` | `late_rows` in the window's config. |

6 combinations, 4 rows.

| Rule | bucket | policy | Action | Deciding | Claim |
|---|---|---|---|---|---|
| `keep` | `open` |  | `keep` | bucket | The bucket ends after the watermark, so its rows stay. |
| `close` | `due` |  | `close` | bucket | The bucket ends between the previous watermark and this one: emit_sql runs over its rows and they are deleted. |
| `late.drop` | `late` | `drop` | `late.drop` | policy | The bucket closed before these rows arrived, and late_rows is drop: they are deleted and counted. |
| `late.reemit` | `late` | `reemit` | `late.reemit` | policy | The bucket closed before these rows arrived, and late_rows is reemit: emit_sql runs over the late rows alone, they are deleted and counted. |

## Invariants

Each is a claim with a check in the tests.

| ID | Claim |
|---|---|
| `window.never_backwards` | No reading moves the watermark behind the committed one. Checked over ten thousand random readings. |
| `window.idle_beats_grace` | Confirmed quiet with anything open closes by idle, and that close is never below the grace close. Checked over the same readings. |
| `progress.quiet_is_watched` | The progress row never confirms more quiet than the engine spent waiting on its source: a restart, a clock step and a sink write held in retries are not quiet. Checked in `internal/core`. |
| `progress.late_never_early` | A progress write that is late, or fails, delays a close and never advances one. Checked in `internal/core` and here. |
