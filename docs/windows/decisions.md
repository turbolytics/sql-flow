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
| `idle` | `off`, `unconfirmed`, `confirmed` | `idle_close_seconds` and the engine's progress row. `off` when not declared. `confirmed` when `last_commit - last_arrival`, bounded by `delivering_for_us`, is at least the bound. `unconfirmed` otherwise, including a row the engine has not written. |
| `source` | `delivering`, `not_delivering` | The engine's progress row. `not_delivering` while the source holds nothing: a consumer between assignments, a websocket reconnecting. A row that never said reads as `delivering`. |

Each fact is measured on one clock, and the two are never compared:

| Fact | Clock | Measured from |
|---|---|---|
| `data` | `event_time` | the window table's newest bucket against the committed watermark |
| `idle` | `engine` | last_commit - last_arrival, bounded by delivering_for_us |
| `source` | `engine` | delivering_for_us, which is negative while the source holds nothing and NULL from one that never said |

24 combinations, 7 rows. A blank cell matches every value.

| Rule | data | idle | source | Action | Watermark | Deciding | Claim |
|---|---|---|---|---|---|---|---|
| `hold.empty` | `none` |  |  | `hold` | unchanged | data | The window holds no rows, so there is nothing to close. |
| `hold.behind` | `behind` |  |  | `hold` | unchanged | data | Everything the window holds ended at or before the watermark, so it has already closed; the watermark never moves backwards. |
| `hold.open` | `open` | `off`, `unconfirmed` |  | `hold` | unchanged | idle | A bucket is open, the stream has not moved past it by the grace, and the engine has not confirmed the stream quiet. |
| `hold.not_delivering` | `open` | `confirmed` | `not_delivering` | `hold` | unchanged | source | The source could not deliver, so silence says nothing about the stream and no bucket closes on idleness across it. |
| `close.grace.not_delivering` | `ripe` | `confirmed` | `not_delivering` | `close.grace` | newest - grace | data | The stream itself moved past the watermark by the grace, which is evidence the data carries and no source has to confirm, so the bucket closes by the grace however long the source held nothing. |
| `close.idle` | `open`, `ripe` | `confirmed` | `delivering` | `close.idle` | newest + size | idle | The engine committed idle_close_seconds after the newest arrival with nothing else arriving, so every open bucket closes, up to the newest bucket's end. |
| `close.grace` | `ripe` | `off`, `unconfirmed` |  | `close.grace` | newest - grace | data | The stream has moved past the watermark by the grace, so the watermark follows it to the newest bucket's start less the grace. |

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
| `window.idle_beats_grace` | Confirmed quiet from a source that could deliver, with anything open, closes by idle, and that close is never below the grace close. Checked over the same readings. |
| `window.silence_needs_a_source` | The same quiet from a source holding nothing closes nothing on its own: an open bucket holds, and a ripe one closes on the grace, which the stream carries and no source has to confirm. Checked over the same readings. |
| `progress.quiet_is_watched` | The progress row never confirms more quiet than the engine spent waiting on a source that could deliver: a restart, a clock step, a sink write held in retries, a consumer between groups and a websocket reconnecting are not quiet, on the idle tick and on the drain alike. Checked in `internal/core`, `internal/kafka` and `internal/websocket`. |
| `progress.late_never_early` | A progress write that is late, or fails, delays a close and never advances one. Checked in `internal/core` and here. |
