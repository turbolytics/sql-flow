package managers

import (
	"fmt"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
)

// The SQL the watermark manager runs. Every statement is generated from the
// declaration, so the collect and the delete cannot disagree, and no clock
// but the watermark appears in any of them. now() is absent on purpose: it
// is frozen at the start of an open transaction, which is the bug #158
// fixed and the reason the user's predicates were hard to get right. The
// watermark itself is read through core.LoadWatermark.

// closedView is the relation emit_sql reads: the rows of every bucket that
// has just closed, or of the one bucket being republished. It is spliced
// into emit_sql as a common table expression rather than created as a view,
// because a CREATE, even of a temporary view, is a write to a catalog, and
// the pass's transaction may write to one database only.
const closedView = "closed"

// defaultEmitSQL publishes the closed rows as they are.
const defaultEmitSQL = "SELECT * FROM " + closedView

// quoteIdent double-quotes an identifier, doubling any quote inside it.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// closedBefore is the predicate for rows whose bucket ended at or before an
// instant: time_column + size <= instant.
func (d Declaration) closedBefore(instant time.Time) string {
	return fmt.Sprintf("%s + INTERVAL '%d' SECOND <= TIMESTAMPTZ '%s'",
		quoteIdent(d.TimeColumn), int64(d.Size/time.Second), core.UTCLiteral(instant))
}

// dueBetween is the predicate for buckets that close in this pass: ended
// after the closed watermark and at or before the asserted one. Before the
// first close everything at or before the assertion is due.
func (d Declaration) dueBetween(closed time.Time, hadClosed bool, asserted time.Time) string {
	if !hadClosed {
		return d.closedBefore(asserted)
	}
	return fmt.Sprintf("%s + INTERVAL '%d' SECOND > TIMESTAMPTZ '%s' AND %s",
		quoteIdent(d.TimeColumn), int64(d.Size/time.Second), core.UTCLiteral(closed), d.closedBefore(asserted))
}

// bucketIs is the predicate for one bucket's rows, for a recompute.
func (d Declaration) bucketIs(bucket time.Time) string {
	return fmt.Sprintf("%s = TIMESTAMPTZ '%s'", quoteIdent(d.TimeColumn), core.UTCLiteral(bucket))
}

// expiredBefore is the predicate for buckets past their lateness: ended at
// or before asserted − lateness. With no lateness that is every bucket the
// pass publishes, so the close deletes what it published, as it always did.
func (d Declaration) expiredBefore(asserted time.Time) string {
	return d.closedBefore(asserted.Add(-d.Lateness))
}

// retainedBetween is the predicate for buckets that closed in an earlier
// pass and whose lateness has not run out: ended after asserted − lateness
// and at or before the closed watermark. The start pass republishes them,
// because the recompute set a late row landed in before a restart was in
// memory.
func (d Declaration) retainedBetween(closed, asserted time.Time) string {
	return fmt.Sprintf("%s + INTERVAL '%d' SECOND > TIMESTAMPTZ '%s' AND %s",
		quoteIdent(d.TimeColumn), int64(d.Size/time.Second), core.UTCLiteral(asserted.Add(-d.Lateness)), d.closedBefore(closed))
}

// oldestSQL reads the oldest bucket the window holds, as microseconds since
// the epoch. NULL when the table is empty.
func (d Declaration) oldestSQL() string {
	return fmt.Sprintf("SELECT epoch_us(min(%s)) FROM %s", quoteIdent(d.TimeColumn), quoteIdent(d.Table))
}

// newestSQL reads the newest bucket start the table holds, as microseconds
// since the epoch. NULL on an empty table.
func (d Declaration) newestSQL() string {
	return fmt.Sprintf("SELECT epoch_us(max(%s)) FROM %s", quoteIdent(d.TimeColumn), quoteIdent(d.Table))
}

// countSQL counts the rows where selects.
func (d Declaration) countSQL(where string) string {
	return fmt.Sprintf("SELECT count(*)::BIGINT FROM %s WHERE %s", quoteIdent(d.Table), where)
}

// deleteSQL removes the rows where selects.
func (d Declaration) deleteSQL(where string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d.Table), where)
}

// collectSQL is what the sink receives: emit_sql with the closed relation
// spliced in front of it as a CTE, over the rows where selects. An emit_sql
// that starts with its own WITH keeps it; the closed CTE is added to its
// list.
func (d Declaration) collectSQL(where string) string {
	closed := fmt.Sprintf("%s AS (SELECT * FROM %s WHERE %s)", closedView, quoteIdent(d.Table), where)
	emit := strings.TrimSpace(d.EmitSQL)
	if emit == "" {
		emit = defaultEmitSQL
	}
	if len(emit) >= 5 && strings.EqualFold(emit[:4], "WITH") && isSpace(emit[4]) {
		return "WITH " + closed + ", " + strings.TrimSpace(emit[5:])
	}
	return "WITH " + closed + " " + emit
}

func isSpace(b byte) bool { return b == ' ' || b == '\n' || b == '\t' || b == '\r' }
