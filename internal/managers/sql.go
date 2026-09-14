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
// fixed and the reason the user's predicates were hard to get right.

// closedView is the relation emit_sql reads: the rows of every bucket that
// has just closed. It is spliced into emit_sql as a common table expression
// rather than created as a view, because a CREATE, even of a temporary
// view, is a write to a catalog, and the close's transaction may write to
// one database only.
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

// newestSQL reads the newest bucket start the table holds, as microseconds
// since the epoch. NULL on an empty table.
func (d Declaration) newestSQL() string {
	return fmt.Sprintf("SELECT epoch_us(max(%s)) FROM %s", quoteIdent(d.TimeColumn), quoteIdent(d.Table))
}

// lastArrivalSQL reads when the newest batch reached the handler, from the
// engine's progress row, as microseconds since the epoch. NULL before the
// first batch.
func lastArrivalSQL() string {
	return "SELECT epoch_us(last_arrival) FROM sqlflow_progress"
}

// countClosedSQL counts the rows a close at the instant would collect.
func (d Declaration) countClosedSQL(instant time.Time) string {
	return fmt.Sprintf("SELECT count(*)::BIGINT FROM %s WHERE %s", quoteIdent(d.Table), d.closedBefore(instant))
}

// deleteClosedSQL removes the rows a close at the instant collected.
func (d Declaration) deleteClosedSQL(instant time.Time) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d.Table), d.closedBefore(instant))
}

// collectSQL is what the sink receives: emit_sql with the closed relation
// spliced in front of it as a CTE. An emit_sql that starts with its own WITH
// keeps it; the closed CTE is added to its list.
func (d Declaration) collectSQL(instant time.Time) string {
	closed := fmt.Sprintf("%s AS (SELECT * FROM %s WHERE %s)",
		closedView, quoteIdent(d.Table), d.closedBefore(instant))
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
