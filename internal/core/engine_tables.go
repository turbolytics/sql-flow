package core

import "time"

// WindowsTable holds each window's watermark. It is declared here beside the
// other engine tables so the one list of them stays in one place; the
// managers package owns its schema and its rows.
const WindowsTable = "sqlflow_windows"

// EngineTables is every table the engine keeps for itself in the state
// database. The stats endpoint leaves them out of the user's tables, and a
// window declaration may not name one.
var EngineTables = []string{offsetsTable, progressTable, WindowsTable, WatermarksTable}

// IsEngineTable reports whether a name is one of EngineTables or the batch
// table a handler stages into.
func IsEngineTable(name string) bool {
	if name == batchTable {
		return true
	}
	for _, t := range EngineTables {
		if name == t {
			return true
		}
	}
	return false
}

// EscapeSQLString doubles single quotes, for a value written into a literal.
func EscapeSQLString(s string) string { return escapeSQLString(s) }

// UTCLiteral renders an instant with its offset, so a TIMESTAMPTZ literal
// carries a zone rather than borrowing the session's.
func UTCLiteral(t time.Time) string { return utcLiteral(t) }
