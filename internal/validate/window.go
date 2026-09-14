package validate

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkWindows checks every window declaration without running anything.
//
// Three faults it catches before the pipeline starts:
//
//   - A `manager:` block. The engine closes windows now, and the two
//     predicates the block carried are gone. The message names what
//     replaces them, because the schema check alone says "unknown key".
//   - A time column the table's CREATE does not declare as TIMESTAMPTZ. A
//     TIMESTAMP bucket is read in the session's zone, and the watermark
//     compares instants, so it closes windows early or never.
//   - An emit_sql that does not read `closed`, which is the only relation a
//     close supplies.
//   - A window table with an index and no state path. DuckDB never reclaims
//     rows deleted from an indexed in-memory table, and the engine deletes
//     every bucket it publishes, so memory grows without bound. #268's
//     fifth footgun, as a warning: the pipeline runs, and the operator is
//     told what it costs.
//
// The column check is textual: validate links no DuckDB, so it reads the
// CREATE statement rather than parsing it.
func checkWindows(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("tables.window", StatusSkipped,
			"the config did not parse, so there were no windows to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		// The schema check reports what is wrong with the shape; a block
		// the config type cannot hold is its finding, not this check's.
		rep.SetCheck("tables.window", StatusSkipped,
			"the config did not decode, so there were no windows to check")
		return
	}

	status := StatusPass
	fail := func(msg string, pos *Position) {
		status = StatusFail
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, msg, pos))
	}

	for i, table := range tableNodes(&root) {
		if key := mappingKey(table, "manager"); key != nil {
			fail(fmt.Sprintf("tables.sql[%d]: manager is gone. Declare the window instead: "+
				"window with time_column, size_seconds, grace_seconds, idle_close_seconds, "+
				"late_rows, emit_sql and sink. The engine closes it; collect_closed_windows_sql "+
				"and delete_closed_windows_sql are not written by the user", i),
				position(key))
		}
	}

	if conf.Tables != nil {
		for i, table := range conf.Tables.SQL {
			if table.Window == nil {
				continue
			}
			node := windowNode(&root, i)
			w := table.Window
			if w.TimeColumn != "" && !declaresTimestamptz(table.SQL, w.TimeColumn) {
				fail(fmt.Sprintf("tables.sql[%d] window: time_column %q is not declared as TIMESTAMPTZ "+
					"in the table's CREATE. The watermark compares instants, and a TIMESTAMP is "+
					"read in the session's zone", i, w.TimeColumn), position(node))
			}
			if strings.TrimSpace(w.EmitSQL) != "" && !mentionsClosed(w.EmitSQL) {
				fail(fmt.Sprintf("tables.sql[%d] window: emit_sql does not read closed, the "+
					"relation holding the rows of every bucket that just closed", i),
					position(mappingKey(node, "emit_sql")))
			}
			if declaresIndex(table.SQL) && (conf.Pipeline.State == nil || conf.Pipeline.State.Path == "") {
				rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
					"tables.sql[%d] window: the table has an index and the pipeline has no "+
						"state.path. DuckDB never reclaims rows deleted from an indexed in-memory "+
						"table, and every closed bucket is deleted, so memory grows without bound. "+
						"Drop the index and sum the appended rows in emit_sql, or set "+
						"pipeline.state.path", i), position(node)))
			}
			// reemit publishes a bucket the sink already holds. A sink that
			// upserts replaces it; a sink that appends keeps both rows, and
			// its reader cannot tell which is current.
			if w.LateRows == "reemit" && appendsOnly(w.Sink.Type) {
				rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
					"tables.sql[%d] window: late_rows is reemit and the %s sink appends, so a "+
						"late row publishes a second row for a bucket the sink already holds. "+
						"Use drop, or a sink that upserts on the bucket's key",
					i, w.Sink.Type), position(mappingKey(node, "late_rows"))))
			}
		}
	}

	rep.SetCheck("tables.window", status, "")
}

// declaresTimestamptz reports whether a CREATE statement declares column as
// TIMESTAMPTZ or TIMESTAMP WITH TIME ZONE.
func declaresTimestamptz(createSQL, column string) bool {
	pattern := `(?is)(?:^|[\s(,])"?` + regexp.QuoteMeta(column) +
		`"?\s+(?:TIMESTAMPTZ|TIMESTAMP\s+WITH\s+TIME\s+ZONE)\b`
	return regexp.MustCompile(pattern).MatchString(createSQL)
}

var closedRef = regexp.MustCompile(`(?i)\bclosed\b`)

// indexDecl matches a CREATE INDEX, a UNIQUE INDEX, or a PRIMARY KEY in a
// table's DDL.
var indexDecl = regexp.MustCompile(`(?is)\b(?:CREATE\s+(?:UNIQUE\s+)?INDEX|PRIMARY\s+KEY|UNIQUE\s*\()`)

func declaresIndex(createSQL string) bool { return indexDecl.MatchString(createSQL) }

// appendsOnly reports a sink type that cannot replace a row it already
// holds. Iceberg appends by design, and a Kafka topic is a log. The
// sqlcommand and ClickHouse sinks depend on the SQL or the table engine, so
// they are the user's call.
func appendsOnly(sinkType string) bool {
	return sinkType == "iceberg" || sinkType == "kafka"
}

func mentionsClosed(sql string) bool { return closedRef.MatchString(sql) }

// tableNodes returns the mapping node of each entry under tables.sql.
func tableNodes(root *yaml.Node) []*yaml.Node {
	doc := root
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		doc = doc.Content[0]
	}
	tables := mappingValue(doc, "tables")
	if tables == nil {
		return nil
	}
	list := mappingValue(tables, "sql")
	if list == nil || list.Kind != yaml.SequenceNode {
		return nil
	}
	return list.Content
}

func windowNode(root *yaml.Node, i int) *yaml.Node {
	tables := tableNodes(root)
	if i >= len(tables) {
		return nil
	}
	return mappingValue(tables[i], "window")
}

// mappingKey returns the key node for name in a mapping, or nil.
func mappingKey(m *yaml.Node, name string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == name {
			return m.Content[i]
		}
	}
	return nil
}

// mappingValue returns the value node for name in a mapping, or nil.
func mappingValue(m *yaml.Node, name string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == name {
			return m.Content[i+1]
		}
	}
	return nil
}

func position(n *yaml.Node) *Position {
	if n == nil {
		return nil
	}
	return &Position{Line: n.Line, Column: n.Column}
}
