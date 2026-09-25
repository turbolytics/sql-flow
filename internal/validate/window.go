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
		// A window is cut on event time, and the watermark rests on the event
		// time the source assigned. Both have to be the same clock, or a
		// bucket cut from a payload field of the handler's choosing is closed
		// on evidence about a different stream. The handler exposes the
		// assigned time as event_time; a windowing pipeline's handler SQL
		// must read it, and a structured handler's batch table must declare
		// it, or there is nothing for the SQL to read.
		if hasWindow(conf) {
			h := conf.Pipeline.Handler
			if !mentionsEventTime(h.SQL) {
				fail("pipeline.handler: a windowing pipeline's handler sql must derive the "+
					"window's time from the event_time column, the time the source assigned "+
					"the record. A bucket cut from another field of the payload is on a "+
					"different clock from the watermark, and the one closes the other on "+
					"evidence about the wrong stream", position(handlerNode(&root)))
			}
			if isStructuredHandler(h.Type) {
				if ddl, ok := tableDDL(conf, h.Table); !ok || !declaresTimestamptz(ddl, "event_time") {
					fail(fmt.Sprintf("pipeline.handler: table %q must declare event_time TIMESTAMPTZ. "+
						"A structured handler's batch table is the user's, and the engine fills "+
						"that column with the time the source assigned each record; without it "+
						"there is nothing for a window to be cut on", h.Table),
						position(handlerNode(&root)))
				}
			}
		}
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
			// The idle close waits for the engine to commit past the bound,
			// and with nothing arriving those commits are one flush interval
			// apart. An idle_close shorter than the interval is honoured
			// late by up to the difference, which reads like a stalled close
			// to anyone timing it.
			flush := conf.Pipeline.FlushIntervalSeconds
			if flush <= 0 {
				flush = config.DefaultFlushIntervalSeconds
			}
			if w.IdleCloseSeconds > 0 && w.IdleCloseSeconds < flush {
				rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
					"tables.sql[%d] window: idle_close_seconds is %d and pipeline.flush_interval_seconds "+
						"is %d. The idle close waits for a commit made that long after the last "+
						"arrival, and a quiet stream commits once a flush interval, so buckets close "+
						"%d to %d seconds after the last arrival. Set flush_interval_seconds at or "+
						"below idle_close_seconds",
					i, w.IdleCloseSeconds, flush, w.IdleCloseSeconds, w.IdleCloseSeconds+flush),
					position(mappingKey(node, "idle_close_seconds"))))
			}
			// reemit publishes emit_sql over the late rows alone, for a bucket
			// the sink already holds. A sink that appends keeps both rows, and
			// its reader has to add them rather than keep the newest.
			if w.LateRows == "reemit" && appendsOnly(w.Sink) {
				rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
					"tables.sql[%d] window: late_rows is reemit and the %s sink appends, so a "+
						"late row publishes a second row for a bucket the sink already holds, "+
						"computed over the late rows alone. Its reader has to add the two. "+
						"Use drop unless it does",
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

// appendsOnly reports a sink that cannot replace a row it already holds.
// Iceberg appends by design, a Kafka topic is a log, and the postgres sink
// in append mode inserts every row. The sqlcommand and ClickHouse sinks
// depend on the SQL or the table engine, so they are the user's call.
func appendsOnly(s config.Sink) bool {
	switch s.Type {
	case "iceberg", "kafka":
		return true
	case "postgres":
		return s.Postgres != nil && s.Postgres.Mode == "append"
	}
	return false
}

func mentionsClosed(sql string) bool { return closedRef.MatchString(sql) }

var eventTimeRef = regexp.MustCompile(`(?i)\bevent_time\b`)

func mentionsEventTime(sql string) bool { return eventTimeRef.MatchString(sql) }

func hasWindow(conf config.Conf) bool {
	if conf.Tables == nil {
		return false
	}
	for _, t := range conf.Tables.SQL {
		if t.Window != nil {
			return true
		}
	}
	return false
}

// isStructuredHandler accepts both spellings the handler registry maps.
func isStructuredHandler(typ string) bool {
	return typ == "handlers.StructuredBatch" || typ == "structured"
}

// tableDDL is the CREATE the config declares for a named table.
func tableDDL(conf config.Conf, name string) (string, bool) {
	if conf.Tables == nil {
		return "", false
	}
	for _, t := range conf.Tables.SQL {
		if t.Name == name {
			return t.SQL, true
		}
	}
	return "", false
}

// handlerNode is the mapping node of pipeline.handler, for a diagnostic's
// position; nil, and so no position, if the document has no such node.
func handlerNode(root *yaml.Node) *yaml.Node {
	doc := root
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		doc = doc.Content[0]
	}
	pipeline := mappingValue(doc, "pipeline")
	if pipeline == nil {
		return nil
	}
	return mappingValue(pipeline, "handler")
}

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
