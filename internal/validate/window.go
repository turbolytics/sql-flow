package validate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkWindows checks every window declaration without running anything.
//
// The faults it catches before the pipeline starts:
//
//   - A `manager:` block, `late_rows`, or `poll_interval_seconds`: keys
//     whose meaning is gone. Each message names what replaces it, because
//     the schema check alone says "unknown key".
//   - A time column the table's CREATE does not declare as TIMESTAMPTZ. A
//     TIMESTAMP bucket is read in the session's zone, and the watermark
//     compares instants, so it closes windows early or never.
//   - A time column the handler's SQL does not compute as
//     time_bucket(INTERVAL '<size>', event_time). The engine decides a
//     record's lateness from that bucket, computed the same way from the
//     same clock, and a row bucketed any other way is not in the bucket the
//     engine decided against.
//   - allowed_lateness_seconds above zero with a sink that appends. A late
//     row republishes its bucket as a whole value, which an appending sink
//     then holds twice. A sink validate cannot classify is warned instead.
//   - An emit_sql that does not read `closed`, which is the only relation a
//     close supplies.
//   - A window table with an index and no state path. DuckDB never reclaims
//     rows deleted from an indexed in-memory table, and the engine deletes
//     every bucket it publishes, so memory grows without bound. #268's
//     fifth footgun, as a warning: the pipeline runs, and the operator is
//     told what it costs.
//
// The column checks are textual: validate links no DuckDB, so it reads the
// CREATE statement and the handler's SQL rather than parsing them.
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
				"allowed_lateness_seconds, emit_sql and sink. The engine closes it; "+
				"collect_closed_windows_sql and delete_closed_windows_sql are not written by the user", i),
				position(key))
		}
		win := mappingValue(table, "window")
		if key := mappingKey(win, "late_rows"); key != nil {
			fail(fmt.Sprintf("tables.sql[%d] window: late_rows is gone. Set allowed_lateness_seconds "+
				"instead: 0 refuses a row for a closed bucket before the handler, which is what drop did "+
				"after the fact; a positive value keeps a closed bucket that long and republishes it whole "+
				"when a late row arrives, which is what reemit tried to do without the delta. See "+
				"docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md", i), position(key))
		}
		if key := mappingKey(win, "poll_interval_seconds"); key != nil {
			fail(fmt.Sprintf("tables.sql[%d] window: poll_interval_seconds is gone. The engine closes a "+
				"window the moment its watermark passes a bucket's end; nothing polls. Remove the key. See "+
				"docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md", i), position(key))
		}
	}

	if conf.Tables != nil {
		// A window is cut on event time, and the watermark rests on the event
		// time the source assigned. Both have to be the same clock, or a
		// bucket cut from a payload field of the handler's choosing is closed
		// on evidence about a different stream. The handler exposes the
		// assigned time as event_time; a windowing pipeline's handler SQL
		// should read it, and a structured handler's batch table should
		// declare it, or there is nothing for the SQL to read.
		//
		// A warning, not a failure, for now. Only the websocket source can be
		// told where its event time is; Kafka assigns the record timestamp
		// and MQTT assigns arrival, and a pipeline that windows a Kafka topic
		// on a timestamp inside the payload -- every shipped example does --
		// would be wrong to comply. Once every source takes an event_time
		// block, complying makes the two clocks one by construction, and
		// this becomes an error.
		if hasWindow(conf) {
			// The per-window rule below requires time_column to be cut from
			// event_time. A structured handler can only do that if its batch
			// table declares the column, so that is refused here first, with
			// the message that says what to add. This was a warning until
			// every source could be told where its time is (#389) and the
			// engine decided lateness from that bucket (#396); it is an error
			// now, because a bucket on another clock is a bucket the engine
			// decides against wrongly.
			h := conf.Pipeline.Handler
			if isStructuredHandler(h.Type) {
				if ddl, ok := tableDDL(conf, h.Table); !ok || !declaresTimestamptz(ddl, "event_time") {
					fail(fmt.Sprintf("pipeline.handler: a windowing pipeline's table %q does not declare "+
						"event_time TIMESTAMPTZ, so the time the source assigned each record cannot "+
						"reach the handler's SQL. Declare it and the engine fills it from the record", h.Table),
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
			// The engine decides a record's lateness from trunc(event_time,
			// size); the handler must put the row in that bucket and no
			// other. This was a warning while the engine only swept the
			// table for late rows and could tolerate the disagreement. It
			// cannot now.
			if !derivesTimeColumn(conf.Pipeline.Handler.SQL, w.TimeColumn, w.SizeSeconds) {
				fail(fmt.Sprintf("tables.sql[%d] window: time_column %q must be computed as "+
					"time_bucket(INTERVAL '%d seconds', event_time) in the handler's SQL. The engine "+
					"decides a record's lateness from that bucket, and a row bucketed any other way is "+
					"not in the bucket the engine decided against. Tell the source where the record's "+
					"time is with event_time: {path, format} if it is in the payload",
					i, w.TimeColumn, w.SizeSeconds), position(node))
			}
			// A late row within lateness republishes its bucket whole, which
			// a sink that appends then holds twice. Flink's contract is the
			// same: a downstream of a window with allowed lateness must
			// handle updates.
			if w.AllowedLatenessSecs > 0 {
				switch {
				case w.LatenessNeedsReplacingSink():
					fail(fmt.Sprintf("tables.sql[%d] window: %s", i, config.LatenessNeedsReplacingSinkMessage(*w)),
						position(mappingKey(node, "allowed_lateness_seconds")))
				case w.Sink.Type == "sqlcommand" || w.Sink.Type == "clickhouse":
					rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
						"tables.sql[%d] window: allowed_lateness_seconds is %d, so a late row republishes "+
							"its bucket as a whole value. The %s sink must replace the row for (bucket, key) "+
							"rather than add to it, which is its SQL's or its table engine's to guarantee",
						i, w.AllowedLatenessSecs, w.Sink.Type), position(mappingKey(node, "allowed_lateness_seconds"))))
				}
			}
		}
	}

	rep.SetCheck("tables.window", status, "")
}

// timeBucketOverEventTime matches `time_bucket(INTERVAL '<literal>', event_time) AS <col>`,
// the one form a windowing pipeline's time_column may take. The engine
// buckets a record with core.BucketStart, which is time_bucket with DuckDB's
// origin, so the handler must bucket the same way or lateness is decided
// against a bucket the row is not in.
var timeBucketOverEventTime = regexp.MustCompile(
	`(?is)time_bucket\s*\(\s*INTERVAL\s+'([^']+)'\s*,\s*event_time\s*\)\s+AS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?`)

// intervalSeconds reads DuckDB's quoted interval literal for the units a
// window size is written in. false for a form it does not read, which
// validate reports rather than guesses at.
func intervalSeconds(literal string) (int, bool) {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(literal)))
	if len(fields) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n <= 0 {
		return 0, false
	}
	per := map[string]int{"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}[strings.TrimSuffix(fields[1], "s")]
	if per == 0 {
		return 0, false
	}
	return n * per, true
}

// derivesTimeColumn reports whether handlerSQL computes column as
// time_bucket over event_time with a size of sizeSeconds.
func derivesTimeColumn(handlerSQL, column string, sizeSeconds int) bool {
	for _, m := range timeBucketOverEventTime.FindAllStringSubmatch(handlerSQL, -1) {
		if !strings.EqualFold(m[2], column) {
			continue
		}
		if secs, ok := intervalSeconds(m[1]); ok && secs == sizeSeconds {
			return true
		}
	}
	return false
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

// tableDDL is the CREATE the config declares for a named table: an entry
// under tables.sql, or a CREATE TABLE in a command, which is where the
// bluesky examples declare the structured handler's batch table.
func tableDDL(conf config.Conf, name string) (string, bool) {
	if conf.Tables != nil {
		for _, t := range conf.Tables.SQL {
			if t.Name == name {
				return t.SQL, true
			}
		}
	}
	for _, c := range conf.Commands {
		for _, stmt := range strings.Split(c.SQL, ";") {
			if m := createTable.FindStringSubmatch(stmt); m != nil && strings.EqualFold(strings.Trim(m[1], `"`), name) {
				return stmt, true
			}
		}
	}
	return "", false
}

// createTable matches the name a CREATE TABLE statement declares.
var createTable = regexp.MustCompile(`(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("?[A-Za-z_][A-Za-z0-9_]*"?)`)

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
