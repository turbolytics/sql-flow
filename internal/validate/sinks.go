package validate

import (
	"fmt"
	"regexp"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkSinks checks every sink block, the pipeline's and each window's,
// without running anything.
//
//   - A postgres upsert without a key, or an append with one. The schema
//     cannot say that key depends on mode; the sink refuses both at start,
//     and validate says so first. A missing mode is the schema's finding.
//   - A window whose postgres sink upserts and whose late_rows is reemit.
//     The sink replaces a bucket's row with what it is handed, and a reemit
//     hands it emit_sql over the late rows alone. Refused.
//   - A sqlcommand sink whose SQL carries ON CONFLICT while a command
//     attaches a Postgres. The DuckDB postgres extension runs that upsert by
//     copying every row's key from the target into DuckDB, so a flush costs
//     the table, not the batch. A warning that names the postgres sink.
func checkSinks(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("sinks.postgres", StatusSkipped, "the config did not parse, so there were no sinks to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		rep.SetCheck("sinks.postgres", StatusSkipped, "the config did not decode, so there were no sinks to check")
		return
	}

	status := StatusPass
	fail := func(msg string, pos *Position) {
		status = StatusFail
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, msg, pos))
	}
	warn := func(msg string, pos *Position) {
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, msg, pos))
	}
	attached := attachesPostgres(conf)

	doc := &root
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		doc = doc.Content[0]
	}
	checkSink("pipeline.sink", conf.Pipeline.Sink, mappingValue(mappingValue(doc, "pipeline"), "sink"), attached, fail, warn)

	if conf.Tables != nil {
		for i, table := range conf.Tables.SQL {
			if table.Window == nil {
				continue
			}
			node := windowNode(&root, i)
			checkSink(fmt.Sprintf("tables.sql[%d] window sink", i), table.Window.Sink, mappingValue(node, "sink"), attached, fail, warn)

			s := table.Window.Sink
			if s.Type == "postgres" && s.Postgres != nil && s.Postgres.Mode == "upsert" && table.Window.LateRows == "reemit" {
				fail(fmt.Sprintf("tables.sql[%d] window: late_rows is reemit and the postgres sink upserts. "+
					"A reemit publishes emit_sql over the late rows alone, and the sink replaces the "+
					"bucket's row with that. Use drop", i), position(mappingKey(node, "late_rows")))
			}
		}
	}

	rep.SetCheck("sinks.postgres", status, "")
}

// checkSink holds one sink block to the rules that need no window.
func checkSink(where string, s config.Sink, node *yaml.Node, attached bool, fail, warn func(string, *Position)) {
	switch s.Type {
	case "postgres":
		if s.Postgres == nil {
			fail(where+": type postgres needs a postgres block with dsn, table and mode", position(node))
			return
		}
		pos := position(mappingValue(node, "postgres"))
		switch s.Postgres.Mode {
		case "upsert":
			if len(s.Postgres.Key) == 0 {
				fail(where+": postgres mode upsert needs key, the columns a row is identified by", pos)
			}
		case "append":
			if len(s.Postgres.Key) > 0 {
				fail(where+": postgres mode append takes no key; every row is inserted as it is", pos)
			}
		}
	case "sqlcommand":
		if attached && s.SQLCommand != nil && onConflict.MatchString(s.SQLCommand.SQL) {
			warn(where+": ON CONFLICT through the DuckDB postgres extension copies every row's key "+
				"from the whole target table into DuckDB on every flush, so a flush costs the table, not "+
				"the batch. The postgres sink does the same write at the cost of the batch: "+
				"type: postgres with table, mode: upsert and key", position(mappingValue(node, "sqlcommand")))
		}
	}
}

var (
	onConflict     = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)
	attachPostgres = regexp.MustCompile(`(?is)\bATTACH\b.*\bTYPE\s+POSTGRES\b`)
)

// attachesPostgres reports whether any command attaches a Postgres.
func attachesPostgres(conf config.Conf) bool {
	for _, c := range conf.Commands {
		if attachPostgres.MatchString(c.SQL) {
			return true
		}
	}
	return false
}
