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
//   - A sqlcommand sink that upserts into an attached Postgres: ON CONFLICT
//     or INSERT OR REPLACE with an INTO naming a Postgres attachment. The
//     DuckDB postgres extension runs that upsert by copying every row's key
//     from the target into DuckDB, so a flush costs the table, not the
//     batch. A warning that names the postgres sink. An upsert into a DuckDB
//     table sends Postgres nothing and is not warned. The check is textual:
//     a USE that makes the attachment the default database hides the target.
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
	attached := postgresAttachments(conf)

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

			if table.Window.ReemitOverwrites() {
				fail(fmt.Sprintf("tables.sql[%d] window: %s", i, config.ReemitOverwritesMessage),
					position(mappingKey(node, "late_rows")))
			}
		}
	}

	rep.SetCheck("sinks.postgres", status, "")
}

// checkSink holds one sink block to the rules that need no window.
func checkSink(where string, s config.Sink, node *yaml.Node, attached attachments, fail, warn func(string, *Position)) {
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
		if s.SQLCommand != nil && attached.upsertsInto(s.SQLCommand.SQL) {
			warn(where+": an upsert through the DuckDB postgres extension copies every row's key "+
				"from the whole target table into DuckDB on every flush, so a flush costs the table, not "+
				"the batch. The postgres sink does the same write at the cost of the batch: "+
				"type: postgres with table, mode: upsert and key", position(mappingValue(node, "sqlcommand")))
		}
	}
}

var (
	upsert = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b|\bINSERT\s+OR\s+REPLACE\b`)
	// attachStatement is one ATTACH, up to the semicolon that ends it.
	attachStatement = regexp.MustCompile(`(?is)\bATTACH\b[^;]*`)
	postgresType    = regexp.MustCompile(`(?i)\bTYPE\s+POSTGRES\b`)
	// attachAlias is the AS after the quoted path: ATTACH 'dsn' AS pg (...).
	attachAlias = regexp.MustCompile(`(?is)'[^']*'\s+AS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?`)
)

// attachments are the Postgres databases a config's commands attach.
type attachments struct {
	aliases []string
	// unnamed is an ATTACH with no AS, whose database takes its name from
	// the connection string. validate does not resolve that name, so any
	// upsert could be aimed at it.
	unnamed bool
}

func postgresAttachments(conf config.Conf) attachments {
	var a attachments
	for _, c := range conf.Commands {
		for _, stmt := range attachStatement.FindAllString(c.SQL, -1) {
			if !postgresType.MatchString(stmt) {
				continue
			}
			if m := attachAlias.FindStringSubmatch(stmt); m != nil {
				a.aliases = append(a.aliases, m[1])
			} else {
				a.unnamed = true
			}
		}
	}
	return a
}

// upsertsInto reports whether sql upserts into one of the attachments.
func (a attachments) upsertsInto(sql string) bool {
	if !upsert.MatchString(sql) {
		return false
	}
	if a.unnamed {
		return true
	}
	for _, alias := range a.aliases {
		into := regexp.MustCompile(`(?i)\bINTO\s+"?` + regexp.QuoteMeta(alias) + `"?\s*\.`)
		if into.MatchString(sql) {
			return true
		}
	}
	return false
}
