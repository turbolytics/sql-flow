package validate

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

const schemaRegistryCheck = "config.schema_registry"

// checkSchemaRegistry holds the schema_registry block and every value block
// to config.CheckSchemaRegistry, and anchors each finding to the line of the
// key it names. run applies the same rules and stops at the first; here the
// reader wants the whole list.
//
// A finding at a path the schema check already reported is dropped. The
// schema refuses an unknown format and a malformed version; the rule checks
// them too because run has no schema pass, and saying it twice helps nobody.
func checkSchemaRegistry(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck(schemaRegistryCheck, StatusSkipped, "the config did not parse, so there was no schema registry block to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		rep.SetCheck(schemaRegistryCheck, StatusSkipped, "the config did not decode, so there was no schema registry block to check")
		return
	}

	reported := map[string]bool{}
	for _, d := range rep.Diagnostics {
		if d.Context != "" {
			reported[d.Context] = true
		}
	}

	status := StatusPass
	for _, v := range conf.CheckSchemaRegistry() {
		context := "/" + strings.Join(v.Path, "/")
		if reported[context] {
			continue
		}
		status = StatusFail
		var pos *Position
		if line, col, ok := lineOf(&root, v.Path); ok {
			pos = &Position{Source: "config", Line: line, Column: col}
		}
		d := diagnostic(v.Code, SeverityError, v.Key()+": "+v.Message, pos)
		d.Context = context
		rep.Add(d)
	}
	rep.SetCheck(schemaRegistryCheck, status, "")
}
