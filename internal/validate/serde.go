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
// Three rules are the schema's too: a missing or empty url, an unknown
// format, a malformed version. The rule states them because run has no
// schema pass. Here such a finding is printed once: when the schema check
// already reported it, at the key or at the block a required key is missing
// from, the rule's copy is dropped. No other finding is ever dropped, so a
// schema error at a path never hides a rule the schema cannot state.
//
// The check fails whenever a rule is broken, printed or not. A per-check
// table that showed this check green on format: protobuf would be wrong.
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
		status = StatusFail
		context := "/" + strings.Join(v.Path, "/")
		parent := "/" + strings.Join(v.Path[:len(v.Path)-1], "/")
		if v.InSchema && (reported[context] || reported[parent]) {
			continue
		}
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
