package validate

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

// checkRollupRules reports every rollup rule the schema cannot express, each
// at the line of the key it names. It generates nothing and connects to
// nothing.
func checkRollupRules(rendered []byte, rep *Report) {
	conf, err := config.ParseRollups(rendered)
	if err != nil {
		rep.SetCheck("rollup.rules", StatusSkipped,
			"the config did not decode, so the rollup rules did not run")
		return
	}

	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("rollup.rules", StatusSkipped,
			"the config did not parse, so the rollup rules did not run")
		return
	}

	// One fault, one diagnostic: a measure type outside the enum breaks the
	// schema and a rule at the same key.
	reported := map[string]bool{}
	for _, d := range rep.Diagnostics {
		if d.Severity == SeverityError && d.Context != "" {
			reported[d.Context] = true
		}
	}

	violations := conf.Check()
	for _, v := range violations {
		context := "/" + strings.Join(v.Path, "/")
		if reported[context] {
			continue
		}
		var pos *Position
		if line, col, ok := lineOf(&root, v.Path); ok {
			pos = &Position{Source: "config", Line: line, Column: col}
		}
		d := diagnostic(v.Code, SeverityError, v.Message, pos)
		d.Context = context
		rep.Add(d)
	}

	if len(violations) > 0 {
		rep.SetCheck("rollup.rules", StatusFail, "")
		return
	}
	rep.SetCheck("rollup.rules", StatusPass, "")
}
