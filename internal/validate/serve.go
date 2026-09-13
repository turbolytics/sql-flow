package validate

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

// checkServeRules reports every serve rule the schema cannot express, each
// at the line of the key it names.
//
// It runs nothing. The rules read names, params and placeholders; whether the
// SQL prepares against the attached backend is for `serve` to find at start.
func checkServeRules(rendered []byte, rep *Report) {
	conf, err := config.ParseServe(rendered)
	if err != nil {
		// The schema check has already said why. A config that does not
		// decode has no datasets to hold to the rules, and saying they passed
		// would be a lie.
		rep.SetCheck("serve.rules", StatusSkipped,
			"the config did not decode, so the serve rules did not run")
		return
	}

	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("serve.rules", StatusSkipped,
			"the config did not parse, so the serve rules did not run")
		return
	}

	// One fault, one diagnostic. A param type outside the enum breaks the
	// schema and a rule at the same key, and a reader fixing it needs one line.
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
		rep.SetCheck("serve.rules", StatusFail, "")
		return
	}
	rep.SetCheck("serve.rules", StatusPass, "")
}
