package validate

import (
	"context"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Validate checks a config offline and reports everything it finds.
//
// It returns an error only when the request itself is unusable. A config that
// is wrong is a Report with diagnostics, not an error: the caller wants the
// whole list, and an agent wants it in one turn.
func Validate(ctx context.Context, req Request) (Report, error) {
	rep := Report{
		Config:      req.Path,
		Checks:      []Check{},
		Diagnostics: []Diagnostic{},
	}

	provided := config.TemplateVars(req.Vars)

	// Template first. It names variables at their real source positions, and
	// rendering has to succeed before anything else has text to inspect.
	checkTemplate(req.Config, provided, &rep)

	rendered, err := config.RenderTemplateString([]byte(req.Config), req.Vars)
	if err != nil {
		// The template check already reported why. Saying the schema passed
		// here would be a lie: it never ran.
		rep.SetCheck("config.schema", StatusSkipped,
			"the template did not render, so there was no config to check")
		rep.Finish()
		return rep, nil
	}

	checkSchema(rendered, &rep)
	checkDrainDeadline(rendered, &rep)

	demoteUnsuppliedVariableErrors(&rep)

	rep.Finish()
	return rep, nil
}

// demoteUnsuppliedVariableErrors turns a schema error caused by an unsupplied
// template variable into a warning.
//
// A variable with no default is a declared required input; checkTemplate says
// so and warns rather than fails, because the environment being incomplete is
// not the config being wrong (#142). The schema check then saw the
// consequence, an empty substitution, and failed on it:
//
//	dsn: {{ SQLFLOW_CLICKHOUSE_DSN }}
//	  error   at '/pipeline/sink/clickhouse/dsn': got null, want string
//	  warning template variable SQLFLOW_CLICKHOUSE_DSN is not set
//
// The two contradicted each other, and only the error reached the reader,
// because the CLI prints errors alone. So `config validate` could not be run
// on a clean shell, which is exactly what #142 asked for, and a connection
// string had to carry a fake default to pass. Nobody should put a bunk DSN in
// a production pipeline to satisfy a linter.
//
// Attribution is by source line: both diagnostics point at the line the
// reference sits on. That is deliberately the stupid version. It does not try
// to judge whether the empty value would produce broken SQL, which is a
// question for the SQL linter, not for a structural check.
func demoteUnsuppliedVariableErrors(rep *Report) {
	lines := map[int]bool{}
	for _, d := range rep.Diagnostics {
		if d.Code == string(errs.CodeConfigTemplateUndefined) &&
			d.Severity == SeverityWarning && d.Position != nil {
			lines[d.Position.Line] = true
		}
	}
	if len(lines) == 0 {
		return
	}

	for i, d := range rep.Diagnostics {
		if d.Severity != SeverityError || d.Position == nil || !lines[d.Position.Line] {
			continue
		}
		rep.Diagnostics[i].Severity = SeverityWarning
		rep.Diagnostics[i].Action = "set the variable named in the warning on this line, " +
			"or give it a default in the config"
	}
}
