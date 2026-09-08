package validate

import (
	"context"

	"github.com/turbolytics/sql-flow/internal/config"
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

	rep.Finish()
	return rep, nil
}
