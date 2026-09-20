package validate

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

// checkTurboStats validates a pipeline's reporter block.
//
// It runs offline, like everything else here: it reaches no control plane and
// posts nothing. What it catches is the config that would let a pipeline
// start and then report nowhere -- a key that cannot sign, a destination that
// is not a URL -- which an operator would otherwise discover from a fleet
// page that stays empty.
func checkTurboStats(rendered []byte, rep *Report) {
	var conf config.Conf
	if err := yaml.Unmarshal(rendered, &conf); err != nil {
		// The schema check already reported why.
		rep.SetCheck("turbostats.rules", StatusSkipped,
			"the config did not parse, so there was no block to check")
		return
	}
	if !conf.Pipeline.TurboStats.Enabled() {
		rep.SetCheck("turbostats.rules", StatusSkipped,
			"this pipeline reports to no control plane")
		return
	}

	var root yaml.Node
	_ = yaml.Unmarshal(rendered, &root)

	violations := conf.Pipeline.TurboStats.Check()
	for _, v := range violations {
		// The block hangs off pipeline, and Check names its own keys.
		path := append([]string{"pipeline"}, v.Path...)
		var pos *Position
		if line, col, ok := lineOf(&root, path); ok {
			pos = &Position{Source: "config", Line: line, Column: col}
		}
		d := diagnostic(v.Code, SeverityError, v.Message, pos)
		d.Context = "/" + strings.Join(path, "/")
		rep.Add(d)
	}

	if len(violations) > 0 {
		rep.SetCheck("turbostats.rules", StatusFail, "")
		return
	}
	rep.SetCheck("turbostats.rules", StatusPass, "")
}
