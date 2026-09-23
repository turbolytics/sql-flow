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
	if conf.Pipeline.TurboStats == nil {
		rep.SetCheck("turbostats.rules", StatusSkipped,
			"this pipeline has no turbostats block")
		return
	}

	var root yaml.Node
	_ = yaml.Unmarshal(rendered, &root)

	// A block that reports nowhere is still checked. Its labels reach the
	// local endpoint, and a rule that would refuse the config the moment
	// someone sets report_to is a defect in it now.
	violations := conf.Pipeline.TurboStats.Check([]string{"pipeline", "turbostats"})
	for _, v := range violations {
		// Check was given the path to its own block, so v.Path is already
		// rooted at the document. Prefixing it again resolved nothing, and
		// lineOf falls back to the enclosing node, so every one of these
		// pointed at the pipeline's first line instead of the bad key.
		path := v.Path
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
