package validate

import (
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkMqtt runs the rules run applies to an mqtt source before it dials:
// a fixed client_id, and a batch no larger than receive_maximum.
func checkMqtt(rendered []byte, rep *Report) {
	var conf config.Conf
	if err := yaml.Unmarshal(rendered, &conf); err != nil {
		rep.SetCheck("source.mqtt", StatusSkipped, "the config did not parse, so there was no source to check")
		return
	}
	if conf.Pipeline.Source.Type != "mqtt" {
		rep.SetCheck("source.mqtt", StatusSkipped, "this pipeline has no mqtt source")
		return
	}
	if err := conf.Pipeline.CheckMQTT(); err != nil {
		var root yaml.Node
		_ = yaml.Unmarshal(rendered, &root)
		var pos *Position
		if line, col, ok := lineOf(&root, []string{"pipeline", "source", "mqtt"}); ok {
			pos = &Position{Source: "config", Line: line, Column: col}
		}
		rep.Add(diagnostic(errs.CodeOf(err), SeverityError, err.Error(), pos))
		rep.SetCheck("source.mqtt", StatusFail, "")
		return
	}
	rep.SetCheck("source.mqtt", StatusPass, "")
}
