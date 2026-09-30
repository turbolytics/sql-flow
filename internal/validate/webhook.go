package validate

import (
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkWebhookAck runs the rule run applies to a webhook source before it
// binds: ack after_flush is refused on a pipeline that windows.
func checkWebhookAck(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("source.webhook.ack", StatusSkipped, "the config did not parse, so there was no source to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		rep.SetCheck("source.webhook.ack", StatusSkipped, "the config did not decode, so there was no source to check")
		return
	}
	if conf.Pipeline.Source.Type != "webhook" {
		rep.SetCheck("source.webhook.ack", StatusSkipped, "this pipeline has no webhook source")
		return
	}
	if err := conf.CheckWebhookAck(); err != nil {
		rep.Add(diagnostic(errs.CodeOf(err), SeverityError, err.Error(), position(sourceNode(&root))))
		rep.SetCheck("source.webhook.ack", StatusFail, "")
		return
	}
	rep.SetCheck("source.webhook.ack", StatusPass, "")
}
