package validate

import (
	"fmt"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkDrainDeadline warns when the sinks' retry ladders can outlive the
// drain.
//
// A short drain deadline is a legitimate choice: the operator wants the
// process out in five seconds whatever the sink is doing. So this is a
// warning and the check passes. What the operator may not have priced is that
// a drain which reaches a retrying sink exits 15 before the ladder finishes,
// and the tail of the stream replays on the next start.
//
// Every sink the pipeline builds is counted, and their ladders are summed:
// the pipeline's own, the dead-letter queue's, and each table manager's all
// spend the one drain budget, and the shutdown runs the loop's flush and then
// each manager's final poll in turn.
func checkDrainDeadline(rendered []byte, rep *Report) {
	var conf config.Conf
	if err := yaml.Unmarshal(rendered, &conf); err != nil {
		// checkSchema has already reported why the YAML did not parse.
		rep.SetCheck("pipeline.drain_deadline", StatusSkipped,
			"the config did not parse, so there was no pipeline to check")
		return
	}

	drain := seconds(conf.Pipeline.DrainDeadlineSeconds, config.DefaultDrainDeadlineSeconds)

	type named struct {
		path string
		sink config.Sink
	}
	sinks := []named{{"pipeline.sink", conf.Pipeline.Sink}}
	if conf.Pipeline.OnError != nil && conf.Pipeline.OnError.DLQ != nil {
		sinks = append(sinks, named{"pipeline.on_error.dlq", *conf.Pipeline.OnError.DLQ})
	}
	if conf.Tables != nil {
		for _, table := range conf.Tables.SQL {
			if table.Manager != nil {
				sinks = append(sinks, named{
					fmt.Sprintf("tables.sql[%s].manager.sink", table.Name),
					table.Manager.Sink,
				})
			}
		}
	}

	var (
		total   time.Duration
		ladders []string
	)
	for _, s := range sinks {
		ladder, ok := ladderDeadline(s.sink)
		if !ok {
			continue
		}
		total += ladder
		ladders = append(ladders, fmt.Sprintf("%s retry.deadline_seconds is %s", s.path, ladder))
	}
	if total > drain {
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
			"pipeline.drain_deadline_seconds is %s and the retry ladders it has to cover "+
				"add up to %s (%s), so a drain that reaches a retrying sink exits 15 "+
				"before the ladders finish",
			drain, total, strings.Join(ladders, ", ")), nil))
	}
	rep.SetCheck("pipeline.drain_deadline", StatusPass, "")
}

// ladderDeadline is the retry deadline a sink runs under, and false when the
// sink gets no ladder: its type is not wrapped, or max_attempts turns
// retrying off.
func ladderDeadline(s config.Sink) (time.Duration, bool) {
	if !config.SinkRetries(s.Type) {
		return 0, false
	}
	attempts, deadline := config.DefaultSinkRetryMaxAttempts, 0
	if s.Retry != nil {
		if s.Retry.MaxAttempts > 0 {
			attempts = s.Retry.MaxAttempts
		}
		deadline = s.Retry.DeadlineSeconds
	}
	if attempts <= 1 {
		return 0, false
	}
	return seconds(deadline, config.DefaultSinkRetryDeadlineSeconds), true
}

// seconds resolves a configured value the way the engine does: absent, zero
// and negative mean the default.
func seconds(configured, fallback int) time.Duration {
	if configured > 0 {
		return time.Duration(configured) * time.Second
	}
	return time.Duration(fallback) * time.Second
}
