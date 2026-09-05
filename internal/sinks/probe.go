package sinks

import (
	"context"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Prober is implemented by a sink that can check its destination before the
// first batch arrives.
//
// Without it a sink only discovers its destination when a batch reaches it.
// clickhouse.Open validates options and never dials, so a pipeline whose DSN
// names a host that does not resolve starts normally, logs "consumer loop
// starting", and runs. With a long flush interval the failure appears minutes
// later, and a supervisor calls the pipeline healthy for all of them.
type Prober interface {
	Probe(ctx context.Context) error
}

// probe checks a sink's destination if it can be checked, and classifies what
// comes back.
//
// It dials once and does not retry. An unreachable destination exits with a
// retryable code, so the supervisor's restart is already the retry; a ladder
// here would only delay the report. Nothing has been consumed yet, so there is
// nothing to lose by failing fast.
func probe(ctx context.Context, s core.Sink) error {
	p, ok := s.(Prober)
	if !ok {
		// Console, noop and sqlcommand reach nothing that can be absent.
		return nil
	}

	if err := ctx.Err(); err != nil {
		return errs.Wrap(errs.CodeSinkInternal, err, "sink: probe cancelled")
	}

	err := p.Probe(ctx)
	if err == nil {
		return nil
	}

	// A server that answered and refused is the user's to fix: a wrong
	// password or a missing database does not resolve on a restart. Reporting
	// it as unreachable would have a supervisor retry a pipeline that cannot
	// start.
	if !isUnreachable(err) {
		return errs.Wrap(errs.CodeSinkInvalid, err,
			"sink: the destination refused the connection at startup")
	}
	return errs.Wrap(errs.CodeSinkUnreachable, err,
		"sink: the destination could not be reached at startup")
}
