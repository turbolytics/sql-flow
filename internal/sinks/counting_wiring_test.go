package sinks

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// TestNewWrapsWithRowCounters pins the decorator's position: outermost, so one
// logical flush is one counted flush whatever the retry ladder does
// underneath. countingBuffered is unexported in internal/core, so this asserts
// the observable consequences rather than the concrete type.
func TestNewWrapsWithRowCounters(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, config.Sink{Type: "console"}, nil,
		WithSinkRole("pipeline"))
	assert.NoError(t, err)

	// New never returns the bare sink once the decorator is wired.
	_, bare := s.(*ConsoleSink)
	assert.That(t, !bare)

	// And the decorator forwards the console sink's depth report rather than
	// hiding it, which is the trap the retry wrapper fell into.
	_, reports := s.(core.BufferedRowReporter)
	assert.That(t, reports)
}

func TestWithSinkRoleSetsTheRole(t *testing.T) {
	var o options
	WithSinkRole("dlq")(&o)
	assert.Equal(t, "dlq", o.role)

	// Unset is empty here; New substitutes "pipeline".
	var d options
	assert.Equal(t, "", d.role)
}
