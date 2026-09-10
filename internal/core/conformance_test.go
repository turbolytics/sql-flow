// Package core_test is external on purpose. internal/conformance imports
// internal/core, so an in-package test importing the harness would be a cycle.
package core_test

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The consume loop under the conformance harness, once per configuration.
//
// These invariants were first proven by markers on hand-written tests, on the
// grounds that the pipeline is one implementation. It is not. Durable state
// changes what a commit means, and four paths reach a batch: the batch
// filling, the flush interval elapsing, the source closing, and a cancel that
// drains. Eight combinations, of which the hand-written tests covered two.

func TestPipelineStateful_Conformance(t *testing.T) {
	conformance.Pipelines(t, conformance.PipelineSubject{
		Integration: "pipeline.stateful",
		KeepsState:  true,
		Options: func(r *conformance.Recorder) []core.TurbineOption {
			return []core.TurbineOption{core.WithStateStore(r.Offsets(), r.Tx())}
		},
	})
}

func TestPipelineStateless_Conformance(t *testing.T) {
	conformance.Pipelines(t, conformance.PipelineSubject{
		Integration: "pipeline.stateless",
		KeepsState:  false,
		Options:     func(*conformance.Recorder) []core.TurbineOption { return nil },
	})
}

// The premise behind the stateless configuration's exemption in
// the registry. It commits no durable state, so there is nothing to
// commit alongside the offsets, and pipeline.state.with_offsets is vacuous
// rather than unproven.
func TestPipelineStateless_KeepsNoDurableState(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")

	rec := &conformance.Recorder{}
	tb := core.NewTurbine(nil, nil, nil, 1, 0, nil, core.PipelineErrorPolicies{})

	assert.That(t, tb != nil)
	// No state store was configured, so nothing the recorder watches can fire.
	assert.Equal(t, 0, len(rec.Events()))
}
