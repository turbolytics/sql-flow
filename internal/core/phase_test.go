package core

import (
	"context"
	"sort"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// phase_duration answers where batch time goes. error_count already labels
// every failure with a phase; until now the same phases carried no duration,
// so an operator could see which stage was failing and not which was slow.

// phaseCounts returns how many observations phase_duration recorded under
// each phase.
func phaseCounts(t *testing.T, r *sdkmetric.ManualReader) map[string]uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))

	counts := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "phase_duration" {
				continue
			}
			data, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range data.DataPoints {
				v, ok := dp.Attributes.Value("phase")
				if !ok {
					continue
				}
				counts[v.AsString()] += dp.Count
			}
		}
	}
	return counts
}

// phasesSeen returns the phases phase_duration recorded, sorted.
func phasesSeen(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var out []string
	for phase := range phaseCounts(t, r) {
		out = append(out, phase)
	}
	sort.Strings(out)
	return out
}

// histogramCount returns the total observations one dimensionless histogram
// recorded.
func histogramCount(t *testing.T, r *sdkmetric.ManualReader, name string) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))

	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if data, ok := m.Data.(metricdata.Histogram[float64]); ok {
				for _, dp := range data.DataPoints {
					total += dp.Count
				}
			}
		}
	}
	return total
}

// recordPhase looks its attributes up by phase name. A phase with no cached
// entry records a point with no phase label, which merges into a series that
// is true of nothing -- silently, with every test above still passing, because
// the observation is made either way.
//
// What breaks if this is wrong: a new phase lands in the taxonomy, goes into
// an unlabelled series, and an operator reads a batch-time decomposition whose
// parts no longer add up.
func TestObservabilityMetrics_EveryPhaseHasCachedAttributes(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	tb := newTestTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1)

	for _, phase := range []string{
		phaseHandlerWrite, phaseHandlerInvoke, phaseHandlerInit,
		phaseSinkWrite, phaseSinkFlush, phaseStateCommit,
	} {
		if _, ok := tb.phaseAttrs[phase]; !ok {
			t.Errorf("phase %q has no cached attribute set", phase)
		}
	}
}

// A batch that runs every phase times every phase. The assertion is the whole
// set rather than a spot check: a phase added to the taxonomy and not to the
// timing is the exact asymmetry this instrument exists to remove.
func TestObservabilityMetrics_ABatchTimesEveryPhaseItRan(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	want := []string{
		phaseHandlerInit, phaseHandlerInvoke, phaseHandlerWrite,
		phaseSinkFlush, phaseSinkWrite, phaseStateCommit,
	}
	sort.Strings(want)
	assert.DeepEqual(t, want, phasesSeen(t, reader))
}

// A phase that fails still spent the time. sink_flush_latency records after
// both error returns in processBatch, so a sink that takes thirty seconds to
// fail leaves it flat -- the operator sees a rising error_count and cannot
// tell a fast failure from a slow one. phase_duration counts both.
//
// What breaks if this is wrong: a slow-failing sink is indistinguishable from
// a fast-failing one, and the scale-out guardrail reads a batch-time
// decomposition that omits the phase burning the time.
func TestObservabilityMetrics_AFailedPhaseStillRecordsItsDuration(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &flushFailingSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)

	assert.Equal(t, uint64(1), phaseCounts(t, reader)[phaseSinkFlush])
	// The contrast that makes the new instrument worth having.
	assert.Equal(t, uint64(0), histogramCount(t, reader, "sink_flush_latency"))
}

// handler.write is the one phase on the per-message path, which runs at
// benchmarked rates near 900k msgs/sec. It is timed per source batch: the
// observation is the time that batch's messages took to reach the handler, so
// the hot path pays two clock reads per message and none per observation.
func TestObservabilityMetrics_HandlerWriteIsTimedPerSourceBatch(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	src := &fakeSource{batches: [][]Message{messages(10), messages(10)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	// Two source batches, twenty messages, two observations.
	assert.Equal(t, uint64(2), phaseCounts(t, reader)[phaseHandlerWrite])
}
