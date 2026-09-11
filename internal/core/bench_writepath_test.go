package core

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// benchSource delivers n messages in batches, generated as the loop consumes
// them rather than buffered up front, so a run at b.N in the millions measures
// the loop and not the fixture.
type benchSource struct {
	n    int
	size int
	ch   chan []Message
}

func (s *benchSource) Start() error { return nil }

func (s *benchSource) Stream() <-chan []Message {
	s.ch = make(chan []Message, 1)
	go func() {
		defer close(s.ch)
		for sent := 0; sent < s.n; {
			k := s.size
			if rem := s.n - sent; rem < k {
				k = rem
			}
			batch := make([]Message, k)
			for i := range batch {
				batch[i] = Message{Value: []byte(`{"a": 1}`)}
			}
			s.ch <- batch
			sent += k
		}
	}()
	return s.ch
}

func (s *benchSource) Commit() error { return nil }
func (s *benchSource) Close() error  { return nil }

// BenchmarkConsumeLoopWritePath measures the consume loop per message, which
// is where phase timing has to be cheap.
//
// Timing each writeMessage call took this loop from 22 ns/op to 91 -- 4.1x --
// so handler.write is measured by bracketing the loop instead. The two batch
// sizes separate the two costs: batch 100000 leaves the loop almost alone with
// its per-message work, and batch 500 pays a full processBatch every 500
// messages, where the other five phases are timed.
//
//	go test ./internal/core/ -bench WritePath -benchmem -run '^$'
//
// fakeHandler.Write increments a counter and returns, so the loop's own
// overhead is the whole measurement. A real handler parses JSON and appends to
// an Arrow builder, which dwarfs it. These figures are the worst case, not the
// expected one.
//
// Compare two commits with benchstat and interleaved runs rather than reading
// absolute figures: on a laptop the arm that runs second is the warmer one.
func BenchmarkConsumeLoopWritePath(b *testing.B) {
	run := func(b *testing.B, batchSize int, opts ...TurbineOption) {
		src := &benchSource{n: b.N, size: 1000}
		tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, batchSize, time.Hour,
			&sync.Mutex{}, PipelineErrorPolicies{}, opts...)

		b.ReportAllocs()
		b.ResetTimer()
		if _, err := tb.ConsumeLoop(context.Background(), 0); err != nil {
			b.Fatal(err)
		}
	}

	metered := func(b *testing.B) TurbineOption {
		b.Helper()
		m, err := NewMetrics(sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewManualReader())))
		if err != nil {
			b.Fatal(err)
		}
		return WithMetrics(m)
	}

	// The default instruments record nothing, which is what a pipeline runs
	// with when --metrics is off. What remains is the clock reads themselves.
	b.Run("metrics_off/batch_100000", func(b *testing.B) { run(b, 100000) })
	b.Run("metrics_off/batch_500", func(b *testing.B) { run(b, 500) })

	// With a real reader attached, the histogram records are paid too.
	b.Run("metrics_on/batch_100000", func(b *testing.B) { run(b, 100000, metered(b)) })
	b.Run("metrics_on/batch_500", func(b *testing.B) { run(b, 500, metered(b)) })
}
