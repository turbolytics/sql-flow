package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

// What the watermark costs, in the two places it is paid.
//
// Per message: Observe, on the consume loop's hot path, behind the tracker's
// mutex. The loop's own budget is tens of nanoseconds a message -- timing each
// writeMessage call once took it from 22 ns to 91 -- so a lock and a map
// lookup per record is not obviously affordable and is measured rather than
// assumed.
//
// Per commit: Next over the partitions, and one UPDATE per window whose
// watermark moved. Against what it replaced: the old design wrote
// sqlflow_progress on every commit and rewrote a tri-state column on every
// idle tick besides, so the comparison that matters is not against zero.
//
//	go test ./internal/core/ -bench 'Watermark|WritePath' -benchmem -run '^$' \
//	  -benchtime 200000x -count 3

// benchSaver writes nowhere: the difference between this and the DuckDB store
// is the price of the statement, which the commit benchmarks price separately.
type benchSaver struct{ n int }

func (s *benchSaver) Save(context.Context, string, time.Time) error {
	s.n++
	return nil
}

func benchSpecs(n int) []WindowSpec {
	specs := make([]WindowSpec, n)
	for i := range specs {
		specs[i] = WindowSpec{
			Name:      "w",
			Size:      time.Minute,
			Grace:     time.Minute,
			IdleClose: 10 * time.Second,
		}
	}
	return specs
}

// BenchmarkWatermarkObserve is the per-record cost on its own, with no engine
// around it: one partition, then four, since the map lookup is keyed on the
// partition and a Kafka consumer holds several.
func BenchmarkWatermarkObserve(b *testing.B) {
	at := time.Now().Add(-time.Hour).UnixNano()

	for _, partitions := range []int32{1, 4} {
		b.Run(partitionName(partitions), func(b *testing.B) {
			w := NewWatermarks(benchSpecs(1), time.Now)
			ps := make([]int32, partitions)
			for i := range ps {
				ps[i] = int32(i)
			}
			w.Assigned(map[string][]int32{"t": ps})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w.Observe("t", int32(i)%partitions, at+int64(i))
			}
		})
	}
}

// BenchmarkWatermarkNext is what a commit pays to compute the assertion,
// before any statement: the minimum over the partitions, once per window.
func BenchmarkWatermarkNext(b *testing.B) {
	at := time.Now().Add(-time.Hour).UnixNano()

	for _, windows := range []int{1, 4} {
		for _, partitions := range []int32{1, 4} {
			b.Run(windowName(windows)+"/"+partitionName(partitions), func(b *testing.B) {
				w := NewWatermarks(benchSpecs(windows), time.Now)
				ps := make([]int32, partitions)
				for i := range ps {
					ps[i] = int32(i)
				}
				w.Assigned(map[string][]int32{"t": ps})
				for _, p := range ps {
					w.Observe("t", p, at)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// A commit that moves the watermark, which is the dear
					// case: every partition delivered since the last one.
					w.Observe("t", 0, at+int64(i)+1)
					w.Commit(w.Next())
				}
			})
		}
	}
}

// BenchmarkCommitStateWindowed is the commit path with and without a window,
// so the watermark's share of a commit is attributable rather than guessed at.
// Interleave the arms and read the difference, not the absolute figures.
func BenchmarkCommitStateWindowed(b *testing.B) {
	ctx := context.Background()
	at := time.Now().Add(-time.Hour).UnixNano()

	// The control: the same commit with no window at all.
	b.Run("no_window", func(b *testing.B) {
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(benchProgressNoop{}))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx, progressOnInterval); err != nil {
				b.Fatal(err)
			}
		}
	})

	// A window whose watermark does not move: every commit computes the
	// minimum and writes nothing, which is what a commit inside one bucket
	// does once the grace has been reached.
	b.Run("window_unmoved", func(b *testing.B) {
		w := NewWatermarks(benchSpecs(1), time.Now)
		w.Assigned(map[string][]int32{"t": {0}})
		w.Observe("t", 0, at)
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(benchProgressNoop{}), WithWindows(w, &benchSaver{}))
		w.Commit(w.Next())

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx, progressOnInterval); err != nil {
				b.Fatal(err)
			}
		}
	})

	// And one that moves on every commit, with the write stubbed: the
	// tracker's own cost at its worst.
	b.Run("window_moved_nowrite", func(b *testing.B) {
		w := NewWatermarks(benchSpecs(1), time.Now)
		w.Assigned(map[string][]int32{"t": {0}})
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(benchProgressNoop{}), WithWindows(w, &benchSaver{}))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w.Observe("t", 0, at+int64(i)+1)
			if err := tb.commitState(ctx, progressOnInterval); err != nil {
				b.Fatal(err)
			}
		}
	})

	// The statement itself: the UPDATE against DuckDB, unpaced, which is what
	// a commit pays when it is the one that writes.
	duckdb := func(b *testing.B, opts ...TurbineOption) {
		conn, cleanup := benchConn(b)
		defer cleanup()
		store := NewWatermarkStore(conn)
		if err := store.Init(ctx); err != nil {
			b.Fatal(err)
		}
		w := NewWatermarks(benchSpecs(1), time.Now)
		w.Assigned(map[string][]int32{"t": {0}})
		tb := benchTurbine(b, append([]TurbineOption{
			WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(benchProgressNoop{}),
			WithWindows(w, store),
		}, opts...)...)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w.Observe("t", 0, at+int64(i)+1)
			if err := tb.commitState(ctx, progressOnInterval); err != nil {
				b.Fatal(err)
			}
		}
	}

	// Unpaced, which is what every commit used to pay: the statement, about
	// 130 microseconds, on every batch where event time advanced.
	b.Run("window_moved_duckdb_every_commit", func(b *testing.B) {
		duckdb(b, WithWatermarkWriteInterval(0))
	})

	// And paced as production paces it: the same commits, one statement a
	// second, which is what the interval buys back.
	b.Run("window_moved_duckdb_paced", func(b *testing.B) { duckdb(b) })
}

func partitionName(n int32) string {
	if n == 1 {
		return "1_partition"
	}
	return "4_partitions"
}

func windowName(n int) string {
	if n == 1 {
		return "1_window"
	}
	return "4_windows"
}

// benchWindowedSource delivers messages carrying an event time, which is what
// a windowing pipeline's records have and what Observe reads.
type benchWindowedSource struct {
	n    int
	size int
	at   int64
	ch   chan []Message
}

func (s *benchWindowedSource) Start() error { return nil }

func (s *benchWindowedSource) Stream() <-chan []Message {
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
				batch[i] = Message{
					Value:        []byte(`{"a": 1}`),
					Topic:        "t",
					EventAtNanos: s.at + int64(sent+i),
				}
			}
			s.ch <- batch
			sent += k
		}
	}()
	return s.ch
}

func (s *benchWindowedSource) Commit() error { return nil }
func (s *benchWindowedSource) Close() error  { return nil }

// BenchmarkConsumeLoopWindowedWritePath is the loop's per-message cost with
// the watermark on, against the same loop with it off. The gap is Observe: a
// mutex, a map lookup and two comparisons per record, plus the placement rule
// a windowing pipeline also runs.
func BenchmarkConsumeLoopWindowedWritePath(b *testing.B) {
	at := time.Now().Add(-time.Hour).UnixNano()

	run := func(b *testing.B, batchSize int, windowed bool) {
		src := &benchWindowedSource{n: b.N, size: 1000, at: at}
		opts := []TurbineOption{}
		if windowed {
			w := NewWatermarks(benchSpecs(1), time.Now)
			w.Assigned(map[string][]int32{"t": {0}})
			opts = append(opts, WithWindows(w, &benchSaver{}), WithEventTimePlacement(true))
		}
		tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, batchSize, time.Hour,
			&sync.Mutex{}, PipelineErrorPolicies{}, opts...)

		b.ReportAllocs()
		b.ResetTimer()
		if _, err := tb.ConsumeLoop(context.Background(), 0); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("windows_off/batch_100000", func(b *testing.B) { run(b, 100000, false) })
	b.Run("windows_on/batch_100000", func(b *testing.B) { run(b, 100000, true) })
	b.Run("windows_off/batch_500", func(b *testing.B) { run(b, 500, false) })
	b.Run("windows_on/batch_500", func(b *testing.B) { run(b, 500, true) })
}
