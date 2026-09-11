package core

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/duckdb"
)

// The commit path runs once per batch, so anything added to it is paid at the
// batch rate: at batch 5000 and a million messages a second, two hundred
// times a second. These benchmarks exist so that cost is a number in seconds
// rather than a twenty minute container A/B, and so a regression shows up as
// a relative change against the same machine on the same day.
//
//	go test ./internal/core/ -bench 'Commit|Progress' -benchmem -run '^$'
//
// Compare two commits with benchstat rather than reading absolute figures:
// the numbers move with the host, the ratio does not.

// benchTx is a transaction boundary that does nothing, so a commit benchmark
// measures the engine's own work rather than DuckDB's.
type benchTx struct{}

func (benchTx) Commit(context.Context) error   { return nil }
func (benchTx) Rollback(context.Context) error { return nil }

// benchOffsets saves nothing, for the same reason.
type benchOffsets struct{}

func (benchOffsets) Save(context.Context, *Marks) error { return nil }

// benchProgressNoop records nothing: the difference between this and the real
// store is the price of keeping sqlflow_progress current.
type benchProgressNoop struct{}

func (benchProgressNoop) Record(context.Context, Progress) error { return nil }

// benchConn opens an in-memory database, which is what a pipeline with no
// state path runs on.
func benchConn(b *testing.B) (adbc.Connection, func()) {
	b.Helper()
	return benchConnAt(b, "")
}

// benchConnAt opens a file-backed database when given a path. A pipeline that
// declares pipeline.state.path runs on one of these, and it is the case that
// actually reads sqlflow_progress, so the cost there is the one that counts.
func benchConnAt(b *testing.B, path string) (adbc.Connection, func()) {
	b.Helper()
	db, err := duckdb.OpenPath(context.Background(), path)
	if err != nil {
		b.Fatal(err)
	}
	conn, err := db.Connect(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	return conn, func() {
		conn.Close()
		db.Close()
	}
}

func benchTurbine(b *testing.B, opts ...TurbineOption) *Turbine {
	b.Helper()
	return NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000,
		time.Hour, &sync.Mutex{}, PipelineErrorPolicies{}, opts...)
}

// BenchmarkCommitState is the headline comparison: the same commit with no
// progress store, with one that does nothing, and with the real one writing
// to DuckDB. The first two isolate the bookkeeping from the statement, so a
// regression can be attributed rather than guessed at.
func BenchmarkCommitState(b *testing.B) {
	ctx := context.Background()

	b.Run("no_progress_store", func(b *testing.B) {
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	// The snapshot and the flag, without the statement.
	b.Run("snapshot_only", func(b *testing.B) {
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(benchProgressNoop{}))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	// The whole thing, including the UPDATE against DuckDB.
	b.Run("duckdb_progress_store", func(b *testing.B) {
		conn, cleanup := benchConn(b)
		defer cleanup()
		store := NewProgressStore(conn)
		if err := store.Init(ctx); err != nil {
			b.Fatal(err)
		}
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, benchTx{}),
			WithProgressStore(store))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	// The real path for a windowed pipeline: state on disk, autocommit off,
	// so the progress write rides the batch's own transaction. Run with its
	// control below, because most of what this measures is DuckDB committing
	// to a file, which the pipeline pays whether or not progress exists.
	b.Run("duckdb_on_disk_no_progress", func(b *testing.B) {
		conn, cleanup := benchConnAt(b, filepath.Join(b.TempDir(), "state.db"))
		defer cleanup()
		po, ok := conn.(adbc.PostInitOptions)
		if !ok {
			b.Fatal("connection does not support disabling autocommit")
		}
		if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
			b.Fatal(err)
		}
		tx, ok := conn.(stateTx)
		if !ok {
			b.Fatal("connection is not a transaction boundary")
		}
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, tx))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("duckdb_on_disk", func(b *testing.B) {
		conn, cleanup := benchConnAt(b, filepath.Join(b.TempDir(), "state.db"))
		defer cleanup()
		store := NewProgressStore(conn)
		if err := store.Init(ctx); err != nil {
			b.Fatal(err)
		}
		po, ok := conn.(adbc.PostInitOptions)
		if !ok {
			b.Fatal("connection does not support disabling autocommit")
		}
		if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
			b.Fatal(err)
		}
		tx, ok := conn.(stateTx)
		if !ok {
			b.Fatal("connection is not a transaction boundary")
		}
		tb := benchTurbine(b, WithStateStore(benchOffsets{}, tx), WithProgressStore(store))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	// An idle tick on a pipeline with no state database still records, and a
	// trickle spends most of its life here.
	b.Run("stateless_with_progress", func(b *testing.B) {
		conn, cleanup := benchConn(b)
		defer cleanup()
		store := NewProgressStore(conn)
		if err := store.Init(ctx); err != nil {
			b.Fatal(err)
		}
		tb := benchTurbine(b, WithProgressStore(store))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tb.commitState(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkProgressStoreRecord is the statement on its own.
//
// Measured at about 112 microseconds in memory, which is why the commit path
// throttles it rather than running it per batch. Preparing the statement and
// binding parameters was tried and is not the answer: 135 microseconds
// against 160 for the text form on the same run, with six times the
// allocations. The cost is what an ADBC round trip costs, not parsing, so
// caching the statement buys almost nothing and writing less often buys
// everything.
func BenchmarkProgressStoreRecord(b *testing.B) {
	ctx := context.Background()
	conn, cleanup := benchConn(b)
	defer cleanup()
	store := NewProgressStore(conn)
	if err := store.Init(ctx); err != nil {
		b.Fatal(err)
	}

	now := time.Now().UTC()
	b.Run("with_arrival", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := store.Record(ctx, Progress{
				LastArrival: now, LastCommit: now, Messages: int64(i),
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	// An idle tick sends no arrival, so it writes one column fewer.
	b.Run("idle_tick", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := store.Record(ctx, Progress{
				LastCommit: now, Messages: int64(i),
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
