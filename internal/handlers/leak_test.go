package handlers

import (
	"context"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
)

// residentAnonBytes is turbostats.ResidentAnonBytes, which moved there so the
// TurboStats bundle and this test read the same number, with the test's
// failure semantics.
func residentAnonBytes(t *testing.T) int64 {
	t.Helper()
	n, err := turbostats.ResidentAnonBytes()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func settle() {
	runtime.GC()
	debug.FreeOSMemory()
}

// TestInferredInvoke_DoesNotLeakNativeMemory drives batches through the
// handler the way the pipeline does, Invoke then Release then Init, and
// asserts the process does not grow.
//
// Before the fix every Invoke retained its result table once more than the
// caller released it. The table was never freed, and with it the Arrow
// buffers DuckDB handed back for every row. Measured on v1.0.6 that cost
// about 44 bytes per message and grew for as long as the process lived. This
// loop pushes half a million messages, which leaked over 20 MiB then, against
// a threshold of 8 MiB now. It runs in under a second, so it belongs in the
// -short pass: a leak linear in messages needs message volume to show, not
// wall clock, and this is the pass CI runs on every change.
func TestInferredInvoke_DoesNotLeakNativeMemory(t *testing.T) {
	coverage.Covers(t, "handler.inferred_mem")
	conn, closeConn := newADBCConn(t)
	defer closeConn()

	ctx := context.Background()
	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	msg := []byte(`{"sensor_id":3,"ts":"2026-09-11T00:00:00","value":12.5}`)
	const batchSize, warmup, iters = 1000, 50, 500

	run := func(n int) {
		for i := 0; i < n; i++ {
			for j := 0; j < batchSize; j++ {
				if err := h.Write(msg); err != nil {
					t.Fatal(err)
				}
			}
			tbl, err := h.Invoke(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if tbl == nil {
				t.Fatal("expected a table")
			}
			tbl.Release()
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}

	run(warmup)
	settle()
	before := residentAnonBytes(t)

	run(iters)
	settle()
	after := residentAnonBytes(t)

	const limit = 8 << 20
	growth := after - before
	t.Logf("resident anon memory: before %d MiB, after %d MiB, growth %d MiB over %d messages",
		before>>20, after>>20, growth>>20, iters*batchSize)
	if growth > limit {
		t.Fatalf("process grew %d MiB over %d messages; the handler is retaining native buffers", growth>>20, iters*batchSize)
	}
}

// TestInferredInvoke_ReleasesIngestRecord checks the Go-allocated side with
// arrow's checked allocator: the record the handler builds to ingest a batch
// must be fully released once Invoke returns. This did not catch the table
// leak above, because those buffers were DuckDB's, not the allocator's. It is
// here so the other half of the refcount contract is enforced too.
func TestInferredInvoke_ReleasesIngestRecord(t *testing.T) {
	coverage.Covers(t, "handler.inferred_mem")
	conn, closeConn := newADBCConn(t)
	defer closeConn()

	ctx := context.Background()
	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	checked := memory.NewCheckedAllocator(memory.NewGoAllocator())
	h.alloc = checked

	for i := 0; i < 20; i++ {
		if err := h.Write([]byte(`{"a":1,"b":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	tbl, err := h.Invoke(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tbl.Release()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	checked.AssertSize(t, 0)
}
