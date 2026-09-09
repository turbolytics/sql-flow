package handlers

import (
	"bufio"
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

// residentAnonBytes is the process's anonymous resident memory. That is the
// figure a native leak moves: Go's heap profiler cannot see a buffer that
// DuckDB allocated across the ADBC boundary, and duckdb_memory() does not
// track it either, so the only honest instrument is the process itself.
//
// Linux reads RssAnon, which excludes file-backed pages and is exact. Other
// platforms fall back to peak resident size from getrusage, which only ever
// rises, so it is a weaker signal there; a leak still shows as growth between
// two measurements, since a steady process has a steady peak.
func residentAnonBytes(t *testing.T) int64 {
	t.Helper()
	if runtime.GOOS == "linux" {
		f, err := os.Open("/proc/self/status")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "RssAnon:") {
				continue
			}
			fields := strings.Fields(line)
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return kb << 10
		}
		t.Fatal("RssAnon not found in /proc/self/status")
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		maxrss <<= 10 // kilobytes everywhere but darwin, which reports bytes
	}
	return maxrss
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
// a threshold of 8 MiB now.
func TestInferredInvoke_DoesNotLeakNativeMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("soak-shaped test, skipped under -short")
	}
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
