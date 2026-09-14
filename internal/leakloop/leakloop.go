// Package leakloop measures whether a component holds on to memory in
// proportion to the work it does.
//
// A leak on a production worker shows up per hour, but it is caused per
// event: per message, per batch, per flush, per reconnect. A soak that replays
// messages fast changes how many of each event happen per hour, so a leak tied
// to flushes looks flat in a run that sends millions of messages and flushes
// a few dozen times. Each loop here drives one kind of event as fast as the
// component allows and reports growth per event of that kind, which is the
// number that transfers to production: multiply it by how often the event
// happens there.
//
// Every sample splits the process into three numbers. A Go heap profile sees
// only the first, and duckdb_memory() only the third.
//
//	Go retained   runtime Sys - HeapReleased
//	native        RssAnon - Go retained, where cgo and allocator growth lands
//	DuckDB        duckdb_memory() on the component's connection
//
// RssAnon is exact on Linux only. On macOS turbostats falls back to peak
// resident size, which only rises, so run the loops in a Linux container when
// the native number matters.
package leakloop

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/turbostats"
)

// Count scales a loop's default event count by SQLFLOW_LEAK_SCALE. The
// defaults finish in seconds so the -short pass runs every loop; a scale of
// 100 is a few million messages and tens of thousands of flushes.
func Count(tb testing.TB, base int) int {
	tb.Helper()
	v := os.Getenv("SQLFLOW_LEAK_SCALE")
	if v == "" {
		return base
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		tb.Fatalf("SQLFLOW_LEAK_SCALE=%q is not a positive number", v)
	}
	n := int(float64(base) * f)
	if n < 1 {
		n = 1
	}
	return n
}

// Sample is one reading of the process.
type Sample struct {
	Events     int64
	RSSAnon    int64
	GoRetained int64
	// DuckDB is duckdb_memory() summed over every tag, or -1 when the loop
	// has no connection.
	DuckDB int64
}

// Native is resident anonymous memory the Go runtime does not account for.
func (s Sample) Native() int64 { return s.RSSAnon - s.GoRetained }

// Loop collects samples for one component under one kind of event.
type Loop struct {
	tb      testing.TB
	name    string
	unit    string
	conn    adbc.Connection
	samples []Sample
}

// New starts a loop. unit names the event, singular: "message", "flush". conn
// is the DuckDB connection whose duckdb_memory() to read, or nil.
func New(tb testing.TB, name, unit string, conn adbc.Connection) *Loop {
	return &Loop{tb: tb, name: name, unit: unit, conn: conn}
}

// Sample reads the process after events events. It collects garbage and
// returns freed pages to the OS first, so Go retained is what the runtime
// cannot give back rather than what it has not got round to.
//
// SQLFLOW_LEAK_MALLOC_TRIM does the same for glibc on Linux: malloc_trim(0)
// before the reading. Native growth that disappears with it is memory freed
// and kept by the allocator. Growth that survives it is still referenced.
func (l *Loop) Sample(events int64) {
	l.tb.Helper()
	runtime.GC()
	debug.FreeOSMemory()
	if os.Getenv("SQLFLOW_LEAK_MALLOC_TRIM") != "" {
		mallocTrim()
	}

	rss, err := turbostats.ResidentAnonBytes()
	if err != nil {
		l.tb.Fatal(err)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := Sample{
		Events:     events,
		RSSAnon:    rss,
		GoRetained: int64(ms.Sys - ms.HeapReleased),
		DuckDB:     -1,
	}
	if l.conn != nil {
		s.DuckDB = DuckDBBytes(l.tb, l.conn)
	}
	l.samples = append(l.samples, s)
}

// Result is growth per event over the samples after warm-up.
type Result struct {
	Events                         int64
	Native, GoRetained, DuckDB     float64
	NativeGrowth, GoRetainedGrowth int64
}

// warmupFraction of the samples are excluded from the fit. Allocators and
// DuckDB's buffer pool grow to a working size over the first thousands of
// events, and that is not a leak.
const warmupFraction = 0.3

// Report fits growth per event and logs every sample with the fit. It needs
// at least four samples.
func (l *Loop) Report() Result {
	l.tb.Helper()
	if len(l.samples) < 4 {
		l.tb.Fatalf("%s: %d samples; take at least four", l.name, len(l.samples))
	}
	from := int(float64(len(l.samples)) * warmupFraction)
	fit := l.samples[from:]
	first, last := fit[0], fit[len(fit)-1]

	r := Result{
		Events:           last.Events,
		Native:           slope(fit, Sample.Native),
		GoRetained:       slope(fit, func(s Sample) int64 { return s.GoRetained }),
		NativeGrowth:     last.Native() - first.Native(),
		GoRetainedGrowth: last.GoRetained - first.GoRetained,
	}
	if l.conn != nil {
		r.DuckDB = slope(fit, func(s Sample) int64 { return s.DuckDB })
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d %s\n", l.name, last.Events, plural(l.unit))
	fmt.Fprintf(&b, "  %12s %10s %10s %10s %10s\n", plural(l.unit), "rss anon", "go", "native", "duckdb")
	for i, s := range l.samples {
		mark := " "
		if i == from {
			mark = ">"
		}
		duck := "-"
		if s.DuckDB >= 0 {
			duck = mib(s.DuckDB)
		}
		fmt.Fprintf(&b, "%s %12d %10s %10s %10s %10s\n", mark, s.Events, mib(s.RSSAnon), mib(s.GoRetained), mib(s.Native()), duck)
	}
	fmt.Fprintf(&b, "  per %s after warm-up (from >): native %+.2f B, go %+.2f B", l.unit, r.Native, r.GoRetained)
	if l.conn != nil {
		fmt.Fprintf(&b, ", duckdb %+.2f B", r.DuckDB)
	}
	fmt.Fprintf(&b, "; native %+.1f MiB over %d %s", float64(r.NativeGrowth)/(1<<20), last.Events-first.Events, plural(l.unit))
	l.tb.Log(b.String())
	return r
}

func plural(unit string) string {
	if strings.HasSuffix(unit, "sh") || strings.HasSuffix(unit, "ch") {
		return unit + "es"
	}
	return unit + "s"
}

func mib(b int64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

// slope is the least-squares growth in bytes per event.
func slope(s []Sample, y func(Sample) int64) float64 {
	var n, sx, sy, sxx, sxy float64
	for _, p := range s {
		x, v := float64(p.Events), float64(y(p))
		n++
		sx += x
		sy += v
		sxx += x * x
		sxy += x * v
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / den
}

// DuckDBBytes is duckdb_memory() summed over every tag on conn.
func DuckDBBytes(tb testing.TB, conn adbc.Connection) int64 {
	tb.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(`SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT FROM duckdb_memory()`); err != nil {
		tb.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	defer reader.Release()
	for reader.Next() {
		if rec := reader.Record(); rec.NumRows() > 0 {
			return rec.Column(0).(*array.Int64).Value(0)
		}
	}
	tb.Fatal("duckdb_memory() returned no rows")
	return 0
}

// Posts returns up to n Jetstream post events, one JSON object each.
//
// SQLFLOW_LEAK_JETSTREAM names a capture from dev/bench/record.py, gzipped or
// not, and the loops then run on real posts: their sizes, languages, and
// nesting. Without it they get generated posts of the same shape.
func Posts(tb testing.TB, n int) [][]byte {
	tb.Helper()
	path := os.Getenv("SQLFLOW_LEAK_JETSTREAM")
	if path == "" {
		return generatedPosts(n)
	}
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			tb.Fatal(err)
		}
		defer gz.Close()
		r = gz
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 32<<20)
	out := make([][]byte, 0, n)
	for len(out) < n && sc.Scan() {
		if line := sc.Bytes(); len(line) > 0 {
			out = append(out, append([]byte(nil), line...))
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	if len(out) == 0 {
		tb.Fatalf("%s holds no events", path)
	}
	return out
}

var langs = []string{"en", "ja", "pt", "es", "de", "fr", "ko", "zh", "it", "nl", "tr", "pl"}

func generatedPosts(n int) [][]byte {
	const startUS = int64(1_789_300_000_000_000)
	out := make([][]byte, n)
	for i := range out {
		lang := `"` + langs[i%len(langs)] + `"`
		if i%7 == 0 {
			lang = ""
		}
		op := "create"
		if i%15 == 0 {
			op = "delete"
		}
		text := strings.Repeat("post text ", 5+i%40)
		out[i] = []byte(fmt.Sprintf(
			`{"did":"did:plc:%024d","time_us":%d,"kind":"commit","commit":{"rev":"3l%011d","operation":%q,"collection":"app.bsky.feed.post","rkey":"3l%011d","record":{"$type":"app.bsky.feed.post","createdAt":"2026-09-13T12:00:00.000Z","langs":[%s],"text":%q},"cid":"bafyrei%052d"}}`,
			i, startUS+int64(i)*20_000, i, op, i, lang, text, i))
	}
	return out
}
