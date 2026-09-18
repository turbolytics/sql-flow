// Package rendertemplate holds the Deploy to Render template's SQL, under
// render/migrations, to what it promises when several pipeline processes
// write to one database. The template is YAML, SQL and shell; this package is
// tests only, because the promise is only as good as the concurrency it was
// tried under, and that needs goroutines and open transactions.
package rendertemplate

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
)

const (
	migrationsDir = "../../render/migrations"
	// The template's pipeline keeps its window in memory and has no state
	// file, which is the stateless configuration of the consume loop.
	templatePipeline = "pipeline.stateless"
	mergeInvariant   = "pipeline.writers.merge_exactly"
)

var writerKey = []string{"bucket", "name", "type", "dimensions_key", "writer"}

// migrations is every file the template's runner applies, in its order.
func migrations(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	assert.NoError(t, err)
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatalf("no migrations under %s", migrationsDir)
	}
	out := make([]string, len(paths))
	for i, p := range paths {
		b, err := os.ReadFile(p)
		assert.NoError(t, err)
		out[i] = string(b)
	}
	return out
}

type server struct {
	dsn  string
	conn *pgx.Conn
}

// start runs Postgres 18, the template's version, and applies the migrations
// after edit has had its way with them. Each case gets its own container: the
// migrations create functions and triggers a second apply would collide with.
func start(t *testing.T, edit func(string) string) *server {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("metrics"),
		tcpostgres.WithUsername("metrics"),
		tcpostgres.WithPassword("metrics"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		// A deadlock reaches the test as one line. The server's log says which
		// locks the two processes held and wanted, which is the diagnosis.
		if t.Failed() {
			if logs, err := pg.Logs(context.Background()); err == nil {
				raw, _ := io.ReadAll(logs)
				for _, line := range strings.Split(string(raw), "\n") {
					if strings.Contains(line, "deadlock") || strings.Contains(line, "waits for") ||
						strings.Contains(line, "blocked by") || strings.Contains(line, "CONTEXT") {
						t.Log(line)
					}
				}
			}
		}
		_ = pg.Terminate(context.Background())
	})
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	srv := &server{dsn: dsn, conn: connect(t, dsn)}
	for _, script := range migrations(t) {
		if edit != nil {
			script = edit(script)
		}
		// One transaction per file, as bin/migrate.sh applies them.
		exec(t, srv.conn, "BEGIN")
		exec(t, srv.conn, script)
		exec(t, srv.conn, "COMMIT")
	}
	return srv
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	exec(t, conn, "SET TIME ZONE 'UTC'")
	return conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql, args...)
	assert.NoError(t, err)
}

var mergeLock = regexp.MustCompile(`  PERFORM pg_advisory_xact_lock\(hashtextextended\('metrics_1m_merge', 0\)\);\n`)

// withoutTheLock removes the merge trigger's advisory lock and nothing else.
func withoutTheLock(script string) string {
	if !strings.Contains(script, "metrics_1m_merge") {
		return script
	}
	out := mergeLock.ReplaceAllString(script, "")
	if out == script {
		panic("no lock in the merge trigger; migrations/0004_writers.sql changed")
	}
	return out
}

const publish = `
INSERT INTO metrics_1m_writers (bucket, name, type, dimensions_key, writer,
  value_sum, value_count, value_min, value_max, value_last, last_at)
VALUES ('2026-09-15 10:01:00+00', 'checkout', 'count', '{}', $1, $2::double precision, 1,
  $2::double precision, $2::double precision, $2::double precision, $3::timestamptz)
ON CONFLICT (bucket, name, type, dimensions_key, writer) DO UPDATE SET
  value_sum = excluded.value_sum, value_count = excluded.value_count, value_min = excluded.value_min,
  value_max = excluded.value_max, value_last = excluded.value_last, last_at = excluded.last_at`

// Two pipeline processes hold parts of one minute of one series: two
// instances behind a load balancer, or the old and the new instance during a
// deploy. The first has published and not yet committed when the second
// publishes. What the minute holds afterwards must be both.
func TestIntegrationTemplateRender_TwoWritersOfOneMinuteAreSummed(t *testing.T) {
	coverage.Covers(t, "template.render")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}

	for _, tt := range []struct {
		name         string
		secondWriter string
		edit         func(string) string
		want         string
		proves       bool
	}{
		{"each process has its own writer id", "b", nil, "12|2|5|7|5", true},
		// The test must be able to fail, and these are the two ways the
		// template could. Without the lock the second writer re-merges from a
		// snapshot that lacks the first's uncommitted row, waits on the
		// merged row, and overwrites it with its own half.
		{"without the lock", "b", withoutTheLock, "5|1|5|5|5", false},
		// The defect the writer column exists to prevent: processes that
		// share an id share a key, and the second replaces the first. It is
		// why bin/entrypoint.sh makes the id and never reads one.
		{"both processes share a writer id", "a", nil, "5|1|5|5|5", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.proves {
				coverage.Invariant(t, mergeInvariant, templatePipeline)
			}
			srv := start(t, tt.edit)
			first, second := connect(t, srv.dsn), connect(t, srv.dsn)

			exec(t, first, "BEGIN")
			exec(t, first, publish, "a", 7.0, "2026-09-15 10:01:10+00")

			done := make(chan error, 1)
			go func() {
				_, err := second.Exec(context.Background(), publish, tt.secondWriter, 5.0, "2026-09-15 10:01:40+00")
				done <- err
			}()
			// Long enough for the second writer to reach the first's lock or row.
			time.Sleep(500 * time.Millisecond)
			exec(t, first, "COMMIT")
			assert.NoError(t, <-done)

			for _, table := range []string{"metrics_1m", "metrics_5m", "metrics_1d"} {
				var got string
				assert.NoError(t, srv.conn.QueryRow(context.Background(),
					"SELECT value_sum||'|'||value_count||'|'||value_min||'|'||value_max||'|'||value_last FROM "+table).Scan(&got))
				if got != tt.want {
					t.Fatalf("%s holds %s, want %s (sum|count|min|max|last)", table, got, tt.want)
				}
			}
		})
	}
}

// The lock waits, then reads a new snapshot. Under REPEATABLE READ the merge
// would read the old one and lose the writer it waited for, so it refuses.
func TestIntegrationTemplateRender_MergeRefusesAnotherIsolationLevel(t *testing.T) {
	coverage.Covers(t, "template.render")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := start(t, nil)

	exec(t, srv.conn, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	_, err := srv.conn.Exec(context.Background(), publish, "a", 7.0, "2026-09-15 10:01:10+00")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "READ COMMITTED"))
	exec(t, srv.conn, "ROLLBACK")
}

type key struct {
	minute int
	series int
}

type row struct {
	sum, min, max, last float64
	count               int64
	lastAt              time.Time
}

var (
	firstMinute = time.Date(2026, 9, 15, 10, 3, 0, 0, time.UTC)
	seriesKeys  = []string{`{}`, `{"plan":"free"}`, `{"plan":"pro"}`}
)

// Twelve minutes from 10:03, so they cross 5m and 15m boundaries.
const minutes = 12

func bucketOf(k key) time.Time { return firstMinute.Add(time.Duration(k.minute) * time.Minute) }

// flush publishes rows through the Postgres sink, the pipeline's own write
// path: a COPY into a temp table, then one INSERT ... ON CONFLICT.
func flush(s sinksWriter, writer string, rows map[key]row) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bucket", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "dimensions_key", Type: arrow.BinaryTypes.String},
		{Name: "writer", Type: arrow.BinaryTypes.String},
		{Name: "value_sum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "value_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value_min", Type: arrow.PrimitiveTypes.Float64},
		{Name: "value_max", Type: arrow.PrimitiveTypes.Float64},
		{Name: "value_last", Type: arrow.PrimitiveTypes.Float64},
		{Name: "last_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for k, r := range rows {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(bucketOf(k).UnixMicro()))
		b.Field(1).(*array.StringBuilder).Append("load")
		b.Field(2).(*array.StringBuilder).Append("gauge")
		b.Field(3).(*array.StringBuilder).Append(seriesKeys[k.series])
		b.Field(4).(*array.StringBuilder).Append(writer)
		b.Field(5).(*array.Float64Builder).Append(r.sum)
		b.Field(6).(*array.Int64Builder).Append(r.count)
		b.Field(7).(*array.Float64Builder).Append(r.min)
		b.Field(8).(*array.Float64Builder).Append(r.max)
		b.Field(9).(*array.Float64Builder).Append(r.last)
		b.Field(10).(*array.TimestampBuilder).Append(arrow.Timestamp(r.lastAt.UnixMicro()))
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	if err := s.WriteTable(context.Background(), tbl); err != nil {
		return err
	}
	return s.Flush(context.Background())
}

type sinksWriter interface {
	WriteTable(context.Context, arrow.Table) error
	Flush(context.Context) error
}

// merge folds one writer's row into what a minute holds.
func merge(into *row, r row, first bool) {
	if first {
		*into = r
		return
	}
	into.sum += r.sum
	into.count += r.count
	into.min = min(into.min, r.min)
	into.max = max(into.max, r.max)
	if r.lastAt.After(into.lastAt) {
		into.last, into.lastAt = r.last, r.lastAt
	}
}

// Four processes publish and republish overlapping minutes of three series at
// once, through the sink, for forty rounds each. Every value is a multiple of
// 0.25, so sums are exact and equality is exact.
//
// What each process last published for a key is what it holds the minute to
// be. The merge of those, worked out here in Go from what the writers
// remember and never from the database, is what metrics_1m and every grain
// above it must hold: nothing lost to another writer, nothing counted twice
// by a republish.
func TestIntegrationTemplateRender_ConcurrentWritersLoseNothingAtAnyGrain(t *testing.T) {
	coverage.Covers(t, "template.render")
	coverage.Invariant(t, mergeInvariant, templatePipeline)
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := start(t, nil)

	const writers, rounds = 4, 40
	published := make([]map[key]row, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for w := range writers {
		published[w] = map[key]row{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sinks.NewPostgresSink(config.PostgresSink{
				DSN: srv.dsn, Table: "metrics_1m_writers", Mode: sinks.PostgresModeUpsert, Key: writerKey,
			})
			if err != nil {
				errs <- err
				return
			}
			defer s.Close()

			rng := rand.New(rand.NewSource(int64(w) + 1))
			for round := range rounds {
				batch := map[key]row{}
				for range 1 + rng.Intn(4) {
					k := key{rng.Intn(minutes), rng.Intn(len(seriesKeys))}
					lo, hi := float64(1+rng.Intn(4))*0.25, float64(5+rng.Intn(4))*0.25
					batch[k] = row{
						sum: float64(1+rng.Intn(40)) * 0.25, count: int64(1 + rng.Intn(9)),
						min: lo, max: hi, last: float64(rng.Intn(9)) * 0.25,
						// Unique across writers and rounds, so which reading is the
						// latest is never a tie.
						lastAt: bucketOf(k).Add(time.Duration(round*writers+w+1) * time.Millisecond),
					}
				}
				if err := flush(s, fmt.Sprintf("w%d", w), batch); err != nil {
					errs <- fmt.Errorf("writer %d round %d: %w", w, round, err)
					return
				}
				for k, r := range batch {
					published[w][k] = r
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	wantMinute := map[key]*row{}
	for _, rows := range published {
		for k, r := range rows {
			if _, ok := wantMinute[k]; !ok {
				wantMinute[k] = &row{}
				merge(wantMinute[k], r, true)
				continue
			}
			merge(wantMinute[k], r, false)
		}
	}
	if len(wantMinute) < minutes*len(seriesKeys)/2 {
		t.Fatalf("only %d keys were written; the writers did not overlap enough to prove anything", len(wantMinute))
	}
	overlapped := 0
	for k := range wantMinute {
		n := 0
		for _, rows := range published {
			if _, ok := rows[k]; ok {
				n++
			}
		}
		if n > 1 {
			overlapped++
		}
	}
	if overlapped == 0 {
		t.Fatal("no key had two writers; nothing was merged")
	}
	t.Logf("%d keys, %d of them published by more than one writer", len(wantMinute), overlapped)

	type got struct {
		sum, min, max float64
		count         int64
		last          *float64
	}
	read := func(sql string) map[string]got {
		out := map[string]got{}
		rows, err := srv.conn.Query(context.Background(), sql)
		assert.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var bucket time.Time
			var series string
			var g got
			assert.NoError(t, rows.Scan(&bucket, &series, &g.sum, &g.count, &g.min, &g.max, &g.last))
			out[bucket.UTC().Format(time.RFC3339)+" "+series] = g
		}
		assert.NoError(t, rows.Err())
		return out
	}

	for _, grain := range []struct {
		table string
		width time.Duration
	}{
		{"metrics_1m", time.Minute}, {"metrics_5m", 5 * time.Minute}, {"metrics_15m", 15 * time.Minute},
		{"metrics_1h", time.Hour}, {"metrics_6h", 6 * time.Hour}, {"metrics_1d", 24 * time.Hour},
	} {
		// Every width divides a day, so truncating to it lands where Postgres's
		// date_bin from a midnight origin does.
		want := map[string]*row{}
		latest := map[string]time.Time{}
		wantTotal := map[string]*row{}
		for k, r := range wantMinute {
			b := bucketOf(k).Truncate(grain.width).Format(time.RFC3339)
			for id, m := range map[string]map[string]*row{b + " " + seriesKeys[k.series]: want, b + " *": wantTotal} {
				if _, ok := m[id]; !ok {
					m[id] = &row{}
					merge(m[id], *r, true)
				} else {
					merge(m[id], *r, false)
				}
			}
			if id := b + " " + seriesKeys[k.series]; bucketOf(k).After(latest[id]) {
				latest[id] = bucketOf(k)
			}
		}
		// A grain's last is its latest minute's last, not the latest last_at
		// within it: the rollups order by bucket. Set once every minute is in.
		for k, r := range wantMinute {
			id := bucketOf(k).Truncate(grain.width).Format(time.RFC3339) + " " + seriesKeys[k.series]
			if bucketOf(k).Equal(latest[id]) {
				want[id].last = r.last
			}
		}

		have := read("SELECT bucket, dimensions_key, value_sum, value_count, value_min, value_max, value_last FROM " + grain.table)
		if len(have) != len(want) {
			t.Fatalf("%s holds %d rows, want %d", grain.table, len(have), len(want))
		}
		for id, w := range want {
			h, ok := have[id]
			if !ok {
				t.Fatalf("%s has no row for %s", grain.table, id)
			}
			if h.sum != w.sum || h.count != w.count || h.min != w.min || h.max != w.max || h.last == nil || *h.last != w.last {
				t.Fatalf("%s %s holds sum=%v count=%d min=%v max=%v last=%v, want sum=%v count=%d min=%v max=%v last=%v",
					grain.table, id, h.sum, h.count, h.min, h.max, h.last, w.sum, w.count, w.min, w.max, w.last)
			}
		}

		if grain.table == "metrics_1m" {
			continue
		}
		total := strings.Replace(grain.table, "metrics_", "metrics_total_", 1)
		haveTotal := read("SELECT bucket, '*', value_sum, value_count, value_min, value_max, NULL::double precision FROM " + total)
		if len(haveTotal) != len(wantTotal) {
			t.Fatalf("%s holds %d rows, want %d", total, len(haveTotal), len(wantTotal))
		}
		for id, w := range wantTotal {
			h := haveTotal[id]
			if h.sum != w.sum || h.count != w.count || h.min != w.min || h.max != w.max {
				t.Fatalf("%s %s holds sum=%v count=%d min=%v max=%v, want sum=%v count=%d min=%v max=%v",
					total, id, h.sum, h.count, h.min, h.max, w.sum, w.count, w.min, w.max)
			}
		}
	}

	// The writers' rows are scaffolding once their minutes are merged. The
	// template's README says deleting them changes no other table, so that a
	// deployer can trim the one table that grows per process.
	before := read("SELECT bucket, dimensions_key, value_sum, value_count, value_min, value_max, value_last FROM metrics_1d")
	exec(t, srv.conn, "DELETE FROM metrics_1m_writers")
	var minuteRows int64
	assert.NoError(t, srv.conn.QueryRow(context.Background(), "SELECT count(*) FROM metrics_1m").Scan(&minuteRows))
	assert.Equal(t, int64(len(wantMinute)), minuteRows)
	after := read("SELECT bucket, dimensions_key, value_sum, value_count, value_min, value_max, value_last FROM metrics_1d")
	assert.Equal(t, len(before), len(after))
	for id, b := range before {
		a := after[id]
		if a.sum != b.sum || a.count != b.count || a.min != b.min || a.max != b.max || *a.last != *b.last {
			t.Fatalf("metrics_1d %s changed when the writers' rows were deleted", id)
		}
	}

	// A series is recorded once however many writers published it.
	var series int64
	assert.NoError(t, srv.conn.QueryRow(context.Background(), "SELECT count(*) FROM series").Scan(&series))
	assert.Equal(t, int64(len(seriesKeys)), series)
}
