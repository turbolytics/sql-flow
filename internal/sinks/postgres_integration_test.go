package sinks

// The Postgres sink's own promises, against a real server. The conformance
// harness proves the contract every sink shares; these prove what this sink
// adds: last row in a batch wins, a retry applies buffered batches in order,
// omitted columns take defaults and keep values, the probe reads the
// catalog, and a refused value is the user's.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func kvTable(pairs ...int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.PrimitiveTypes.Int64},
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i := 0; i < len(pairs); i += 2 {
		b.Field(0).(*array.Int64Builder).Append(pairs[i])
		b.Field(1).(*array.Int64Builder).Append(pairs[i+1])
	}
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func readKV(t *testing.T, srv *postgresServer, table string) map[int64]int64 {
	t.Helper()
	rows, err := srv.direct.Query(context.Background(), "SELECT k, v FROM "+table)
	assert.NoError(t, err)
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var k, v int64
		assert.NoError(t, rows.Scan(&k, &v))
		out[k] = v
	}
	assert.NoError(t, rows.Err())
	return out
}

// newKVTable creates a table from ddl, where %[1]s is the table's name.
func newKVTable(t *testing.T, srv *postgresServer, ddl string) string {
	t.Helper()
	table := fmt.Sprintf("kv_%d", time.Now().UnixNano())
	for _, stmt := range strings.Split(fmt.Sprintf(ddl, table), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := srv.direct.Exec(context.Background(), stmt)
		assert.NoError(t, err)
	}
	return table
}

func newKeyedSink(t *testing.T, dsn, table string, key ...string) *PostgresSink {
	t.Helper()
	s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeUpsert, Key: key})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func flushTable(t *testing.T, s *PostgresSink, tbl arrow.Table) error {
	t.Helper()
	defer tbl.Release()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	return s.Flush(context.Background())
}

// Two rows with one key in a batch: the last one in the batch wins, and
// nothing fails. The extension kept one of the two without saying which;
// Postgres alone would refuse the statement with 21000.
func TestIntegrationSinkPostgres_LastRowInABatchWins(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := newKeyedSink(t, srv.directDSN(t), table, "k")

	assert.NoError(t, flushTable(t, s, kvTable(1, 10, 2, 20, 1, 30)))
	assert.DeepEqual(t, map[int64]int64{1: 30, 2: 20}, readKV(t, srv, table))
}

// A failed flush leaves two batches buffered that share a key. The retry
// applies them one at a time, in order, so the table holds the second
// batch's value. Merged in one statement, the two rows for k=1 would fail
// with 21000, which the first attempt would have applied.
func TestIntegrationSinkPostgres_RetryAppliesBufferedBatchesInOrder(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn, proxy := srv.proxyDSN(t)
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := newKeyedSink(t, dsn, table, "k")
	ctx := context.Background()

	first := kvTable(1, 1)
	defer first.Release()
	assert.NoError(t, s.WriteTable(ctx, first))
	proxy.Break(t)
	broken, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.Flush(broken)
	cancel()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, 1, s.BufferedRows())

	second := kvTable(1, 2)
	defer second.Release()
	assert.NoError(t, s.WriteTable(ctx, second))
	assert.Equal(t, 2, s.BufferedRows())

	proxy.Heal(t)
	assert.NoError(t, s.Flush(ctx))
	assert.Equal(t, 0, s.BufferedRows())
	assert.DeepEqual(t, map[int64]int64{1: 2}, readKV(t, srv, table))
}

// The INSERT names the batch's columns and no others, so a column the batch
// omits takes its default on insert and keeps its value on update. That is
// footgun 3 in #268, closed: the extension sent the omitted column as NULL.
func TestIntegrationSinkPostgres_OmittedColumnsTakeDefaultsAndKeepValues(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL, stamp timestamptz NOT NULL DEFAULT clock_timestamp())")
	s := newKeyedSink(t, srv.directDSN(t), table, "k")

	assert.NoError(t, flushTable(t, s, kvTable(1, 1)))
	var first time.Time
	assert.NoError(t, srv.direct.QueryRow(ctx, "SELECT stamp FROM "+table+" WHERE k = 1").Scan(&first))

	assert.NoError(t, flushTable(t, s, kvTable(1, 2)))
	var second time.Time
	var v int64
	assert.NoError(t, srv.direct.QueryRow(ctx, "SELECT stamp, v FROM "+table+" WHERE k = 1").Scan(&second, &v))
	assert.Equal(t, int64(2), v)
	assert.That(t, second.Equal(first))
}

// The probe reads the catalog. A missing table, a key column the table
// lacks, and a key with no exact unique index are the user's to fix, and
// exit 10. A unique index is a conflict target in any column order; a
// partial unique index is not, because ON CONFLICT (k) will not infer it.
// append onto a table with a unique index starts, with a warning.
func TestIntegrationSinkPostgres_ProbeChecksTheTarget(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	dsn := srv.directDSN(t)

	probe := func(mode, table string, key ...string) (*PostgresSink, error) {
		s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: mode, Key: key})
		assert.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s, s.Probe(ctx)
	}
	userError := func(err error) {
		t.Helper()
		assert.Error(t, err)
		assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
		assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	}

	_, err := probe(PostgresModeUpsert, "no_such_table", "k")
	userError(err)
	assert.That(t, strings.Contains(err.Error(), "does not exist"))

	pk := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	_, err = probe(PostgresModeUpsert, pk, "k")
	assert.NoError(t, err)
	_, err = probe(PostgresModeUpsert, pk, "nope")
	userError(err)
	_, err = probe(PostgresModeUpsert, pk, "v")
	userError(err)
	assert.That(t, strings.Contains(err.Error(), "no unique index or constraint covers exactly (v)"))

	uix := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint NOT NULL, v bigint, w bigint NOT NULL); CREATE UNIQUE INDEX ON %[1]s (w, k)")
	_, err = probe(PostgresModeUpsert, uix, "k", "w")
	assert.NoError(t, err)

	partial := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint NOT NULL, v bigint); CREATE UNIQUE INDEX ON %[1]s (k) WHERE v > 0")
	_, err = probe(PostgresModeUpsert, partial, "k")
	userError(err)

	a, err := probe(PostgresModeAppend, pk)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(a.Warnings()))
	assert.That(t, strings.Contains(a.Warnings()[0], "at-least-once"))

	plain := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint, v bigint)")
	p, err := probe(PostgresModeAppend, plain)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(p.Warnings()))
}

// A value the column refuses is the user's: exit 10, never retried, and the
// batch stays buffered so the depth gauge shows what is owed. A NOT NULL
// column the batch omits is refused at the merge.
func TestIntegrationSinkPostgres_ARefusedValueIsAUserError(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn := srv.directDSN(t)

	small := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v smallint NOT NULL)")
	s := newKeyedSink(t, dsn, small, "k")
	err := flushTable(t, s, kvTable(1, 1<<40))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, !retryable(err))
	assert.Equal(t, 1, s.BufferedRows())

	notNull := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL, w bigint NOT NULL)")
	s2 := newKeyedSink(t, dsn, notNull, "k")
	err = flushTable(t, s2, kvTable(1, 1))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "23502"))
	assert.That(t, strings.Contains(err.Error(), notNull))
}

// A connection the server closes is replaced on the next flush, and the
// staging table with it: a temp table dies with its session.
func TestIntegrationSinkPostgres_ARedialRecreatesTheStagingTable(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := newKeyedSink(t, srv.directDSN(t), table, "k")

	assert.NoError(t, flushTable(t, s, kvTable(1, 1)))
	_, err := srv.direct.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")
	assert.NoError(t, err)

	// The first flush after the kill finds the connection dead and fails as
	// unreachable, keeping the batch; the next one redials and delivers.
	tbl := kvTable(2, 2)
	defer tbl.Release()
	assert.NoError(t, s.WriteTable(ctx, tbl))
	if err := s.Flush(ctx); err != nil {
		assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
		assert.NoError(t, s.Flush(ctx))
	}
	assert.DeepEqual(t, map[int64]int64{1: 1, 2: 2}, readKV(t, srv, table))
}
