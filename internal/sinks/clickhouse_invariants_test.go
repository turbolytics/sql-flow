package sinks

// The clickhouse.* invariants: claims only the ClickHouse sink makes, each
// one a sentence on docs/published/clickhouse.mdx. A test here that fails is
// a page that is now wrong. Fix the page or the sink, never the assertion.

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestSinkClickhouse_DSNSchemeDecidesProtocolTLSAndPort(t *testing.T) {
	coverage.Invariant(t, "clickhouse.dsn.scheme", "sink.clickhouse")

	cases := []struct {
		dsn      string
		protocol clickhouse.Protocol
		tls      bool
		addr     string
	}{
		{dsn: "clickhouses://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "https://h/db", protocol: clickhouse.HTTP, tls: true, addr: "h:8443"},
		{dsn: "clickhouse://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "http://h/db", protocol: clickhouse.HTTP, tls: false, addr: "h:8123"},
		{dsn: "natives://h/db", protocol: clickhouse.Native, tls: true, addr: "h:9440"},
		{dsn: "tcp://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
		{dsn: "native://h/db", protocol: clickhouse.Native, tls: false, addr: "h:9000"},
	}
	for _, c := range cases {
		opts, err := clickhouseOptions(c.dsn)
		assert.NoError(t, err)
		assert.Equal(t, c.protocol, opts.Protocol)
		assert.Equal(t, c.tls, opts.TLS != nil)
		assert.DeepEqual(t, []string{c.addr}, opts.Addr)
	}

	// @ and : in a password parse raw.
	opts, err := clickhouseOptions("clickhouses://u:p@ss:w0rd@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "p@ss:w0rd", opts.Auth.Password)

	// # left raw ends the URL early, so what follows reads as a port.
	_, err = clickhouseOptions("clickhouses://u:pa#ss@h/db")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "invalid port"))

	// Percent-encoded, the same password parses.
	opts, err = clickhouseOptions("clickhouses://u:pa%23ss@h/db")
	assert.NoError(t, err)
	assert.Equal(t, "pa#ss", opts.Auth.Password)
}

func TestSinkClickhouse_TLSVerifiesAgainstTheSystemRoots(t *testing.T) {
	coverage.Invariant(t, "clickhouse.tls.verifies", "sink.clickhouse")

	opts, err := clickhouseOptions("clickhouses://h/db")
	assert.NoError(t, err)
	assert.False(t, opts.TLS.InsecureSkipVerify)
	assert.Nil(t, opts.TLS.RootCAs)

	// No option skips verification: the sink's config is a DSN and a table.
	// A field added here is a page claim to revisit.
	var names []string
	typ := reflect.TypeOf(config.ClickhouseSink{})
	for i := 0; i < typ.NumField(); i++ {
		names = append(names, typ.Field(i).Name)
	}
	assert.DeepEqual(t, []string{"DSN", "Table"}, names)
}

type textCol struct {
	name   string
	values []*string
}

func str(s string) *string { return &s }

// textColumns builds one batch of nullable utf8 columns, in the order given.
func textColumns(t *testing.T, cols ...textCol) arrow.Table {
	t.Helper()
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{
			Name:     c.name,
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		}
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i, c := range cols {
		sb := b.Field(i).(*array.StringBuilder)
		for _, v := range c.values {
			if v == nil {
				sb.AppendNull()
			} else {
				sb.Append(*v)
			}
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	t.Cleanup(tbl.Release)
	return tbl
}

// flushOnce writes one batch and flushes it, which is one delivery.
func flushOnce(t *testing.T, s core.Sink, tbl arrow.Table) error {
	t.Helper()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	return s.Flush(context.Background())
}

func TestIntegrationSinkClickhouse_InsertsByName(t *testing.T) {
	coverage.Invariant(t, "clickhouse.insert.by_name", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		b String, a String, d String DEFAULT 'omitted-default'
	) ENGINE = MergeTree() ORDER BY tuple()`)

	// Produced in the opposite order to the table, and without d.
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "a", values: []*string{str("A")}},
		textCol{name: "b", values: []*string{str("B")}})))
	var a, b, d string
	assert.NoError(t, s.conn.QueryRow(context.Background(),
		"SELECT a, b, d FROM "+s.table).Scan(&a, &b, &d))
	assert.Equal(t, "A", a)
	assert.Equal(t, "B", b)
	assert.Equal(t, "omitted-default", d)

	// A column the table lacks fails the batch, naming it.
	err := flushOnce(t, s, textColumns(t,
		textCol{name: "a", values: []*string{str("A")}},
		textCol{name: "extra", values: []*string{str("X")}}))
	assert.Error(t, err)
	assert.That(t, errs.HasCode(err, errs.CodeSinkWriteFailed))
	assert.That(t, strings.Contains(err.Error(), "column extra is not present in the table"))
}

func TestIntegrationSinkClickhouse_NeverCreatesATable(t *testing.T) {
	coverage.Invariant(t, "clickhouse.table.not_created", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	table := fmt.Sprintf("absent_%d", time.Now().UnixNano())
	s := mustDirectSink(t, sharedClickhouse(t), table)

	err := flushOnce(t, s, textColumns(t, textCol{name: "a", values: []*string{str("A")}}))
	assert.Error(t, err)
	assert.That(t, errs.HasCode(err, errs.CodeSinkWriteFailed))

	var exists uint8
	assert.NoError(t, s.conn.QueryRow(context.Background(),
		"EXISTS TABLE "+table).Scan(&exists))
	assert.Equal(t, uint8(0), exists)
}

func TestIntegrationSinkClickhouse_EnumNullIsZero(t *testing.T) {
	coverage.Invariant(t, "clickhouse.enum.null_is_zero", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	null := textColumns(t, textCol{name: "e", values: []*string{nil}})
	ctx := context.Background()

	// No 0 member: the INSERT succeeds and the row cannot be read.
	noZero := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, noZero, null))
	var n uint64
	assert.NoError(t, noZero.conn.QueryRow(ctx, "SELECT count() FROM "+noZero.table).Scan(&n))
	assert.Equal(t, uint64(1), n)
	var e string
	err := noZero.conn.QueryRow(ctx, "SELECT toString(e) FROM "+noZero.table).Scan(&e)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "UNKNOWN_ELEMENT_OF_ENUM"))
	var raw int8
	assert.NoError(t, noZero.conn.QueryRow(ctx, "SELECT CAST(e AS Int8) FROM "+noZero.table).Scan(&raw))
	assert.Equal(t, int8(0), raw)

	// A 0 member: the NULL reads as that member.
	zero := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Enum8('' = 0, 'a' = 1, 'b' = 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, zero, null))
	assert.NoError(t, zero.conn.QueryRow(ctx, "SELECT toString(e) FROM "+zero.table).Scan(&e))
	assert.Equal(t, "", e)

	// Nullable: the NULL reads as NULL.
	nullable := newLiveClickhouseSink(t,
		`CREATE TABLE %s (e Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, nullable, null))
	var isNull uint8
	assert.NoError(t, nullable.conn.QueryRow(ctx, "SELECT e IS NULL FROM "+nullable.table).Scan(&isNull))
	assert.Equal(t, uint8(1), isNull)
}

func TestIntegrationSinkClickhouse_EncodeFailureIsNotRetried(t *testing.T) {
	coverage.Invariant(t, "clickhouse.encode.fails_once", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (ts DateTime) ENGINE = MergeTree() ORDER BY tuple()`)
	r := newRetrying(s, RetryPolicy{
		MaxAttempts:    4,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		Deadline:       time.Minute,
	})
	retries := 0
	r.onRetry = func(int, error) { retries++ }

	err := flushOnce(t, r, textColumns(t,
		textCol{name: "ts", values: []*string{str("2026-09-01T12:00:00Z")}}))
	assert.Error(t, err)
	assert.That(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
	assert.Equal(t, 0, retries)
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
}

func TestIntegrationSinkClickhouse_TemporalTextTakesTwoShapes(t *testing.T) {
	coverage.Invariant(t, "clickhouse.temporal_text.two_shapes", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		k String, ts DateTime('UTC'), tokyo DateTime('Asia/Tokyo')
	) ENGINE = MergeTree() ORDER BY k`)
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "k", values: []*string{str("plain"), str("offset")}},
		textCol{name: "ts", values: []*string{str("2026-09-01 12:00:00"), str("2026-09-01 12:00:00 +09:00")}},
		textCol{name: "tokyo", values: []*string{str("2026-09-01 12:00:00"), str("2026-09-01 12:00:00 +09:00")}})))

	// The column's zone does not move the instant: the sink parses the text
	// in UTC and sends the instant, so a Tokyo column holds what a UTC one
	// does.
	var moved uint64
	assert.NoError(t, s.conn.QueryRow(ctx,
		"SELECT countIf(toUnixTimestamp(ts) != toUnixTimestamp(tokyo)) FROM "+s.table).Scan(&moved))
	assert.Equal(t, uint64(0), moved)

	got := map[string]string{}
	rows, err := s.conn.Query(ctx, "SELECT k, toString(ts) FROM "+s.table)
	assert.NoError(t, err)
	for rows.Next() {
		var k, ts string
		assert.NoError(t, rows.Scan(&k, &ts))
		got[k] = ts
	}
	assert.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, "2026-09-01 12:00:00", got["plain"])
	assert.Equal(t, "2026-09-01 03:00:00", got["offset"])

	// Any other shape fails before it reaches the server.
	err = flushOnce(t, s, textColumns(t,
		textCol{name: "k", values: []*string{str("iso")}},
		textCol{name: "ts", values: []*string{str("2026-09-01T12:00:00Z")}}))
	assert.Error(t, err)
	assert.That(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
	assert.That(t, strings.Contains(err.Error(),
		`parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05"`))
}

func TestIntegrationSinkClickhouse_DecimalTakesText(t *testing.T) {
	coverage.Invariant(t, "clickhouse.decimal.text_only", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (amount Decimal(10, 2)) ENGINE = MergeTree() ORDER BY tuple()`)
	assert.NoError(t, flushOnce(t, s, textColumns(t,
		textCol{name: "amount", values: []*string{str("12.34")}})))
	var got string
	assert.NoError(t, s.conn.QueryRow(ctx, "SELECT toString(amount) FROM "+s.table).Scan(&got))
	assert.Equal(t, "12.34", got)

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "amount",
		Type: arrow.PrimitiveTypes.Float64,
	}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Float64Builder).Append(12.34)
	rec := b.NewRecord()
	defer rec.Release()
	floats := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer floats.Release()

	// A float is refused: the driver has no exact conversion to Decimal.
	err := flushOnce(t, s, floats)
	assert.Error(t, err)
	assert.That(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
	assert.That(t, strings.Contains(err.Error(),
		"converting float64 to Decimal(10, 2) is unsupported"))
}

func TestIntegrationSinkClickhouse_ReplayDuplicatesUntilMerge(t *testing.T) {
	coverage.Invariant(t, "clickhouse.replay.duplicates_until_merge", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	batch := func() arrow.Table {
		return textColumns(t,
			textCol{name: "id", values: []*string{str("1"), str("2")}},
			textCol{name: "v", values: []*string{str("first"), str("first")}})
	}
	count := func(s *ClickhouseSink, suffix string) uint64 {
		var n uint64
		assert.NoError(t, s.conn.QueryRow(ctx, "SELECT count() FROM "+s.table+suffix).Scan(&n))
		return n
	}

	// A replay into ReplacingMergeTree: both copies until a merge. Merges stop
	// until the count is taken, or a background merge between the two inserts
	// would read 2 and fail with no change to the sink.
	rmt := newLiveClickhouseSink(t, `CREATE TABLE %s (id String, v String) ENGINE = ReplacingMergeTree ORDER BY id`)
	assert.NoError(t, rmt.conn.Exec(ctx, "SYSTEM STOP MERGES "+rmt.table))
	assert.NoError(t, flushOnce(t, rmt, batch()))
	assert.NoError(t, flushOnce(t, rmt, batch()))
	assert.Equal(t, uint64(4), count(rmt, ""))
	assert.Equal(t, uint64(2), count(rmt, " FINAL"))
	assert.NoError(t, rmt.conn.Exec(ctx, "SYSTEM START MERGES "+rmt.table))
	assert.NoError(t, rmt.conn.Exec(ctx, "OPTIMIZE TABLE "+rmt.table+" FINAL"))
	assert.Equal(t, uint64(2), count(rmt, ""))

	// An identical block inside the deduplication window is dropped at insert.
	dedup := newLiveClickhouseSink(t, `CREATE TABLE %s (id String, v String) ENGINE = MergeTree ORDER BY id
		SETTINGS non_replicated_deduplication_window = 100`)
	assert.NoError(t, flushOnce(t, dedup, batch()))
	assert.NoError(t, flushOnce(t, dedup, batch()))
	assert.Equal(t, uint64(2), count(dedup, ""))
}

func TestIntegrationSinkClickhouse_OneFlushIsOneInsert(t *testing.T) {
	coverage.Invariant(t, "clickhouse.flush.one_insert", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (a String) ENGINE = MergeTree ORDER BY tuple()`)
	for _, v := range []string{"x", "y", "z"} {
		assert.NoError(t, s.WriteTable(ctx, textColumns(t, textCol{name: "a", values: []*string{str(v)}})))
	}
	assert.NoError(t, s.Flush(ctx))

	assert.NoError(t, s.conn.Exec(ctx, "SYSTEM FLUSH LOGS"))
	var inserts, rows uint64
	assert.NoError(t, s.conn.QueryRow(ctx, `
		SELECT count(), sum(written_rows) FROM system.query_log
		WHERE type = 'QueryFinish' AND query_kind = 'Insert'
		  AND has(tables, currentDatabase() || '.' || ?)`, s.table).Scan(&inserts, &rows))
	assert.Equal(t, uint64(1), inserts)
	assert.Equal(t, uint64(3), rows)
}

func TestIntegrationSinkClickhouse_WrongPasswordFailsTheProbe(t *testing.T) {
	coverage.Invariant(t, "clickhouse.auth.fails_start", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ctx := context.Background()
	ch := sharedClickhouse(t)
	host, err := ch.Host(ctx)
	assert.NoError(t, err)
	port, err := ch.MappedPort(ctx, "8123/tcp")
	assert.NoError(t, err)

	dsn := fmt.Sprintf("clickhouse://%s:wrong@%s/%s",
		clickhouseUser, net.JoinHostPort(host, port.Port()), clickhouseDatabase)
	s, err := NewClickhouseSink(config.ClickhouseSink{
		DSN:   dsn,
		Table: "never_written",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	// The HTTP status and code vary by server and user: self-hosted 26.8
	// answers this named user 403 and code 516, and Cloud answers its default
	// user 401 and code 194. The message is what every one of them shares.
	err = s.Probe(ctx)
	assert.Error(t, err)
	t.Logf("probe error: %v", err)
	assert.That(t, strings.Contains(err.Error(),
		"Authentication failed: password is incorrect, or there is no user with such name"))
}

// The page's retry sentence: a connection that fails is retried, an INSERT
// the server refused is not, and neither is a value the driver cannot encode.
func TestIntegrationSinkClickhouse_ErrorsClassifyForTheRetryLadder(t *testing.T) {
	coverage.Invariant(t, "sink.error.classifies", "sink.clickhouse")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	t.Parallel()
	ladder := func(s core.Sink) (*retrying, *int) {
		r := newRetrying(s, RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
			Deadline:       time.Minute,
		})
		n := 0
		r.onRetry = func(int, error) { n++ }
		return r, &n
	}
	row := func(col string) arrow.Table {
		return textColumns(t, textCol{name: col, values: []*string{str("x")}})
	}

	// Unreachable: nothing listens on port 1, so every attempt is refused and
	// the ladder runs to its limit.
	dead, err := NewClickhouseSink(config.ClickhouseSink{
		DSN:   "clickhouse://u:p@127.0.0.1:1/db",
		Table: "t",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { dead.Close() })
	r, retries := ladder(dead)
	err = flushOnce(t, r, row("a"))
	assert.Error(t, err)
	assert.Equal(t, 2, *retries)
	assert.That(t, errs.HasCode(err, errs.CodeSinkUnreachable))

	// Rejected: the server answered and refused the INSERT.
	live := newLiveClickhouseSink(t, `CREATE TABLE %s (a String) ENGINE = MergeTree() ORDER BY tuple()`)
	r, retries = ladder(live)
	err = flushOnce(t, r, row("extra"))
	assert.Error(t, err)
	assert.Equal(t, 0, *retries)
	assert.That(t, errs.HasCode(err, errs.CodeSinkWriteFailed))

	// A user fault: the value never reaches the network.
	typed := newLiveClickhouseSink(t, `CREATE TABLE %s (ts DateTime) ENGINE = MergeTree() ORDER BY tuple()`)
	r, retries = ladder(typed)
	err = flushOnce(t, r, textColumns(t,
		textCol{name: "ts", values: []*string{str("2026-09-01T12:00:00Z")}}))
	assert.Error(t, err)
	assert.Equal(t, 0, *retries)
	assert.That(t, errs.HasCode(err, errs.CodeSinkEncodeFailed))
}
