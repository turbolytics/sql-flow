package sinks

// The #290 adversarial review, as tests. Each one is a fault an ordinary
// network, pooler or schema produces, run against the sink the way run
// builds it: through New, with the retry ladder and the probe.

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// stallProxy forwards TCP to Postgres. Frozen, it holds every byte and keeps
// both sockets open, which is what a partition or a hung server looks like
// from the client: nothing fails, nothing answers. drop closes every socket,
// with a FIN or, for rst, a reset.
type stallProxy struct {
	ln     net.Listener
	target string
	frozen atomic.Bool
	mu     sync.Mutex
	conns  []*net.TCPConn
}

func newStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c.(*net.TCPConn))
		}
	}()
	t.Cleanup(func() {
		p.frozen.Store(false)
		ln.Close()
		p.drop(false)
	})
	return p
}

func (p *stallProxy) handle(c *net.TCPConn) {
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, c, up.(*net.TCPConn))
	p.mu.Unlock()
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if err != nil {
				dst.Close()
				return
			}
			for p.frozen.Load() {
				time.Sleep(10 * time.Millisecond)
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				src.Close()
				return
			}
		}
	}
	go pipe(up, c)
	go pipe(c, up)
}

func (p *stallProxy) drop(rst bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		if rst {
			_ = c.SetLinger(0)
		}
		_ = c.Close()
	}
	p.conns = nil
}

// dsnVia rewrites a DSN to reach Postgres through the proxy.
func (p *stallProxy) dsnVia(t *testing.T, dsn string) string {
	t.Helper()
	cc, err := pgx.ParseConfig(dsn)
	assert.NoError(t, err)
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cc.User, cc.Password, p.ln.Addr(), cc.Database)
}

// target is the host and port a DSN names.
func target(t *testing.T, dsn string) string {
	t.Helper()
	cc, err := pgx.ParseConfig(dsn)
	assert.NoError(t, err)
	return net.JoinHostPort(cc.Host, fmt.Sprint(cc.Port))
}

// deadline3s is a retry block small enough that a test waits it out.
var deadline3s = &config.SinkRetry{DeadlineSeconds: 3}

func newRunSink(t *testing.T, dsn, table, mode string, key []string, retry *config.SinkRetry) (core.Sink, error) {
	t.Helper()
	s, err := New(context.Background(), config.Sink{Type: "postgres", Retry: retry, Postgres: &config.PostgresSink{
		DSN: dsn, Table: table, Mode: mode, Key: key,
	}}, nil)
	return s, err
}

func mustRunSink(t *testing.T, dsn, table, mode string, key []string, retry *config.SinkRetry) core.Sink {
	t.Helper()
	s, err := newRunSink(t, dsn, table, mode, key, retry)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return s
}

func countRows(t *testing.T, srv *postgresServer, table string) int64 {
	t.Helper()
	var n int64
	assert.NoError(t, srv.direct.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n))
	return n
}

// 1. A Postgres that holds packets and keeps the socket open. A flush must
// give up inside the retry deadline, coded unreachable, rather than block
// the window's poll forever while the window table grows. Then, with the
// server answering again, the next flush redials and delivers.
func TestIntegrationSinkPostgres_AStalledServerFailsTheFlushInsideTheDeadline(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	p := newStallProxy(t, target(t, srv.directDSN(t)))
	s := mustRunSink(t, p.dsnVia(t, srv.directDSN(t)), table, PostgresModeUpsert, []string{"k"}, deadline3s)

	assert.NoError(t, flushSink(t, s, kvTable(1, 1)))

	p.frozen.Store(true)
	tbl := kvTable(2, 2)
	assert.NoError(t, s.WriteTable(ctx, tbl))
	tbl.Release()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- s.Flush(ctx) }()
	select {
	case err := <-done:
		assert.Error(t, err)
		assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
		assert.That(t, time.Since(start) < 3*time.Second+2*time.Second)
	case <-time.After(20 * time.Second):
		t.Fatalf("a flush into a stalled server was still blocked after %s, with a 3s retry deadline", time.Since(start).Round(time.Second))
	}

	p.frozen.Store(false)
	assert.NoError(t, s.Flush(ctx))
	assert.Equal(t, int64(2), countRows(t, srv, table))
}

// 1, at startup. A probe against a server that accepts and never answers
// fails the start inside the deadline, coded unreachable, exit 12.
func TestIntegrationSinkPostgres_AStalledServerFailsTheProbeInsideTheDeadline(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	p := newStallProxy(t, target(t, srv.directDSN(t)))
	p.frozen.Store(true)

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := newRunSink(t, p.dsnVia(t, srv.directDSN(t)), table, PostgresModeUpsert, []string{"k"}, deadline3s)
		done <- err
	}()
	select {
	case err := <-done:
		assert.Error(t, err)
		assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
		assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
		assert.That(t, time.Since(start) < 3*time.Second+2*time.Second)
	case <-time.After(20 * time.Second):
		t.Fatalf("the probe against a stalled server was still blocked after %s", time.Since(start).Round(time.Second))
	}
}

// 2. A pooler or load balancer closes an idle connection between flushes,
// with a FIN or a reset and no Postgres error. The next flush retries,
// redials and delivers; the pipeline does not stop.
func TestIntegrationSinkPostgres_AConnectionClosedBetweenFlushesIsRetried(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	for _, rst := range []bool{false, true} {
		t.Run(fmt.Sprintf("rst=%v", rst), func(t *testing.T) {
			table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
			p := newStallProxy(t, target(t, srv.directDSN(t)))
			s := mustRunSink(t, p.dsnVia(t, srv.directDSN(t)), table, PostgresModeUpsert, []string{"k"}, nil)

			assert.NoError(t, flushSink(t, s, kvTable(1, 1)))
			p.drop(rst)
			time.Sleep(200 * time.Millisecond)
			if err := flushSink(t, s, kvTable(2, 2)); err != nil {
				t.Fatalf("flush after the connection closed: code=%s err=%v", errs.CodeOf(err), err)
			}
			assert.Equal(t, int64(2), countRows(t, srv, table))
		})
	}
}

// 3. A user's table named sqlflow_staging on the search path survives. The
// staging table is qualified with pg_temp everywhere, so a fresh session with
// no temp schema cannot resolve the name to someone else's table.
func TestIntegrationSinkPostgres_AUserTableNamedLikeStagingSurvives(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	for _, stmt := range []string{
		"CREATE TABLE public.sqlflow_staging (x int)",
		"INSERT INTO public.sqlflow_staging VALUES (42)",
	} {
		_, err := srv.direct.Exec(ctx, stmt)
		assert.NoError(t, err)
	}
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := mustRunSink(t, srv.directDSN(t), table, PostgresModeUpsert, []string{"k"}, nil)

	assert.NoError(t, flushSink(t, s, kvTable(1, 1)))
	assert.Equal(t, int64(1), countRows(t, srv, "public.sqlflow_staging"))
	assert.Equal(t, int64(1), countRows(t, srv, table))
}

// 4. A unique index treats NULLs as distinct, so ON CONFLICT never matches a
// null key and a redelivery inserts it again. The probe refuses a nullable
// key column at startup, which keeps idempotent_on_key true for every table
// the sink accepts.
func TestIntegrationSinkPostgres_ANullableKeyIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	table := newKVTable(t, srv, "CREATE TABLE %[1]s (k text, v int, UNIQUE (k))")
	_, err := newRunSink(t, srv.directDSN(t), table, PostgresModeUpsert, []string{"k"}, nil)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), `key column "k" is nullable`))
}

// 9. A deferrable unique constraint and an invalid index both look unique in
// pg_index, and ON CONFLICT refuses both: 55000 and 42P10 at the first flush.
// The probe refuses them at startup instead.
func TestIntegrationSinkPostgres_DeferrableAndInvalidIndexesAreRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	deferrable := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint NOT NULL, v int, CONSTRAINT %[1]s_u UNIQUE (k) DEFERRABLE INITIALLY IMMEDIATE)")

	invalid := newKVTable(t, srv, "CREATE TABLE %[1]s (k bigint NOT NULL, v int); INSERT INTO %[1]s VALUES (1, 1), (1, 2)")
	// CONCURRENTLY cannot run in a transaction, and fails on the duplicate,
	// leaving the index behind marked invalid.
	_, err := srv.direct.Exec(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY %[1]s_k ON %[1]s (k)", invalid))
	assert.Error(t, err)
	_, err = srv.direct.Exec(ctx, "DELETE FROM "+invalid)
	assert.NoError(t, err)

	for _, table := range []string{deferrable, invalid} {
		_, err := newRunSink(t, srv.directDSN(t), table, PostgresModeUpsert, []string{"k"}, nil)
		assert.Error(t, err)
		assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
		assert.That(t, strings.Contains(err.Error(), "no unique index or constraint covers exactly (k)"))
	}
}

// flushSink writes one table through a sink built by New and flushes it.
func flushSink(t *testing.T, s core.Sink, tbl arrow.Table) error {
	t.Helper()
	defer tbl.Release()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	return s.Flush(context.Background())
}
