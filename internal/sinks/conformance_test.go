package sinks

// The ClickHouse sink under the conformance harness, against a real server
// behind toxiproxy.
//
// This is the first subject, so it is also the proof that the harness needs
// nothing sink-specific. Everything it asks for is here: build the sink,
// break and heal its network, read back what was delivered, and produce a
// one-row table the DDL accepts. A second sink supplies the same five things
// and inherits both invariants.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// clickhouseImage is pinned so a server upgrade is a commit rather than a
// Tuesday. 26.8 is the self-hosted version the ClickHouse integration page
// was verified against.
const clickhouseImage = "clickhouse/clickhouse-server:26.8"

// The credentials the container is started with, stated here rather than
// taken from the module's defaults so the dsn below and the container cannot
// disagree.
const (
	clickhouseUser     = "sqlflow"
	clickhousePassword = "sqlflow"
	clickhouseDatabase = "sqlflow"
)

// clickhouseDSN addresses a ClickHouse HTTP endpoint as the sink's dsn parser
// reads it: a clickhouse:// port is an HTTP port.
func clickhouseDSN(addr string) string {
	return fmt.Sprintf("clickhouse://%s:%s@%s/%s",
		clickhouseUser, clickhousePassword, addr, clickhouseDatabase)
}

func TestIntegrationSinkClickhouse_Conformance(t *testing.T) {
	// Before the skip: the unit pass never reaches the harness, and a test
	// that emits nothing there reads as covering no feature at all.
	coverage.Covers(t, "sink.clickhouse")

	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("docker network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	ch, err := tcclickhouse.Run(ctx, clickhouseImage,
		network.WithNetwork([]string{"clickhouse"}, nw),
		testcontainers.WithExposedPorts("8123/tcp"),
		tcclickhouse.WithUsername(clickhouseUser),
		tcclickhouse.WithPassword(clickhousePassword),
		tcclickhouse.WithDatabase(clickhouseDatabase),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Terminate(context.Background()) })

	// The sink speaks the HTTP interface on 8123, and the proxy forwards to
	// the container's alias on the shared network rather than to a host port.
	proxy := conformance.NewProxy(t, nw, "clickhouse:8123")

	table := fmt.Sprintf("conformance_%d", time.Now().UnixNano())

	// A second connection, straight to the container, for DDL and read-back.
	// The harness's own reads must not cross the fault it injects, or a
	// broken destination would look like an empty one.
	direct := mustDirectSink(t, ch, table)
	// Log rather than MergeTree, because the read-back has to observe the order
	// rows arrived in. MergeTree stores a part sorted by its ORDER BY key, so
	// on a MergeTree table a sink that delivered [3, 2, 1] reads back [1, 2, 3]
	// and satisfies preserves_order without preserving anything. An arrival
	// timestamp does not rescue it: now64() can resolve to the same value for
	// every row of one insert block, and the sink flushes a batch as one block.
	//
	// Log appends and reads sequentially, so insertion order survives. The sink
	// builds INSERT INTO <table> (<columns>) from the Arrow schema and does not
	// care which engine backs the table.
	assert.NoError(t, direct.conn.Exec(ctx,
		"CREATE TABLE "+table+" (id Int64) ENGINE = Log"))

	dsn := clickhouseDSN(proxy.Addr)

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.clickhouse",

		New: func(t *testing.T) core.Sink {
			s, err := NewClickhouseSink(config.ClickhouseSink{DSN: dsn, Table: table})
			assert.NoError(t, err)
			t.Cleanup(func() { s.Close() })
			return s
		},

		Break: proxy.Break,
		Heal:  proxy.Heal,

		ReadBack: func(t *testing.T) []conformance.Row {
			// No ORDER BY: the Log engine returns rows in the order they were
			// inserted, which is the order this read-back has to report.
			rows, err := direct.conn.Query(context.Background(),
				"SELECT id FROM "+table)
			assert.NoError(t, err)
			defer rows.Close()

			var out []conformance.Row
			for rows.Next() {
				var id int64
				assert.NoError(t, rows.Scan(&id))
				out = append(out, conformance.Row{"id": id})
			}
			assert.NoError(t, rows.Err())
			return out
		},

		Table: func(t *testing.T, id int64) arrow.Table {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			}, nil)

			b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			defer b.Release()
			b.Field(0).(*array.Int64Builder).Append(id)

			rec := b.NewRecord()
			defer rec.Release()
			return array.NewTableFromRecords(schema, []arrow.Record{rec})
		},
	})
}

// mustDirectSink opens a sink on the container's own host port, bypassing the
// proxy.
//
// The module's ConnectionString reports the native port, and the sink's dsn
// parser reads a clickhouse:// port as HTTP, so the mapped 8123 is looked up
// rather than taken from it.
func mustDirectSink(t *testing.T, ch *tcclickhouse.ClickHouseContainer, table string) *ClickhouseSink {
	t.Helper()
	ctx := context.Background()

	host, err := ch.Host(ctx)
	assert.NoError(t, err)
	port, err := ch.MappedPort(ctx, "8123/tcp")
	assert.NoError(t, err)

	s, err := NewClickhouseSink(config.ClickhouseSink{
		DSN:   clickhouseDSN(net.JoinHostPort(host, port.Port())),
		Table: table,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	assert.NoError(t, s.conn.Ping(ctx))
	return s
}
