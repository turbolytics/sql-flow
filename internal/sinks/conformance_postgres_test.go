package sinks

// The Postgres sink under the resilience harness and the type runner,
// against a real server behind the fault proxy.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// postgresImage is pinned so a server upgrade is a commit rather than a
// surprise.
const postgresImage = "postgres:16"

const (
	postgresUser     = "sqlflow"
	postgresPassword = "sqlflow"
	postgresDatabase = "sqlflow"
)

// postgresServer is one container on a network, with a direct connection
// for DDL and read-back that never crosses the fault proxy.
type postgresServer struct {
	container *tcpostgres.PostgresContainer
	network   *testcontainers.DockerNetwork
	direct    *pgx.Conn
}

func startPostgres(t *testing.T) *postgresServer {
	t.Helper()
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("docker network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	pg, err := tcpostgres.Run(ctx, postgresImage,
		network.WithNetwork([]string{"postgres"}, nw),
		tcpostgres.WithDatabase(postgresDatabase),
		tcpostgres.WithUsername(postgresUser),
		tcpostgres.WithPassword(postgresPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	direct, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = direct.Close(context.Background()) })
	// Every read-back renders on a UTC session, so a timestamptz expect is
	// one string whatever the host's zone.
	_, err = direct.Exec(ctx, "SET TIME ZONE 'UTC'")
	assert.NoError(t, err)

	return &postgresServer{container: pg, network: nw, direct: direct}
}

// directDSN addresses the server on its host port, past the proxy.
func (s *postgresServer) directDSN(t *testing.T) string {
	t.Helper()
	dsn, err := s.container.ConnectionString(context.Background(), "sslmode=disable")
	assert.NoError(t, err)
	return dsn
}

// proxyDSN addresses the server through the fault proxy.
func (s *postgresServer) proxyDSN(t *testing.T) (string, *conformance.Proxy) {
	proxy := conformance.NewProxy(t, s.network, "postgres:5432")
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		postgresUser, postgresPassword, proxy.Addr, postgresDatabase), proxy
}

func idTable(t *testing.T, id int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func TestIntegrationSinkPostgres_Conformance(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()
	srv := startPostgres(t)
	dsn, proxy := srv.proxyDSN(t)

	table := fmt.Sprintf("conformance_%d", time.Now().UnixNano())
	_, err := srv.direct.Exec(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY)")
	assert.NoError(t, err)

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.postgres",

		New: func(t *testing.T) core.Sink {
			s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeUpsert, Key: []string{"id"}})
			assert.NoError(t, err)
			t.Cleanup(func() { s.Close() })
			return s
		},

		Break: proxy.Break,
		Heal:  proxy.Heal,

		// ctid order is insertion order on a table nothing has vacuumed,
		// which is what preserves_order has to observe.
		ReadBack: func(t *testing.T) []conformance.Row {
			rows, err := srv.direct.Query(context.Background(), "SELECT id FROM "+table+" ORDER BY ctid")
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

		Table:           idTable,
		OrderedReadBack: true,
	})
}

func TestIntegrationSinkPostgres_Types(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn := srv.directDSN(t)

	declared, err := coverage.TypesFor("sink.postgres")
	assert.NoError(t, err)
	nulls, err := coverage.NullsFor("sink.postgres")
	assert.NoError(t, err)
	elemNulls, err := coverage.NullElementsFor("sink.postgres")
	assert.NoError(t, err)

	// The runner prepares one destination per row and reads it back through
	// the direct connection, so each sink can close as the next row opens
	// its own. Holding them all to the end of the test ran the server out
	// of connections (53300) after about a hundred rows.
	var previous *PostgresSink
	t.Cleanup(func() {
		if previous != nil {
			previous.Close()
		}
	})

	conformance.Types(t, conformance.TypeSubject{
		Integration:      "sink.postgres",
		Declared:         declared,
		Nulls:            nulls,
		ListElementNulls: elemNulls,

		Prepare: func(t *testing.T, key, columnType string) conformance.TypeDestination {
			// An unsupported row fails before any statement is built, so
			// its column type never matters.
			if columnType == "" {
				columnType = "bigint"
			}
			table := fmt.Sprintf("t_%d", time.Now().UnixNano())
			_, err := srv.direct.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (v %s)", table, columnType))
			assert.NoError(t, err)

			if previous != nil {
				previous.Close()
			}
			sink, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeAppend})
			assert.NoError(t, err)
			previous = sink

			return conformance.TypeDestination{
				Sink: sink,
				ReadBack: func(t *testing.T) (any, error) {
					var rendered *string
					err := srv.direct.QueryRow(context.Background(), "SELECT v::text FROM "+table+" LIMIT 1").Scan(&rendered)
					if err != nil {
						return nil, err
					}
					if rendered == nil {
						return nil, nil
					}
					return *rendered, nil
				},
				// The runner writes [value, null, value] into a json column.
				ReadBackNullElement: func(t *testing.T) (bool, error) {
					var isNull bool
					err := srv.direct.QueryRow(context.Background(), "SELECT json_typeof(v -> 1) = 'null' FROM "+table+" LIMIT 1").Scan(&isNull)
					return isNull, err
				},
			}
		},
	})
}
