package sinks

// The ClickHouse sink under the type runner, against a real server.
//
// This is the first type table, so it is also the proof that the runner needs
// nothing sink-specific. Everything it asks for is here: a destination with
// one column of a given type, a sink writing to it, and a read-back that does
// not go through the sink.
//
// One table per row, not one wide table. type.undeclared.fails_loud requires
// the batch to fail, and a shared table would let one unsupported column
// destroy every other row's evidence.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestIntegrationSinkClickhouse_Types(t *testing.T) {
	coverage.Covers(t, "sink.clickhouse")

	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()

	ch, err := tcclickhouse.Run(ctx, clickhouseImage,
		testcontainers.WithExposedPorts("8123/tcp"),
		tcclickhouse.WithUsername(clickhouseUser),
		tcclickhouse.WithPassword(clickhousePassword),
		tcclickhouse.WithDatabase(clickhouseDatabase),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Terminate(context.Background()) })

	declared, err := coverage.TypesFor("sink.clickhouse")
	assert.NoError(t, err)
	nulls, err := coverage.NullsFor("sink.clickhouse")
	assert.NoError(t, err)
	elemNulls, err := coverage.NullElementsFor("sink.clickhouse")
	assert.NoError(t, err)

	conformance.Types(t, conformance.TypeSubject{
		Integration:      "sink.clickhouse",
		Declared:         declared,
		Nulls:            nulls,
		ListElementNulls: elemNulls,

		Prepare: func(t *testing.T, key, columnType string) conformance.TypeDestination {
			// An unsupported row fails before any INSERT is built, so the
			// column type it gets never matters. Int64 is the cheapest DDL
			// that always parses.
			if columnType == "" {
				columnType = "Int64"
			}

			table := fmt.Sprintf("t_%d", time.Now().UnixNano())
			sink := mustDirectSink(t, ch, table)
			assert.NoError(t, sink.conn.Exec(context.Background(), fmt.Sprintf(
				"CREATE TABLE %s (v %s) ENGINE = MergeTree() ORDER BY tuple()",
				table, columnType)))

			return conformance.TypeDestination{
				Sink: core.Sink(sink),

				// Rendered by the server rather than decoded by the driver.
				// The runner compares presence, not shape, and toString gives
				// one scannable answer for every column type there is.
				ReadBack: func(t *testing.T) (any, error) {
					rows, err := sink.conn.Query(context.Background(),
						"SELECT v IS NULL, toString(v) FROM "+table+" LIMIT 1")
					if err != nil {
						return nil, err
					}
					defer rows.Close()

					if !rows.Next() {
						return nil, rows.Err()
					}
					var isNull uint8
					var rendered string
					if err := rows.Scan(&isNull, &rendered); err != nil {
						return nil, err
					}
					if err := rows.Err(); err != nil {
						return nil, err
					}
					if isNull == 1 {
						return nil, nil
					}
					return rendered, nil
				},

				// ClickHouse arrays are 1-indexed, so v[2] is the null the
				// runner wrote between two values. Asking the server is the
				// only way to tell [1, 0, 3] from [1, NULL, 3]: the list
				// itself is not null in either case.
				ReadBackNullElement: func(t *testing.T) (bool, error) {
					rows, err := sink.conn.Query(context.Background(),
						"SELECT v[2] IS NULL FROM "+table+" LIMIT 1")
					if err != nil {
						return false, err
					}
					defer rows.Close()

					if !rows.Next() {
						return false, rows.Err()
					}
					var isNull uint8
					if err := rows.Scan(&isNull); err != nil {
						return false, err
					}
					return isNull == 1, rows.Err()
				},
			}
		},
	})
}
