package handlers

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// benchMessages builds a batch of messages shaped like the benchmark fixture
// data: a few scalar fields plus a nested properties object.
func benchMessages(n int) [][]byte {
	cities := []string{"New York", "San Francisco", "Los Angeles", "Chicago", "Austin"}
	msgs := make([][]byte, n)
	for i := 0; i < n; i++ {
		msgs[i] = []byte(fmt.Sprintf(
			`{"ip":"1.2.3.4","event":"click","userId":"user-%d","timestamp":"2024-01-01T00:00:00Z","type":"track","properties":{"city":"%s"}}`,
			i, cities[i%len(cities)],
		))
	}
	return msgs
}

const benchSQL = `SELECT properties.city as city, COUNT(*) as count FROM %s GROUP BY properties.city`

func benchmarkHandler(b *testing.B, batchSize int, newHandler func(b *testing.B) (handlerUnderTest, func())) {
	msgs := benchMessages(batchSize)
	h, cleanup := newHandler(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(int64(len(msgs) * len(msgs[0])))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := h.Init(ctx); err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := h.Write(m); err != nil {
				b.Fatal(err)
			}
		}
		table, err := h.Invoke(ctx)
		if err != nil {
			b.Fatal(err)
		}
		table.Release()
	}
	b.StopTimer()

	b.ReportMetric(float64(batchSize)*float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

type handlerUnderTest interface {
	Init(ctx context.Context) error
	Write([]byte) error
	Invoke(ctx context.Context) (arrow.Table, error)
}

func BenchmarkInferredMemBatch(b *testing.B) {
	for _, size := range []int{500, 5000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			benchmarkHandler(b, size, func(b *testing.B) (handlerUnderTest, func()) {
				conn, cleanup := newBenchADBCConn(b)
				h, err := NewInferredMemBatchHandler(conn, fmt.Sprintf(benchSQL, "batch"))
				if err != nil {
					b.Fatal(err)
				}
				return h, cleanup
			})
		})
	}
}

func BenchmarkStructuredBatch(b *testing.B) {
	for _, size := range []int{500, 5000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			benchmarkHandler(b, size, func(b *testing.B) (handlerUnderTest, func()) {
				conn, cleanup := newBenchADBCConn(b)

				stmt, err := conn.NewStatement()
				if err != nil {
					b.Fatal(err)
				}
				if err := stmt.SetSqlQuery(`CREATE TABLE source (event STRING, properties STRUCT(city TEXT));`); err != nil {
					b.Fatal(err)
				}
				if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
					b.Fatal(err)
				}
				stmt.Close()

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "event", Type: arrow.BinaryTypes.String},
					{Name: "properties", Type: arrow.StructOf(
						arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
					)},
				}, nil)

				h, err := NewStructuredBatchHandler(conn, fmt.Sprintf(benchSQL, "source"), "source", schema)
				if err != nil {
					b.Fatal(err)
				}
				return h, cleanup
			})
		})
	}
}
