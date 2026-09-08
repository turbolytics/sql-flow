package sinks

// The two sinks that need no container: console writes to an io.Writer, and
// sqlcommand writes through the pipeline's own DuckDB connection. Both run
// the same harness the ClickHouse subject does, at unit level.
//
// Both were exempt from sink.flush.keeps_batch until this commit, on the
// grounds that nothing crosses a network. That is the argument for skipping a
// retry ladder. The invariant is about whether the pipeline may commit
// offsets for rows that never landed, and it applies to any sink that can
// fail at all.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// breakableWriter is stdout redirected somewhere that can fail: a full disk,
// a closed pipe, a detached terminal.
type breakableWriter struct {
	mu     sync.Mutex
	broken bool
	out    []byte
}

func (w *breakableWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken {
		return 0, errors.New("write: no space left on device")
	}
	w.out = append(w.out, p...)
	return len(p), nil
}

func (w *breakableWriter) set(broken bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.broken = broken
}

func (w *breakableWriter) rows(t *testing.T) []conformance.Row {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []conformance.Row
	dec := json.NewDecoder(bytes.NewReader(w.out))
	for {
		var row map[string]any
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("console sink wrote something that is not JSON: %v", err)
		}
		// Every value decodes as float64; the harness compares int64 ids.
		out = append(out, conformance.Row{"id": int64(row["id"].(float64))})
	}
	return out
}

func TestSinkConsole_Conformance(t *testing.T) {
	w := &breakableWriter{}

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.console",
		New:         func(*testing.T) core.Sink { return NewConsoleSinkTo(w) },
		Break:       func(*testing.T) { w.set(true) },
		Heal:        func(*testing.T) { w.set(false) },
		ReadBack:    func(t *testing.T) []conformance.Row { return w.rows(t) },
		Table:       func(t *testing.T, id int64) arrow.Table { return oneRowTable(t, id) },
	})
}

func TestSinkSqlcommand_Conformance(t *testing.T) {
	conn := newSinkTestConn(t)
	stamp := time.Now().UnixNano()
	target := fmt.Sprintf("conformance_target_%d", stamp)
	gate := fmt.Sprintf("conformance_gate_%d", stamp)
	seq := fmt.Sprintf("conformance_seq_%d", stamp)

	// arrived orders the read-back by when a row landed rather than by its id.
	// ORDER BY id sorts, so a sink that delivered [3, 2, 1] would read back
	// [1, 2, 3] and satisfy preserves_order without preserving anything.
	exec(t, conn, "CREATE SEQUENCE "+seq+" START 1")
	exec(t, conn, "CREATE TABLE "+target+
		" (id BIGINT, arrived BIGINT DEFAULT nextval('"+seq+"'))")
	exec(t, conn, "CREATE TABLE "+gate+" (open BOOLEAN)")
	exec(t, conn, "INSERT INTO "+gate+" VALUES (true)")

	// The fault is a gate table the sink's SQL joins against, not the
	// destination itself. Dropping the destination would break ReadBack too,
	// and the harness reads back while the fault is installed: a broken
	// destination must not be indistinguishable from an empty one.
	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.sqlcommand",
		New: func(t *testing.T) core.Sink {
			s, err := NewSQLCommandSink(conn,
				"INSERT INTO "+target+" (id) SELECT b.id FROM "+sinkBatchTable+" b, "+gate, nil)
			assert.NoError(t, err)
			return s
		},
		Break: func(t *testing.T) { exec(t, conn, "DROP TABLE "+gate) },
		Heal: func(t *testing.T) {
			exec(t, conn, "CREATE TABLE "+gate+" (open BOOLEAN)")
			exec(t, conn, "INSERT INTO "+gate+" VALUES (true)")
		},
		ReadBack: func(t *testing.T) []conformance.Row {
			return queryIDs(t, conn, "SELECT id FROM "+target+" ORDER BY arrived")
		},
		Table: func(t *testing.T, id int64) arrow.Table { return oneRowTable(t, id) },
	})
}

func queryIDs(t *testing.T, conn adbc.Connection, sql string) []conformance.Row {
	t.Helper()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))

	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	var out []conformance.Row
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(0)
		for i := 0; i < int(rec.NumRows()); i++ {
			out = append(out, conformance.Row{"id": col.(*array.Int64).Value(i)})
		}
	}
	assert.NoError(t, reader.Err())
	return out
}
