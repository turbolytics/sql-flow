package run

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"go.uber.org/zap"
)

// debugAddr is where Flask's app.run() puts the Python engine's debug API, so
// the URL developers already have keeps working.
const debugAddr = "127.0.0.1:5000"

// newDebugHandler serves GET /debug?sql=..., executing arbitrary SQL against
// the live pipeline connection. The connection is not safe for concurrent use,
// so every query takes the same lock the pipeline holds while it invokes the
// handler and flushes.
func newDebugHandler(conn adbc.Connection, lock *sync.Mutex) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug", func(w http.ResponseWriter, r *http.Request) {
		sql := r.URL.Query().Get("sql")
		if sql == "" {
			writeDebugJSON(w, http.StatusBadRequest, map[string]string{"error": "No SQL query provided"})
			return
		}

		rows, err := queryRows(r.Context(), conn, lock, sql)
		if err != nil {
			writeDebugJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		writeDebugJSON(w, http.StatusOK, rows)
	})
	return mux
}

// queryRows returns one array of column values per row, the shape Flask's
// jsonify produces from duckdb's fetchall(). Values are encoded while the
// record is alive: string and binary columns hand back Go values that alias
// the Arrow buffers, which the reader frees on release.
func queryRows(ctx context.Context, conn adbc.Connection, lock *sync.Mutex, sql string) ([][]json.RawMessage, error) {
	lock.Lock()
	defer lock.Unlock()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	rows := [][]json.RawMessage{}
	for reader.Next() {
		rec := reader.Record()
		for i := range int(rec.NumRows()) {
			row := make([]json.RawMessage, rec.NumCols())
			for c := range int(rec.NumCols()) {
				v, err := json.Marshal(rec.Column(c).GetOneForMarshal(i))
				if err != nil {
					return nil, err
				}
				row[c] = v
			}
			rows = append(rows, row)
		}
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func writeDebugJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// startDebugServer runs the debug API for the lifetime of the process, as the
// Python engine's Flask thread does.
func startDebugServer(conn adbc.Connection, lock *sync.Mutex, l *zap.Logger) {
	l.Info("starting http debug server", zap.String("addr", "http://"+debugAddr+"/debug?sql=..."))

	go func() {
		if err := http.ListenAndServe(debugAddr, newDebugHandler(conn, lock)); err != nil {
			l.Error("http debug server stopped", zap.Error(err))
		}
	}()
}
