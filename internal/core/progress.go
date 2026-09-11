package core

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// progressTable is engine bookkeeping beside sqlflow_offsets: one row that
// says when the newest batch arrived, when state last committed, and how many
// messages have been consumed. The window predicate reads it to tell a quiet
// stream from a replay; /stats and /healthz read the in-memory copy.
const progressTable = "sqlflow_progress"

// Progress is the pipeline's liveness in three facts. Wall clock, UTC.
type Progress struct {
	LastArrival time.Time
	LastCommit  time.Time
	Messages    int64
}

// progressSaver is what the Turbine needs; ProgressStore is the DuckDB one.
type progressSaver interface {
	Record(ctx context.Context, p Progress) error
}

// ProgressStore keeps the one-row table current. It is created on the state
// connection when the pipeline has a state file, so its writes ride the same
// transaction as the offsets, and on a connection to the in-memory database
// otherwise.
type ProgressStore struct {
	conn adbc.Connection
}

func NewProgressStore(conn adbc.Connection) *ProgressStore {
	return &ProgressStore{conn: conn}
}

// Init creates the table and its single row if they are absent.
func (s *ProgressStore) Init(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS ` + progressTable + ` (
		    last_arrival TIMESTAMPTZ,
		    last_commit  TIMESTAMPTZ,
		    messages     BIGINT NOT NULL
		)`,
		`INSERT INTO ` + progressTable + ` (last_arrival, last_commit, messages)
		 SELECT NULL, NULL, 0 WHERE NOT EXISTS (SELECT 1 FROM ` + progressTable + `)`,
	} {
		if err := s.exec(ctx, q); err != nil {
			return fmt.Errorf("initialising %s: %w", progressTable, err)
		}
	}
	return nil
}

// utcLiteral renders an instant with its offset, so the value carries a zone
// rather than borrowing the session's. The columns are TIMESTAMPTZ for the
// same reason: a predicate writes `now() - last_arrival` and gets the right
// answer whatever timezone the server runs in. Written as TIMESTAMP against a
// UTC instant, a server in New York read the difference as four hours in the
// future and never closed a window.
func utcLiteral(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.999999-07:00")
}

// Record rewrites the row. A zero LastArrival is left as it was, which is how
// an idle tick moves the commit clock without touching the arrival clock.
func (s *ProgressStore) Record(ctx context.Context, p Progress) error {
	q := fmt.Sprintf(`UPDATE %s SET last_commit = TIMESTAMPTZ '%s', messages = %d`,
		progressTable, utcLiteral(p.LastCommit), p.Messages)
	if !p.LastArrival.IsZero() {
		q += fmt.Sprintf(`, last_arrival = TIMESTAMPTZ '%s'`, utcLiteral(p.LastArrival))
	}
	if err := s.exec(ctx, q); err != nil {
		return fmt.Errorf("recording progress: %w", err)
	}
	return nil
}

func (s *ProgressStore) exec(ctx context.Context, q string) error {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
