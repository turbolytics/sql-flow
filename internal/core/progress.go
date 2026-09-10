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
		    last_arrival TIMESTAMP,
		    last_commit  TIMESTAMP,
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

// Record rewrites the row. A zero LastArrival is left as it was, which is how
// an idle tick moves the commit clock without touching the arrival clock.
func (s *ProgressStore) Record(ctx context.Context, p Progress) error {
	q := fmt.Sprintf(`UPDATE %s SET last_commit = TIMESTAMP '%s', messages = %d`,
		progressTable, p.LastCommit.UTC().Format("2006-01-02 15:04:05.999999"), p.Messages)
	if !p.LastArrival.IsZero() {
		q += fmt.Sprintf(`, last_arrival = TIMESTAMP '%s'`,
			p.LastArrival.UTC().Format("2006-01-02 15:04:05.999999"))
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
