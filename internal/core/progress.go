package core

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// progressTable is engine bookkeeping beside sqlflow_offsets: one row that
// says when the newest batch arrived, when state last committed, and how many
// messages have been consumed. One UPDATE sets them together, so the row is
// a statement: as of last_commit, the newest arrival was last_arrival. The
// window predicate closes on idleness only when that gap reaches its bound,
// so a row that stops being written confirms nothing. /stats and /healthz
// read the in-memory copy.
const progressTable = "sqlflow_progress"

// Progress is the pipeline's liveness. Wall clock, UTC.
type Progress struct {
	LastArrival time.Time
	LastCommit  time.Time
	Messages    int64

	// Delivering is what the source says about whether it could deliver at
	// all: nil from a source that has no opinion, which is every source but
	// Kafka and the websocket. Three states, because a source that cannot
	// deliver and a source that was never asked mean opposite things to a
	// window: the first holds every bucket open, the second holds none.
	Delivering *bool
	// DeliveringSince is when the source last became able to deliver: a Kafka
	// consumer's assignment, a websocket's dial. Zero while it cannot, and
	// zero when nothing was asked. On the same clock as the two above, so a
	// reader subtracts row values and never its own now.
	DeliveringSince time.Time

	// LastError is when the loop last recorded an error, and Errors is how
	// many it has recorded. In memory only: they describe this process, not
	// the durable state, so the progress table does not carry them.
	LastError time.Time
	Errors    int64
}

// ProgressSaver is what the Turbine records liveness into; ProgressStore is
// the DuckDB one. Exported so a caller outside core can wrap or replace the
// store without redeclaring the interface.
type ProgressSaver interface {
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
		    last_arrival     TIMESTAMPTZ,
		    last_commit      TIMESTAMPTZ,
		    delivering_since TIMESTAMPTZ,
		    delivering       BOOLEAN,
		    messages         BIGINT NOT NULL
		)`,
		// A state database written before these columns existed opens
		// without them, and the manager's read names both.
		`ALTER TABLE ` + progressTable + ` ADD COLUMN IF NOT EXISTS delivering_since TIMESTAMPTZ`,
		`ALTER TABLE ` + progressTable + ` ADD COLUMN IF NOT EXISTS delivering BOOLEAN`,
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
	// Cleared rather than left, because a stale instant reads as a source
	// that is still delivering and the manager would count quiet across an
	// outage.
	if p.DeliveringSince.IsZero() {
		q += `, delivering_since = NULL`
	} else {
		q += fmt.Sprintf(`, delivering_since = TIMESTAMPTZ '%s'`, utcLiteral(p.DeliveringSince))
	}
	switch {
	case p.Delivering == nil:
		q += `, delivering = NULL`
	case *p.Delivering:
		q += `, delivering = TRUE`
	default:
		q += `, delivering = FALSE`
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
