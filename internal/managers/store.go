package managers

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
)

// windowsTable holds each window's watermark: the event-time instant every
// bucket ending at or before it has been published and deleted. It is state,
// beside sqlflow_offsets and sqlflow_progress, so it survives a restart when
// the pipeline has a state path and is the memory of what was published.
const windowsTable = core.WindowsTable

// Store reads and writes the watermark of each window on one connection. It
// never commits: the manager owns the transaction, so the watermark lands
// together with the delete it describes.
//
// The table carries no index and is never deleted from. One row per window,
// updated in place. DuckDB never frees rows deleted from an indexed table,
// and a row rewritten every poll under an index is that leak on a timer.
type Store struct {
	conn adbc.Connection
}

func NewStore(conn adbc.Connection) *Store {
	return &Store{conn: conn}
}

// Init creates the table if it is absent. Run it under autocommit, before
// the pipeline turns autocommit off on its connection, so the DDL commits
// on its own the way the offsets table's does.
func (s *Store) Init(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS ` + windowsTable + ` (
	    name      VARCHAR NOT NULL,
	    watermark TIMESTAMPTZ NOT NULL,
	    closed_at TIMESTAMPTZ NOT NULL
	)`
	if err := s.exec(ctx, q); err != nil {
		return fmt.Errorf("initialising %s: %w", windowsTable, err)
	}
	return nil
}

// Load returns the persisted watermark for a window, or ok false when the
// window has never closed a bucket.
func (s *Store) Load(ctx context.Context, name string) (watermark time.Time, ok bool, err error) {
	q := fmt.Sprintf(`SELECT epoch_us(watermark) FROM %s WHERE name = '%s'`,
		windowsTable, core.EscapeSQLString(name))
	micros, found, err := queryInt64(ctx, s.conn, q)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("loading watermark for %s: %w", name, err)
	}
	if !found {
		return time.Time{}, false, nil
	}
	return time.UnixMicro(micros).UTC(), true, nil
}

// Save writes the watermark. It updates the window's row, or inserts it the
// first time. It does not commit.
func (s *Store) Save(ctx context.Context, name string, watermark, closedAt time.Time) error {
	esc := core.EscapeSQLString(name)
	update := fmt.Sprintf(`UPDATE %s SET watermark = TIMESTAMPTZ '%s', closed_at = TIMESTAMPTZ '%s' WHERE name = '%s'`,
		windowsTable, core.UTCLiteral(watermark), core.UTCLiteral(closedAt), esc)
	n, err := s.execRows(ctx, update)
	if err != nil {
		return fmt.Errorf("saving watermark for %s: %w", name, err)
	}
	if n > 0 {
		return nil
	}
	insert := fmt.Sprintf(`INSERT INTO %s (name, watermark, closed_at) VALUES ('%s', TIMESTAMPTZ '%s', TIMESTAMPTZ '%s')`,
		windowsTable, esc, core.UTCLiteral(watermark), core.UTCLiteral(closedAt))
	if err := s.exec(ctx, insert); err != nil {
		return fmt.Errorf("saving watermark for %s: %w", name, err)
	}
	return nil
}

func (s *Store) exec(ctx context.Context, q string) error {
	_, err := s.execRows(ctx, q)
	return err
}

func (s *Store) execRows(ctx context.Context, q string) (int64, error) {
	return execRows(ctx, s.conn, q)
}

// execRows runs a statement and reports the rows it affected.
func execRows(ctx context.Context, conn adbc.Connection, q string) (int64, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return 0, err
	}
	return stmt.ExecuteUpdate(ctx)
}

// queryInt64 runs a one-value query. found is false when the value is NULL
// or there is no row, which is what an aggregate over an empty table and a
// lookup of an unknown name both produce.
// queryProgressRow reads the two engine durations the window decides on from
// one row: the quiet the engine confirmed, and how long the source has been
// able to deliver. A NULL second column is a source that cannot deliver.
func queryProgressRow(ctx context.Context, conn adbc.Connection, q string) (quiet, deliveringFor int64, delivering bool, err error) {
	// deliveringFor is -1 where the source never said, which StateOf reads as
	// no bound rather than as an instant resumption.
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, 0, false, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return 0, 0, false, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, 0, false, err
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		cols := make([]*array.Int64, 3)
		for i := range cols {
			col, ok := rec.Column(i).(*array.Int64)
			if !ok {
				return 0, 0, false, fmt.Errorf("%s: column %d wants BIGINT, got %T", q, i, rec.Column(i))
			}
			cols[i] = col
		}
		if cols[0].IsNull(0) {
			return 0, -1, true, nil
		}
		return cols[0].Value(0), cols[1].Value(0), cols[2].Value(0) == 1, nil
	}
	// No row at all: nothing said anything, which reads as delivering and
	// bounds no quiet, the same as a row whose column is NULL.
	return 0, -1, true, reader.Err()
}

func queryInt64(ctx context.Context, conn adbc.Connection, q string) (value int64, found bool, err error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, false, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return 0, false, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, false, err
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return 0, false, fmt.Errorf("%s: want a BIGINT column, got %T", q, rec.Column(0))
		}
		if col.IsNull(0) {
			return 0, false, nil
		}
		return col.Value(0), true, nil
	}
	return 0, false, reader.Err()
}
