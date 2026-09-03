package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// offsetsTable is where a pipeline's Kafka positions live in DuckDB, next to
// the state the offsets describe. Keeping both in one database is what lets a
// later commit writes them together in a single transaction: state and the
// offset that produced it can never disagree after a crash.
const offsetsTable = "sqlflow_offsets"

// OffsetStore persists Marks to DuckDB. It holds no lock and starts no
// transaction of its own: Save issues writes on the connection it is given,
// and it is the caller's job to commit -- typically together with the batch
// that advanced those offsets.
type OffsetStore struct {
	conn adbc.Connection
}

// NewOffsetStore wraps a DuckDB connection. The connection is expected to be
// the same one the pipeline uses for its batch writes, so offsets land in the
// same transaction as the state they describe.
func NewOffsetStore(conn adbc.Connection) *OffsetStore {
	return &OffsetStore{conn: conn}
}

// Init creates the offsets table if it does not already exist. It is safe to
// call on every start, including against a state file from a previous run.
func (s *OffsetStore) Init(ctx context.Context) error {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return fmt.Errorf("creating statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(`
		CREATE TABLE IF NOT EXISTS ` + offsetsTable + ` (
		    topic        VARCHAR NOT NULL,
		    partition    INTEGER NOT NULL,
		    "offset"     BIGINT  NOT NULL,
		    leader_epoch INTEGER NOT NULL,
		    PRIMARY KEY (topic, partition)
		)
	`); err != nil {
		return fmt.Errorf("setting create-table query: %w", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("creating offsets table: %w", err)
	}
	return nil
}

// Save upserts one row per topic/partition in marks. It does not commit: the
// caller owns the transaction, so these writes can land together with
// whatever state change they are recording the position for.
func (s *OffsetStore) Save(ctx context.Context, marks *Marks) error {
	var writeErr error
	marks.Each(func(topic string, partition int32, mark Mark) {
		if writeErr != nil {
			return
		}

		stmt, err := s.conn.NewStatement()
		if err != nil {
			writeErr = fmt.Errorf("creating statement: %w", err)
			return
		}
		defer stmt.Close()

		if err := stmt.SetSqlQuery(fmt.Sprintf(`
			INSERT INTO %s (topic, partition, "offset", leader_epoch)
			VALUES ('%s', %d, %d, %d)
			ON CONFLICT (topic, partition) DO UPDATE SET
			    "offset" = EXCLUDED."offset",
			    leader_epoch = EXCLUDED.leader_epoch
		`, offsetsTable, escapeSQLString(topic), partition, mark.Offset, mark.LeaderEpoch)); err != nil {
			writeErr = fmt.Errorf("setting upsert query: %w", err)
			return
		}
		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			writeErr = fmt.Errorf("upserting offset for %s/%d: %w", topic, partition, err)
			return
		}
	})
	return writeErr
}

// Load returns every position recorded in the offsets table. A store with no
// rows yet returns an empty, non-nil Marks -- the caller must not treat that
// as offset zero.
func (s *OffsetStore) Load(ctx context.Context) (*Marks, error) {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("creating statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(fmt.Sprintf(
		`SELECT topic, partition, "offset", leader_epoch FROM %s`, offsetsTable,
	)); err != nil {
		return nil, fmt.Errorf("setting select query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying offsets: %w", err)
	}
	defer reader.Release()

	marks := NewMarks()
	for reader.Next() {
		rec := reader.Record()

		topics, ok := rec.Column(0).(*array.String)
		if !ok {
			return nil, fmt.Errorf("unexpected type for topic column: %T", rec.Column(0))
		}
		partitions, ok := rec.Column(1).(*array.Int32)
		if !ok {
			return nil, fmt.Errorf("unexpected type for partition column: %T", rec.Column(1))
		}
		offsets, ok := rec.Column(2).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("unexpected type for offset column: %T", rec.Column(2))
		}
		leaderEpochs, ok := rec.Column(3).(*array.Int32)
		if !ok {
			return nil, fmt.Errorf("unexpected type for leader_epoch column: %T", rec.Column(3))
		}

		for i := 0; i < int(rec.NumRows()); i++ {
			// Clone: topics.Value(i) aliases the record's backing buffer,
			// which is released -- and can be freed or reused -- once this
			// function returns.
			marks.Advance(strings.Clone(topics.Value(i)), partitions.Value(i), Mark{
				Offset:      offsets.Value(i),
				LeaderEpoch: leaderEpochs.Value(i),
			})
		}
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("reading offsets: %w", err)
	}

	return marks, nil
}

// escapeSQLString doubles single quotes so a topic name containing one cannot
// break out of the string literal it is interpolated into.
func escapeSQLString(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}
