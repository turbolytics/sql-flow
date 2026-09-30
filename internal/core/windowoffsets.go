package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// WindowOffsetsTable holds, per window, bucket and partition, the lowest
// offset that wrote a row into the bucket. It is what lets the committed
// offset stay at or below every row a retained bucket holds, so Kafka is the
// durable copy of window state: a worker that starts without the rows, on a
// new disk or after a rebalance, replays from there and rebuilds each bucket
// whole.
const WindowOffsetsTable = "sqlflow_window_offsets"

// OffsetRecord is one row of that table.
type OffsetRecord struct {
	Window    string
	Bucket    time.Time
	Topic     string
	Partition int32
	// Mark is the lowest offset seen for the bucket and the leader epoch it
	// was read under.
	Mark Mark
}

type offsetKey struct {
	window    string
	bucket    int64
	topic     string
	partition int32
}

// WindowOffsetsDelta is what the store must write to match the tracker.
type WindowOffsetsDelta struct {
	Added   []OffsetRecord
	Expired map[string]time.Time
	Dropped map[string][]int32
}

// WindowOffsets tracks the lowest offset feeding each retained bucket.
//
// Note runs per record on the consume loop, so it touches only a slice of
// this batch's first sightings, scanned linearly: a batch spans few
// partitions and buckets. Merge folds that slice into the map once per
// batch. Everything else runs on the consume loop too, between batches, so
// nothing here takes a lock.
type WindowOffsets struct {
	specs  []WindowSpec
	recs   map[offsetKey]Mark
	batch  []OffsetRecord
	delta  WindowOffsetsDelta
	closed map[string]time.Time
}

func NewWindowOffsets(specs []WindowSpec) *WindowOffsets {
	return &WindowOffsets{
		specs:  specs,
		recs:   map[offsetKey]Mark{},
		closed: map[string]time.Time{},
		delta:  WindowOffsetsDelta{Expired: map[string]time.Time{}, Dropped: map[string][]int32{}},
	}
}

// Note records a placed record against its bucket in every window. Offsets
// rise within a partition, so the first sighting of a key in a batch is its
// lowest in that batch, and a key already merged holds a lower one still.
func (o *WindowOffsets) Note(m Message) {
	if !m.HasMetadata() || m.EventAtNanos <= 0 {
		return
	}
	at := time.Unix(0, m.EventAtNanos).UTC()
	for _, spec := range o.specs {
		bucket := BucketStart(at, spec.Size)
		seen := false
		for i := range o.batch {
			r := &o.batch[i]
			if r.Partition == m.Partition && r.Bucket.Equal(bucket) && r.Window == spec.Name && r.Topic == m.Topic {
				seen = true
				break
			}
		}
		if !seen {
			o.batch = append(o.batch, OffsetRecord{
				Window: spec.Name, Bucket: bucket, Topic: m.Topic, Partition: m.Partition,
				Mark: Mark{Offset: m.Offset, LeaderEpoch: m.LeaderEpoch},
			})
		}
	}
}

// Merge folds the batch's sightings in. A key already held keeps its lower
// offset. A new key is pending for the store until Saved.
func (o *WindowOffsets) Merge() {
	for _, r := range o.batch {
		k := offsetKey{r.Window, r.Bucket.UnixNano(), r.Topic, r.Partition}
		if cur, ok := o.recs[k]; ok && cur.Offset <= r.Mark.Offset {
			continue
		}
		o.recs[k] = r.Mark
		o.delta.Added = append(o.delta.Added, r)
	}
	o.batch = o.batch[:0]
}

// Expire drops the records of window's buckets past their lateness against
// the manager's closed watermark: bucket_end + lateness <= closed, the
// predicate the manager deletes the rows by. It reports whether a record
// went, which is when the position to commit may have moved.
func (o *WindowOffsets) Expire(window string, closed time.Time) bool {
	var spec WindowSpec
	for _, s := range o.specs {
		if s.Name == window {
			spec = s
		}
	}
	if spec.Name == "" || !closed.After(o.closed[window]) {
		return false
	}
	o.closed[window] = closed
	o.delta.Expired[window] = closed
	limit := closed.Add(-spec.Lateness).UnixNano()
	dropped := false
	for k := range o.recs {
		if k.window == window && k.bucket+int64(spec.Size) <= limit {
			delete(o.recs, k)
			dropped = true
		}
	}
	kept := o.delta.Added[:0]
	for _, r := range o.delta.Added {
		if r.Window == window && r.Bucket.Add(spec.Size).UnixNano() <= limit {
			continue
		}
		kept = append(kept, r)
	}
	o.delta.Added = kept
	return dropped
}

// Drop forgets every record of the partitions.
func (o *WindowOffsets) Drop(topic string, partitions []int32) {
	drop := map[int32]bool{}
	for _, p := range partitions {
		drop[p] = true
	}
	for k := range o.recs {
		if k.topic == topic && drop[k.partition] {
			delete(o.recs, k)
		}
	}
	kept := o.delta.Added[:0]
	for _, r := range o.delta.Added {
		if r.Topic == topic && drop[r.Partition] {
			continue
		}
		kept = append(kept, r)
	}
	o.delta.Added = kept
	o.delta.Dropped[topic] = append(o.delta.Dropped[topic], partitions...)
}

// Low is the position to commit for every partition processed holds: the
// one before the lowest offset still feeding a retained bucket, or processed's
// own where nothing retained holds rows from the partition. Never above
// processed.
func (o *WindowOffsets) Low(processed *Marks) *Marks {
	low := NewMarks()
	processed.Each(func(topic string, partition int32, mark Mark) {
		best := mark
		for k, m := range o.recs {
			if k.topic == topic && k.partition == partition && m.Offset-1 < best.Offset {
				best = Mark{Offset: m.Offset - 1, LeaderEpoch: m.LeaderEpoch}
			}
		}
		low.Advance(topic, partition, best)
	})
	return low
}

// Closed is the closed watermark Expire last saw per window.
func (o *WindowOffsets) Closed() map[string]time.Time { return o.closed }

// Load seeds the tracker from the store at start. Nothing loaded is pending.
func (o *WindowOffsets) Load(recs []OffsetRecord) {
	for _, r := range recs {
		o.recs[offsetKey{r.Window, r.Bucket.UnixNano(), r.Topic, r.Partition}] = r.Mark
	}
}

// Pending is what the store has not yet written.
func (o *WindowOffsets) Pending() WindowOffsetsDelta { return o.delta }

// Saved clears Pending, once the transaction that wrote it has committed.
func (o *WindowOffsets) Saved() {
	o.delta = WindowOffsetsDelta{Expired: map[string]time.Time{}, Dropped: map[string][]int32{}}
}

// WindowOffsetStore keeps sqlflow_window_offsets on the pipeline's
// connection, written in the batch's transaction beside the rows the records
// describe. No index: rows are deleted as buckets expire, and DuckDB never
// frees rows deleted from an indexed table (#268).
type WindowOffsetStore struct {
	conn  adbc.Connection
	specs []WindowSpec
}

func NewWindowOffsetStore(conn adbc.Connection, specs []WindowSpec) *WindowOffsetStore {
	return &WindowOffsetStore{conn: conn, specs: specs}
}

// Init creates the table if it is absent. Run it under autocommit, before
// the pipeline turns autocommit off.
func (s *WindowOffsetStore) Init(ctx context.Context) error {
	return s.exec(ctx, `CREATE TABLE IF NOT EXISTS `+WindowOffsetsTable+` (
	    window_name  VARCHAR     NOT NULL,
	    bucket       TIMESTAMPTZ NOT NULL,
	    topic        VARCHAR     NOT NULL,
	    partition    INTEGER     NOT NULL,
	    "offset"     BIGINT      NOT NULL,
	    leader_epoch INTEGER     NOT NULL
	)`)
}

// Load reads every record. A key written twice keeps its lower offset.
func (s *WindowOffsetStore) Load(ctx context.Context) ([]OffsetRecord, error) {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(`SELECT window_name, epoch_us(bucket), topic, partition, min("offset"),
	        arg_min(leader_epoch, "offset")
	    FROM ` + WindowOffsetsTable + ` GROUP BY window_name, bucket, topic, partition`); err != nil {
		return nil, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, errs.Wrap(errs.CodeStateCorrupt, err, "reading %s from the state file", WindowOffsetsTable)
	}
	defer reader.Release()
	var out []OffsetRecord
	for reader.Next() {
		rec := reader.Record()
		names := rec.Column(0).(*array.String)
		buckets := rec.Column(1).(*array.Int64)
		topics := rec.Column(2).(*array.String)
		parts := rec.Column(3).(*array.Int32)
		offs := rec.Column(4).(*array.Int64)
		epochs := rec.Column(5).(*array.Int32)
		for i := 0; i < int(rec.NumRows()); i++ {
			out = append(out, OffsetRecord{
				Window:    strings.Clone(names.Value(i)),
				Bucket:    time.UnixMicro(buckets.Value(i)).UTC(),
				Topic:     strings.Clone(topics.Value(i)),
				Partition: parts.Value(i),
				Mark:      Mark{Offset: offs.Value(i), LeaderEpoch: epochs.Value(i)},
			})
		}
	}
	return out, reader.Err()
}

// Save writes a delta. It does not commit; the caller owns the transaction.
func (s *WindowOffsetStore) Save(ctx context.Context, d WindowOffsetsDelta) error {
	for window, closed := range d.Expired {
		var spec WindowSpec
		for _, sp := range s.specs {
			if sp.Name == window {
				spec = sp
			}
		}
		// bucket + size + lateness <= closed, as the tracker expires.
		limit := closed.Add(-spec.Lateness).Add(-spec.Size)
		if err := s.exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE window_name = '%s' AND bucket <= TIMESTAMPTZ '%s'`,
			WindowOffsetsTable, escapeSQLString(window), utcLiteral(limit))); err != nil {
			return fmt.Errorf("expiring offset records for %s: %w", window, err)
		}
	}
	for topic, parts := range d.Dropped {
		if len(parts) == 0 {
			continue
		}
		if err := s.exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE topic = '%s' AND partition IN (%s)`,
			WindowOffsetsTable, escapeSQLString(topic), joinInt32(parts))); err != nil {
			return fmt.Errorf("dropping offset records: %w", err)
		}
	}
	if len(d.Added) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(`INSERT INTO ` + WindowOffsetsTable + ` VALUES `)
	for i, r := range d.Added {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `('%s', TIMESTAMPTZ '%s', '%s', %d, %d, %d)`, escapeSQLString(r.Window),
			utcLiteral(r.Bucket), escapeSQLString(r.Topic), r.Partition, r.Mark.Offset, r.Mark.LeaderEpoch)
	}
	return s.exec(ctx, b.String())
}

func joinInt32(ps []int32) string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = fmt.Sprint(p)
	}
	return strings.Join(out, ", ")
}

func (s *WindowOffsetStore) exec(ctx context.Context, q string) error {
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
