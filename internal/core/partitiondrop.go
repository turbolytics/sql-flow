package core

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// PartitionDropper deletes rows from every partition-owned window, on the
// pipeline's connection and inside its open transaction where there is one.
type PartitionDropper interface {
	// DropPartitions deletes the rows of the partitions the group revoked.
	DropPartitions(ctx context.Context, topic string, partitions []int32) error
	// DropAll deletes every row: the pipeline is stopping, or starting, and
	// whoever holds the partitions next recounts them.
	DropAll(ctx context.Context) error
}

// DuckDBPartitionDropper is the PartitionDropper over window tables in
// DuckDB. The turbine deletes the stored positions and the offset records
// beside the rows.
type DuckDBPartitionDropper struct {
	conn   adbc.Connection
	tables []string
}

func NewDuckDBPartitionDropper(conn adbc.Connection, tables []string) *DuckDBPartitionDropper {
	return &DuckDBPartitionDropper{conn: conn, tables: tables}
}

// DropPartitions deletes by kafka_partition alone. The topic is not in the
// predicate because a partition-owned pipeline reads exactly one.
func (d *DuckDBPartitionDropper) DropPartitions(ctx context.Context, topic string, partitions []int32) error {
	if len(partitions) == 0 {
		return nil
	}
	in := joinInt32(partitions)
	for _, table := range d.tables {
		q := fmt.Sprintf(`DELETE FROM %s WHERE kafka_partition IN (%s)`, quoteIdent(table), in)
		if err := d.exec(ctx, q); err != nil {
			return fmt.Errorf("dropping partitions %s of %s from %s: %w", in, topic, table, err)
		}
	}
	return nil
}

func (d *DuckDBPartitionDropper) DropAll(ctx context.Context) error {
	for _, table := range d.tables {
		if err := d.exec(ctx, `DELETE FROM `+quoteIdent(table)); err != nil {
			return fmt.Errorf("dropping every row of %s: %w", table, err)
		}
	}
	return nil
}

func (d *DuckDBPartitionDropper) exec(ctx context.Context, q string) error {
	stmt, err := d.conn.NewStatement()
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
