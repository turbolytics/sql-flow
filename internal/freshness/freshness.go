// Package freshness measures how recent the data in a table is, and names
// the store it lives in the same way for every reporter. It knows tables,
// time columns and grains, and nothing about rollups or pipelines, so any
// process that reaches a store reports the same numbers: the rollup daemon
// first, and `sqlflow monitor` later.
package freshness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the one method this package needs; *pgx.Conn has it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Observation is a table's newest bucket as one reporter read it.
type Observation struct {
	// Table is qualified by its schema, so two reporters whose search paths
	// differ name one table the same way.
	Table      string
	TimeColumn string
	Grain      time.Duration
	// NewestBucketAt is the start of the newest bucket, nil for an empty
	// table.
	NewestBucketAt *time.Time
	// ObservedAt is the database's clock at the read, so the age never
	// depends on the reporter's clock.
	ObservedAt time.Time
}

// Age is the time from the end of the newest bucket to ObservedAt. A
// bucket ends at its start plus the grain. ok is false for an empty table.
func (o Observation) Age() (age time.Duration, ok bool) {
	if o.NewestBucketAt == nil {
		return 0, false
	}
	return o.ObservedAt.Sub(o.NewestBucketAt.Add(o.Grain)), true
}

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Observe reads table's newest bucket. timeColumn holds bucket starts, and
// an index that leads with it makes the read one index probe. table is
// unqualified and resolves through the connection's search_path, as the
// writers' statements do.
func Observe(ctx context.Context, q Querier, table, timeColumn string, grain time.Duration) (Observation, error) {
	o := Observation{Table: table, TimeColumn: timeColumn, Grain: grain}
	var schema *string
	err := q.QueryRow(ctx, fmt.Sprintf(
		"SELECT (SELECT relnamespace::regnamespace::text FROM pg_class WHERE oid = to_regclass($1)), max(%s), now() FROM %s",
		quote(timeColumn), quote(table)), quote(table)).Scan(&schema, &o.NewestBucketAt, &o.ObservedAt)
	if err != nil {
		return o, fmt.Errorf("freshness %s: %w", table, err)
	}
	if schema != nil {
		o.Table = *schema + "." + table
	}
	return o, nil
}

// Store names the database a table lives in.
type Store struct {
	// ID is "pg:" and 16 hex characters. It carries no host and no
	// credentials.
	ID string
	// Kind is KindSystem or KindAddress.
	Kind string
}

const (
	// KindSystem is an id from the server's system identifier, which
	// belongs to its data directory: every reporter derives the same id,
	// whatever hostname it dialed.
	KindSystem = "system"
	// KindAddress is an id from the address the server answered on, used
	// when the server refuses pg_control_system(). Another reporter may
	// see another address, so the id may not match its.
	KindAddress = "address"
)

// StoreOf names the database q reaches.
func StoreOf(ctx context.Context, q Querier) (Store, error) {
	var sysid int64
	var db string
	err := q.QueryRow(ctx, "SELECT system_identifier, current_database() FROM pg_control_system()").Scan(&sysid, &db)
	if err == nil {
		return Store{ID: systemID(sysid, db), Kind: KindSystem}, nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		return Store{}, fmt.Errorf("store id: %w", err)
	}
	var host string
	var port int
	if err := q.QueryRow(ctx,
		"SELECT coalesce(host(inet_server_addr()), 'local'), coalesce(inet_server_port(), 0), current_database()").Scan(&host, &port, &db); err != nil {
		return Store{}, fmt.Errorf("store id: %w", err)
	}
	return Store{ID: addressID(host, port, db), Kind: KindAddress}, nil
}

func systemID(sysid int64, db string) string {
	return hashID(fmt.Sprintf("%d/%s", sysid, db))
}

func addressID(host string, port int, db string) string {
	return hashID(fmt.Sprintf("%s:%d/%s", host, port, db))
}

func hashID(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "pg:" + hex.EncodeToString(sum[:])[:16]
}
