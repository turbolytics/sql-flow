package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/zeebo/assert"
	"testing"
)

func TestArrowGetSchema(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open duckdb: %v", err)
	}
	defer db.Close()

	// Create a table with 5 columns
	createStmt := `
	CREATE TABLE users (
		id INTEGER,
		name TEXT,
		email TEXT,
		age INTEGER,
		created_at TIMESTAMP
	);
	`

	_, err = db.Exec(createStmt)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Check that the table was created
	rows, err := db.Query(`PRAGMA table_info('users');`)
	if err != nil {
		t.Fatalf("failed to query table info: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	assert.NoError(t, rows.Err())

	if count != 5 {
		t.Fatalf("expected 5 columns, got %d", count)
	}

	conn, err := db.Conn(context.Background())
	assert.NoError(t, err)
	defer conn.Close()

	arrow, err := duckdb.NewArrowFromConn(conn)
	assert.NoError(t, err)

	rdr, err := arrow.QueryContext(
		context.Background(),
		"select * from users",
	)
	assert.NoError(t, err)

	fmt.Println(rdr.Schema())
}
