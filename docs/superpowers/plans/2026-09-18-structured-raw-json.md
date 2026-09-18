# StructuredBatch Raw JSON Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `StructuredBatch` corrupting a JSON object stored in a text column when a string inside it has an escape.

**Architecture:** One branch in `appendValue`: decode escapes only when the JSON value is a string. Objects and arrays are stored as the bytes received.

**Tech Stack:** Go, `github.com/buger/jsonparser`, `github.com/zeebo/assert`, DuckDB through ADBC in tests.

**Spec:** `docs/superpowers/specs/2026-09-18-structured-raw-json-design.md`

## Global Constraints

- A plain JSON string with an escape still decodes: `"a\nb"` stores a newline.
- Every test calls `coverage.Covers(t, "handler.structured")` first and is named `TestHandlerStructured_…`.
- Tests need libduckdb: `SQLFLOW_DUCKDB_LIB`, or the Homebrew install at `/opt/homebrew/lib/libduckdb.dylib`. `make libduckdb` installs one under `bin/`.
- Prose follows `CLAUDE.md`. Commit messages name the defect, the fix, and the evidence.
- Branch from `main`: `fix/structured-raw-json`.

---

### Task 1: an object in a text column keeps its escapes

**Files:**
- Modify: `internal/handlers/structured.go` (`appendValue`, the `*arrow.StringType` arm, near line 137)
- Modify: `CHANGELOG.md`
- Test: `internal/handlers/structured_test.go`

**Interfaces:**
- Consumes: `newTestADBCConn`, `createTable`, `NewStructuredBatchHandler`, all in `structured_test.go` or the package.
- Produces: nothing.

- [ ] **Step 1: Write the failing tests**

Append to `internal/handlers/structured_test.go`. The assertions run in SQL: the handler's query keeps a row only when the stored text is what was sent, so the row count is the verdict. `$$…$$` is DuckDB's dollar quoting, which spares the Go string a second layer of escapes.

```go
// A free-form object is carried in a text column and parsed in SQL. Its text
// must be the bytes received: decoding the escapes inside it turns \" into a
// bare quote, and the text stops being JSON.
func TestHandlerStructured_ObjectInATextColumnKeepsItsEscapes(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE events (name TEXT, d TEXT);`)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "d", Type: arrow.BinaryTypes.String},
	}, nil)

	h, err := NewStructuredBatchHandler(conn, `
SELECT * FROM events
WHERE d = $${"q":"say \"hi\"","p":"a\\b"}$$
  AND json_extract_string(d, '$.q') = 'say "hi"'`, "events", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"name":"b","d":{"q":"say \"hi\"","p":"a\\b"}}`)))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res.NumRows())
	res.Release()
}

// The decode the object must skip is the one a string needs.
func TestHandlerStructured_StringInATextColumnStillDecodes(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE events (s TEXT);`)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)

	h, err := NewStructuredBatchHandler(conn,
		`SELECT * FROM events WHERE s = 'a' || chr(10) || 'b' AND length(s) = 3`, "events", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"s":"a\nb"}`)))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res.NumRows())
	res.Release()
}
```

- [ ] **Step 2: Run the tests to verify the first fails and the second passes**

Run: `go test ./internal/handlers/ -run 'TestHandlerStructured_(ObjectInATextColumn|StringInATextColumn)' -v`
Expected: `ObjectInATextColumnKeepsItsEscapes` FAILS with `0` rows where it wants `1`. `StringInATextColumnStillDecodes` PASSES: it guards behavior that already works.

If the first fails with an error naming `json_extract_string`, the test DuckDB cannot load the json extension. Drop that line of the `WHERE`; the equality on `d` alone decides the case.

- [ ] **Step 3: Implement**

In `internal/handlers/structured.go`, in `appendValue`, replace:

```go
	case *arrow.StringType:
		builder.(*array.StringBuilder).Append(jsonString(val))
```

with:

```go
	case *arrow.StringType:
		// Only a JSON string has escapes to decode. An object, an array, a
		// number or a boolean arrives as its own JSON text, and decoding
		// that rewrites \" inside it into a bare quote.
		if dataType == jsonparser.String {
			builder.(*array.StringBuilder).Append(jsonString(val))
		} else {
			builder.(*array.StringBuilder).Append(unsafeString(val))
		}
```

- [ ] **Step 4: Run the package**

Run: `go test -short -race ./internal/handlers/`
Expected: PASS, every test.

- [ ] **Step 5: Changelog**

In `CHANGELOG.md`, under `## Unreleased`, `### Fixed`, add:

```markdown
- `StructuredBatch` stored a JSON object read into a `TEXT` column with the
  escapes inside it decoded, so `{"q":"say \"hi\""}` became
  `{"q":"say "hi""}`, which is not JSON, and `from_json` failed on it. Only a
  JSON string is decoded now. An object or an array is stored as the bytes
  received.
```

- [ ] **Step 6: Commit**

```bash
git add internal/handlers/structured.go internal/handlers/structured_test.go CHANGELOG.md
git commit -m "handlers: an object in a text column keeps the escapes inside it

appendValue decoded JSON escapes in every value bound for a string
column. For a JSON string that is right: jsonparser hands over the bytes
between the quotes. For an object the bytes are the JSON text, and
decoding turned {\"q\":\"say \\\"hi\\\"\"} into text that no longer parses.
Reproduced through a webhook pipeline in v2026.09.17.3.

Only a JSON string is decoded. A second test holds that a plain string
with \\n still stores a newline."
```

---

## Verification

- [ ] `make test-go` passes.
