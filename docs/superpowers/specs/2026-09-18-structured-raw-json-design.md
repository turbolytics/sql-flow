# StructuredBatch: an object read into a text column keeps its escapes

Issue #331. Reproduced in `turbolytics/sql-flow:v2026.09.17.3-dirty` on
2026-09-18. Part of
[Deploy to Render](2026-09-18-render-metrics-template-design.md).

## The problem

A `StructuredBatch` table may declare a `TEXT` column for a field whose value
is a JSON object. The handler stores the object's text, which is how a
pipeline carries a free-form object: the metrics template reads `dimensions`
this way and parses it in SQL.

The text is corrupted when any string inside the object has an escape. Posted
to a webhook pipeline with `dimensions TEXT`:

```json
{"name":"b","dimensions":{"q":"say \"hi\"","n":1}}
```

The handler's row:

```
"dimensions": "{\"q\":\"say \"hi\"\",\"n\":1}"
```

The stored text is `{"q":"say "hi"","n":1}`, which is not JSON. `from_json`
fails on it, and the metrics template drops the row.

`appendValue` in `internal/handlers/structured.go` sends every value bound
for a string column through `jsonString`, which decodes escape sequences.
That is right for a JSON string: `jsonparser` hands over the bytes between
the quotes, escapes intact. It is wrong for an object or an array, where the
bytes are the JSON text itself and its escapes are part of it.

## Scope

In: `appendValue` decodes escapes only when the JSON value is a string. The
inferred handlers build rows through the same function and get the same fix.

Out: a `JSON` column type for `StructuredBatch`. `TEXT` parsed in SQL works
once the text is intact.

## The change

In `appendValue`, the `*arrow.StringType` arm:

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

A number or a boolean in a string column has no backslash, so `jsonString`
already returned it untouched. Only objects and arrays change.

## Testing

`internal/handlers/structured_test.go`, feature `handler.structured`:

- `{"d":{"q":"say \"hi\"","p":"a\\b"}}` into `d TEXT`: the column equals the
  object's bytes as sent, and `json_extract_string(d, '$.q')` is `say "hi"`.
- The same object as a list element, `ds TEXT[]`.
- A plain string with an escape, `"a\nb"`, still decodes to a newline. This
  is the behavior `jsonString` exists for, and the fix must not undo it.
