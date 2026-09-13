// Package sqlparams finds named placeholders in a SQL statement and numbers
// them.
//
// DuckDB binds parameters by position. Through ADBC its parameter schema names
// them "0", "1", and a bound record's field names are ignored, so a statement
// that says $since and $until binds whatever arrives first to whichever
// placeholder appears first. sqlflow serve numbers the placeholders itself:
// Rewrite turns each $name into $N and records which name each N is.
package sqlparams

import (
	"fmt"
	"strings"
)

// Rewritten is a statement with its named placeholders numbered.
type Rewritten struct {
	// SQL is the statement with every $name replaced by $N.
	SQL string
	// Names holds each distinct name in order of first appearance. Names[0]
	// is $1. A name used twice appears once.
	Names []string
}

// Rewrite numbers every $name in sql, in order of first appearance.
//
// It skips '…' strings, "…" identifiers, -- and /* */ comments, and $$…$$ or
// $tag$…$tag$ strings, so $fake inside any of them is text. It does not handle
// an E'…' string with a backslash-escaped quote. An unterminated string or
// comment runs to the end of the statement, and DuckDB reports it at prepare.
//
// A positional $1 is an error: DuckDB refuses to mix positional and named
// parameters, and a statement that uses only positional ones would bind in an
// order the config does not state.
func Rewrite(sql string) (Rewritten, error) {
	var out strings.Builder
	out.Grow(len(sql))

	index := map[string]int{}
	var names []string

	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			end := strings.IndexByte(sql[i+1:], c)
			if end < 0 {
				out.WriteString(sql[i:])
				i = len(sql)
				continue
			}
			// A doubled quote inside a string closes it and opens another,
			// which copies the same bytes as treating it as an escape.
			out.WriteString(sql[i : i+end+2])
			i += end + 2

		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				out.WriteString(sql[i:])
				i = len(sql)
				continue
			}
			out.WriteString(sql[i : i+end+1])
			i += end + 1

		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				out.WriteString(sql[i:])
				i = len(sql)
				continue
			}
			out.WriteString(sql[i : i+2+end+2])
			i += 2 + end + 2

		case c == '$':
			j := i + 1
			switch {
			case j < len(sql) && isDigit(sql[j]):
				k := j
				for k < len(sql) && isDigit(sql[k]) {
					k++
				}
				return Rewritten{}, fmt.Errorf(
					"positional parameter %s: name it instead, as $name", sql[i:k])

			case j < len(sql) && (sql[j] == '$' || isIdentStart(sql[j])):
				k := j
				for k < len(sql) && isIdentPart(sql[k]) {
					k++
				}
				if k < len(sql) && sql[k] == '$' {
					// $$ or $tag$ opens a dollar-quoted string, closed by
					// the same delimiter.
					delim := sql[i : k+1]
					end := strings.Index(sql[k+1:], delim)
					if end < 0 {
						out.WriteString(sql[i:])
						i = len(sql)
						continue
					}
					stop := k + 1 + end + len(delim)
					out.WriteString(sql[i:stop])
					i = stop
					continue
				}

				name := sql[j:k]
				n, ok := index[name]
				if !ok {
					names = append(names, name)
					n = len(names)
					index[name] = n
				}
				fmt.Fprintf(&out, "$%d", n)
				i = k

			default:
				// A lone $ is not a parameter. DuckDB reports it.
				out.WriteByte(c)
				i++
			}

		default:
			out.WriteByte(c)
			i++
		}
	}

	return Rewritten{SQL: out.String(), Names: names}, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }
