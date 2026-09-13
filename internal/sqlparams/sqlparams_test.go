package sqlparams

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A misnumbered placeholder binds a value to the wrong column and answers
// with wrong rows and a 200. Each case is a place a naive scan would count a
// $name that is not a parameter, or miss one that is.
func TestCliServe_RewriteNumbersNamedParams(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for _, tt := range []struct {
		name      string
		sql       string
		wantSQL   string
		wantNames []string
	}{
		{
			name:      "first appearance order",
			sql:       "SELECT * FROM t WHERE b = $b AND a = $a",
			wantSQL:   "SELECT * FROM t WHERE b = $1 AND a = $2",
			wantNames: []string{"b", "a"},
		},
		{
			name:      "a repeated name keeps its number",
			sql:       "SELECT $a, $b, $a",
			wantSQL:   "SELECT $1, $2, $1",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "no params",
			sql:       "SELECT 1",
			wantSQL:   "SELECT 1",
			wantNames: nil,
		},
		{
			name:      "a string literal is text",
			sql:       "SELECT '$fake', $real",
			wantSQL:   "SELECT '$fake', $1",
			wantNames: []string{"real"},
		},
		{
			name:      "a doubled quote stays inside the string",
			sql:       "SELECT 'it''s $fake', $real",
			wantSQL:   "SELECT 'it''s $fake', $1",
			wantNames: []string{"real"},
		},
		{
			name:      "a quoted identifier is text",
			sql:       `SELECT "$fake" FROM t WHERE x = $real`,
			wantSQL:   `SELECT "$fake" FROM t WHERE x = $1`,
			wantNames: []string{"real"},
		},
		{
			name:      "a line comment is text",
			sql:       "SELECT $a -- $fake\n, $b",
			wantSQL:   "SELECT $1 -- $fake\n, $2",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "a block comment is text",
			sql:       "SELECT /* $fake */ $a",
			wantSQL:   "SELECT /* $fake */ $1",
			wantNames: []string{"a"},
		},
		{
			name:      "an empty dollar quote is text",
			sql:       "SELECT $$ $fake $$, $a",
			wantSQL:   "SELECT $$ $fake $$, $1",
			wantNames: []string{"a"},
		},
		{
			name:      "a tagged dollar quote is text",
			sql:       "SELECT $q$ $fake $$ $q$, $a",
			wantSQL:   "SELECT $q$ $fake $$ $q$, $1",
			wantNames: []string{"a"},
		},
		{
			name:      "a lone dollar is copied",
			sql:       "SELECT $ , $a",
			wantSQL:   "SELECT $ , $1",
			wantNames: []string{"a"},
		},
		{
			name:      "a keyword is a valid name",
			sql:       "SELECT $from, $to",
			wantSQL:   "SELECT $1, $2",
			wantNames: []string{"from", "to"},
		},
		{
			name:      "an unterminated string runs to the end",
			sql:       "SELECT $a, 'open $fake",
			wantSQL:   "SELECT $1, 'open $fake",
			wantNames: []string{"a"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Rewrite(tt.sql)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantSQL, got.SQL)
			assert.DeepEqual(t, tt.wantNames, got.Names)
		})
	}
}

// DuckDB refuses to mix positional and named parameters, and the server
// numbers placeholders itself, so a $1 in the config is always a mistake.
func TestCliServe_RewriteRejectsPositionalParams(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	_, err := Rewrite("SELECT * FROM t WHERE a = $1 AND b = $name")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "$1"))
}
