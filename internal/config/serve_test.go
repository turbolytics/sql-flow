package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// validServe breaks no rule. Each rule test mutates one thing, so a failure
// names that rule and not an unrelated one.
const validServe = `
commands:
  - name: create
    sql: CREATE TABLE t (bucket TIMESTAMPTZ, lang VARCHAR, posts BIGINT);
serve:
  name: test
  http:
    addr: 127.0.0.1:0
    cors:
      allowed_origins: [https://turbolytics.io, http://localhost:3000]
  auth:
    tokens:
      - {name: page, token: page-token}
      - {name: ops, token: ops-token}
  limits:
    max_rows: 100
    timeout_seconds: 5
  datasets:
    - name: status
      sql: SELECT count(*) AS n FROM t
    - name: posts_by_lang
      params:
        - {name: since, type: timestamp}
        - {name: lang, type: string}
      limits:
        max_rows: 7
      grains:
        1h:
          sql: SELECT * FROM t WHERE bucket >= coalesce($since, now()) AND lang = coalesce($lang, lang)
        5m:
          sql: SELECT * FROM t WHERE lang = coalesce($lang, lang) AND bucket >= coalesce($since, now())
`

func parseServe(t *testing.T, text string) *ServeConf {
	t.Helper()
	conf, err := ParseServe([]byte(text))
	assert.NoError(t, err)
	return conf
}

// A serve file is decoded as strictly as a pipeline: an unknown key is a
// typo, and dropping it silently would serve something the author did not
// write.
func TestCliServe_LoadServeRenderedDecodesStrictly(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	dir := t.TempDir()
	good := filepath.Join(dir, "serve.yml")
	assert.NoError(t, os.WriteFile(good, []byte(validServe), 0o644))

	conf, rendered, err := LoadServeRendered(good, nil)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(validServe), strings.TrimSpace(string(rendered)))
	assert.Equal(t, "posts_by_lang", conf.Serve.Datasets[1].Name)
	assert.Equal(t, "timestamp", conf.Serve.Datasets[1].Params[0].Type)
	assert.DeepEqual(t, []string{"1h", "5m"}, conf.Serve.Datasets[1].GrainNames())

	bad := filepath.Join(dir, "typo.yml")
	assert.NoError(t, os.WriteFile(bad, []byte(strings.Replace(validServe, "max_rows: 100", "max_row: 100", 1)), 0o644))
	_, _, err = LoadServeRendered(bad, nil)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigParseFailed, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "max_row"))
}

// validate chooses a schema from the top-level keys. A file with both keys is
// a pipeline, so the pipeline schema reports serve as unknown.
func TestCliServe_IsServeReadsTopLevelKeys(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	assert.True(t, IsServe([]byte(validServe)))
	assert.False(t, IsServe([]byte("pipeline:\n  name: p\n")))
	assert.False(t, IsServe([]byte("pipeline: {}\nserve: {}\n")))
	assert.False(t, IsServe([]byte("- not a mapping\n")))
	assert.False(t, IsServe([]byte("")))
}

// A dataset's own limit wins, then the top level's, then the default. Zero
// means unset at every level.
func TestCliServe_LimitsResolveDatasetThenTopLevelThenDefault(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conf := parseServe(t, validServe)
	status, posts := conf.Serve.Datasets[0], conf.Serve.Datasets[1]

	assert.Equal(t, 100, conf.Serve.MaxRows(status))
	assert.Equal(t, 7, conf.Serve.MaxRows(posts))
	assert.Equal(t, 5*time.Second, conf.Serve.Timeout(posts))
	assert.Equal(t, "127.0.0.1:0", conf.Serve.Addr())

	bare := &Serve{}
	assert.Equal(t, DefaultServeMaxRows, bare.MaxRows(ServeDataset{}))
	assert.Equal(t, DefaultServeTimeoutSeconds*time.Second, bare.Timeout(ServeDataset{}))
	assert.Equal(t, DefaultServeAddr, bare.Addr())
}

func TestCliServe_CheckAcceptsAValidConfig(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conf := parseServe(t, validServe)
	assert.Equal(t, 0, len(conf.Check()))
	assert.NoError(t, conf.CheckError())
}

// Every rule reports its code, the YAML path validate turns into a line, and
// a message naming the thing that is wrong.
func TestCliServe_CheckReportsEachRuleAtItsPath(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for _, tt := range []struct {
		name     string
		from, to string
		code     errs.Code
		path     string
		message  string
	}{
		{"no tokens", "      - {name: page, token: page-token}\n      - {name: ops, token: ops-token}", "      []",
			errs.CodeConfigInvalid, "serve.auth.tokens", "no token"},
		{"duplicate token name", "{name: ops, token: ops-token}", "{name: page, token: ops-token}",
			errs.CodeConfigInvalid, "serve.auth.tokens.1.name", "page is used twice"},
		{"empty token", "token: ops-token", `token: ""`,
			errs.CodeConfigInvalid, "serve.auth.tokens.1.token", "ops has an empty value"},
		{"duplicate token value", "token: ops-token", "token: page-token",
			errs.CodeConfigInvalid, "serve.auth.tokens.1.token", "page and ops have the same value"},
		{"bad addr", "addr: 127.0.0.1:0", "addr: localhost",
			errs.CodeConfigInvalid, "serve.http.addr", "not a host:port"},
		{"origin with a path", "https://turbolytics.io,", "https://turbolytics.io/demo,",
			errs.CodeConfigInvalid, "serve.http.cors.allowed_origins.0", "no path"},
		{"origin with a trailing slash", "https://turbolytics.io,", "https://turbolytics.io/,",
			errs.CodeConfigInvalid, "serve.http.cors.allowed_origins.0", "no path"},
		{"wildcard origin", "https://turbolytics.io,", "https://*.turbolytics.io,",
			errs.CodeConfigInvalid, "serve.http.cors.allowed_origins.0", "wildcards"},
		{"origin without a scheme", "https://turbolytics.io,", "turbolytics.io,",
			errs.CodeConfigInvalid, "serve.http.cors.allowed_origins.0", "http://"},
		{"uppercase origin", "https://turbolytics.io,", "https://Turbolytics.io,",
			errs.CodeConfigInvalid, "serve.http.cors.allowed_origins.0", "lowercase"},
		{"negative max_rows", "max_rows: 100", "max_rows: -1",
			errs.CodeConfigInvalid, "serve.limits.max_rows", "negative"},
		{"top-level rate_limit", "timeout_seconds: 5", "timeout_seconds: 5\n    rate_limit: {requests_per_second: 10}",
			errs.CodeConfigServeReserved, "serve.limits.rate_limit", "not enforced"},
		{"dataset rate_limit", "max_rows: 7", "max_rows: 7\n        rate_limit: {burst: 2}",
			errs.CodeConfigServeReserved, "serve.datasets.1.limits.rate_limit", "not enforced"},
		{"bad dataset name", "name: status", "name: Status",
			errs.CodeConfigServeDataset, "serve.datasets.0.name", `"Status"`},
		{"duplicate dataset", "name: status", "name: posts_by_lang",
			errs.CodeConfigServeDataset, "serve.datasets.1.name", "declared twice"},
		{"sql and grains", "      limits:\n        max_rows: 7", "      sql: SELECT 1\n      limits:\n        max_rows: 7",
			errs.CodeConfigServeDataset, "serve.datasets.1", "both sql and grains"},
		{"neither sql nor grains", "      sql: SELECT count(*) AS n FROM t\n", "",
			errs.CodeConfigServeDataset, "serve.datasets.0", "neither sql nor grains"},
		{"bad grain name", "        5m:", "        5M:",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.5M", `"5M"`},
		{"param named grain", "      sql: SELECT count(*) AS n FROM t",
			"      params: [{name: grain, type: string}]\n      sql: SELECT count(*) AS n FROM t WHERE $grain IS NULL",
			errs.CodeConfigServeDataset, "serve.datasets.0.params.0.name", "grain selects the grain"},
		{"bad param type", "{name: lang, type: string}", "{name: lang, type: text}",
			errs.CodeConfigServeDataset, "serve.datasets.1.params.1.type", `"text"`},
		{"min on a string param", "{name: lang, type: string}", "{name: lang, type: string, min: 1}",
			errs.CodeConfigServeDataset, "serve.datasets.1.params.1.min", "min and max bound integer params only"},
		{"max on a timestamp param", "{name: since, type: timestamp}", "{name: since, type: timestamp, max: 5}",
			errs.CodeConfigServeDataset, "serve.datasets.1.params.0.max", "min and max bound integer params only"},
		{"min above max", "{name: lang, type: string}", "{name: lang, type: integer, min: 5, max: 1}",
			errs.CodeConfigServeDataset, "serve.datasets.1.params.1.max", "min 5 above max 1"},
		{"positional placeholder", "SELECT count(*) AS n FROM t", "SELECT $1 AS n FROM t",
			errs.CodeConfigServeDataset, "serve.datasets.0.sql", "$1"},
		{"undeclared placeholder", "SELECT count(*) AS n FROM t", "SELECT $nope AS n FROM t",
			errs.CodeConfigServeDataset, "serve.datasets.0.sql", "$nope is not a declared param"},
		{"a grain ignores a param", "WHERE lang = coalesce($lang, lang) AND", "WHERE",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.5m.sql", "grain 5m does not use param lang"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.That(t, strings.Contains(validServe, tt.from))
			conf := parseServe(t, strings.Replace(validServe, tt.from, tt.to, 1))

			violations := conf.Check()
			assert.Equal(t, 1, len(violations))
			assert.Equal(t, tt.code, violations[0].Code)
			assert.Equal(t, tt.path, strings.Join(violations[0].Path, "."))
			if !strings.Contains(violations[0].Message, tt.message) {
				t.Fatalf("message %q does not contain %q", violations[0].Message, tt.message)
			}
		})
	}

	t.Run("no datasets", func(t *testing.T) {
		conf := parseServe(t, validServe)
		conf.Serve.Datasets = nil

		violations := conf.Check()
		assert.Equal(t, 1, len(violations))
		assert.Equal(t, "serve.datasets", strings.Join(violations[0].Path, "."))
	})
}

// Two faults in one param must keep two paths. Violation paths are built by
// extending a parent path, and an append that reuses the parent's backing
// array rewrites the first violation's path when the second is added.
func TestCliServe_CheckKeepsEveryViolationPath(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conf := parseServe(t, strings.Replace(validServe,
		"{name: lang, type: string}", "{name: Lang, type: text}", 1))

	var paths []string
	for _, v := range conf.Check() {
		paths = append(paths, strings.Join(v.Path, "."))
	}
	assert.That(t, len(paths) >= 2)
	assert.Equal(t, "serve.datasets.1.params.1.name", paths[0])
	assert.Equal(t, "serve.datasets.1.params.1.type", paths[1])
}

// serve refuses to start on any violation, and a reader fixes them all in one
// pass only if the error lists them all.
func TestCliServe_CheckErrorListsEveryViolation(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	text := strings.Replace(validServe, "timeout_seconds: 5", "timeout_seconds: 5\n    rate_limit: {burst: 1}", 1)
	text = strings.Replace(text, "name: status", "name: Status", 1)

	err := parseServe(t, text).CheckError()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigServeReserved, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "serve.limits.rate_limit"))
	assert.That(t, strings.Contains(err.Error(), "serve.datasets.0.name"))
}
