package validate

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func exampleRollups(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	return string(src)
}

func TestValidateRollups_TheExamplePasses(t *testing.T) {
	coverage.Covers(t, "cli.rollup", "validate.schema")

	rep, err := Validate(context.Background(), Request{Config: exampleRollups(t)})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema"))
	assert.Equal(t, StatusPass, checkStatus(t, rep, "rollup.rules"))

	// A rollups file has no pipeline and no datasets of its own to hold to
	// serve's rules, so neither reports on it.
	for _, c := range rep.Checks {
		if strings.HasPrefix(c.ID, "pipeline.") || strings.HasPrefix(c.ID, "serve.") {
			t.Fatalf("a rollups file reports check %s: %s", c.ID, c.Status)
		}
	}
}

func TestValidateRollups_ARuleIsReportedAtItsLine(t *testing.T) {
	coverage.Covers(t, "cli.rollup", "validate.schema")

	text := strings.Replace(exampleRollups(t), "max_buckets: 365", "max_buckets: 0", 1)
	rep, err := Validate(context.Background(), Request{Config: text})
	assert.NoError(t, err)
	assert.False(t, rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "rollup.rules"))

	found := errorsOf(rep)
	assert.Equal(t, 1, len(found))
	assert.Equal(t, string(errs.CodeConfigRollup), found[0].Code)
	wantLine := strings.Count(text[:strings.Index(text, "max_buckets")], "\n") + 1
	assert.Equal(t, wantLine, found[0].Position.Line)
}

func TestValidateRollups_AnUnknownKeyFailsTheRollupsSchema(t *testing.T) {
	coverage.Covers(t, "cli.rollup", "validate.schema")

	text := strings.Replace(exampleRollups(t), "    grains:", "    grain_typo: 1\n    grains:", 1)
	rep, err := Validate(context.Background(), Request{Config: text})
	assert.NoError(t, err)
	assert.False(t, rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema"))
}
