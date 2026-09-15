package rollup

import (
	"os"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/zeebo/assert"
)

// examplePath is the declaration every generator test reads.
const examplePath = "../../dev/config/rollups/bluesky.yml"

func loadExample(t *testing.T) *config.RollupsConf {
	t.Helper()
	conf, err := config.LoadRollups(examplePath)
	assert.NoError(t, err)
	return conf
}

// golden holds got equal to the committed file at path. UPDATE_GOLDEN=1
// rewrites the file instead.
func golden(t *testing.T, path, got string) {
	t.Helper()
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		assert.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	assert.NoError(t, err)
	if string(want) != got {
		t.Fatalf("%s is stale. Read the diff, then run UPDATE_GOLDEN=1 go test ./internal/rollup/", path)
	}
}
