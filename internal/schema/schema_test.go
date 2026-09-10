package schema

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/internal/sources"
	"github.com/zeebo/assert"
	"gopkg.in/yaml.v3"
)

// commentsDir is where the reflector reads field documentation from. Tests run
// in their own package directory, so the path is relative to this one.
const commentsDir = "../config"

// goldenPath is the schema the engine embeds and ships.
const goldenPath = "../validate/schemas/config.json"

// The schema is generated, so the committed file is a build artifact and this
// is the golden test that keeps it current. Regenerate with `make schema`.
func TestConfigSchema_CommittedFileMatchesTheTypes(t *testing.T) {
	coverage.Covers(t, "config.validation")

	generated, err := Generate(commentsDir)
	assert.NoError(t, err)

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		assert.NoError(t, os.WriteFile(goldenPath, generated, 0o644))
		t.Log("schema updated")
		return
	}

	committed, err := os.ReadFile(goldenPath)
	assert.NoError(t, err)

	if !bytes.Equal(committed, generated) {
		t.Fatalf("%s is stale. Run `make schema`.", goldenPath)
	}
}

// The enum is the reason to generate rather than hand-write. A hand-copied
// list drifts the moment someone adds an integration, and the drift is silent
// until a user writes the new type and validate rejects what run accepts --
// which is what happened to `type: webhook`.
func TestConfigSchema_TypeEnumsComeFromTheRegistries(t *testing.T) {
	generated, err := Generate(commentsDir)
	assert.NoError(t, err)

	var doc map[string]any
	assert.NoError(t, yaml.Unmarshal(generated, &doc))

	for _, tt := range []struct {
		path []string
		want []string
	}{
		{[]string{"pipeline", "source", "type"}, sources.Kinds()},
		{[]string{"pipeline", "sink", "type"}, sinks.Kinds()},
		{[]string{"pipeline", "handler", "type"}, handlers.ConfigTypes()},
		{[]string{"pipeline", "on_error", "dlq", "type"}, sinks.Kinds()},
	} {
		got := enumAt(t, doc, tt.path)
		assert.DeepEqual(t, tt.want, got)
	}
}

// Every config the engine accepts today must still validate. A generated
// schema that rejects a shipped example is a regression in the format, not a
// stricter check.
func TestConfigSchema_AcceptsEveryShippedExample(t *testing.T) {
	coverage.Covers(t, "config.validation")

	generated, err := Generate(commentsDir)
	assert.NoError(t, err)

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(generated))
	assert.NoError(t, err)

	c := jsonschema.NewCompiler()
	assert.NoError(t, c.AddResource(ID, doc))
	compiled, err := c.Compile(ID)
	assert.NoError(t, err)

	paths, err := filepath.Glob("../../dev/config/examples/*.yml")
	assert.NoError(t, err)
	assert.That(t, len(paths) > 0)

	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			rendered, err := config.RenderTemplate(p, nil)
			assert.NoError(t, err)

			var v any
			assert.NoError(t, yaml.Unmarshal(rendered, &v))

			norm, err := jsonRoundTrip(v)
			assert.NoError(t, err)

			if err := compiled.Validate(norm); err != nil {
				t.Fatalf("generated schema rejects a shipped example:\n%v", err)
			}
		})
	}
}

func enumAt(t *testing.T, doc map[string]any, path []string) []string {
	t.Helper()

	node := doc["properties"].(map[string]any)
	for i, key := range path {
		next, ok := node[key].(map[string]any)
		if !ok {
			t.Fatalf("path %v: %q missing at depth %d", path, key, i)
		}
		if i == len(path)-1 {
			raw, ok := next["enum"].([]any)
			if !ok {
				t.Fatalf("path %v: no enum", path)
			}
			out := make([]string, 0, len(raw))
			for _, v := range raw {
				out = append(out, v.(string))
			}
			return out
		}
		props, ok := next["properties"].(map[string]any)
		if !ok {
			t.Fatalf("path %v: no properties under %q", path, key)
		}
		node = props
	}
	return nil
}
