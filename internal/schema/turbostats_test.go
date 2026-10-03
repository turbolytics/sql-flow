package schema

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
	"github.com/zeebo/assert"
)

// wireDir is where the reflector reads the contract's doc comments.
const wireDir = "../../turbostats/wire"

const (
	bundleGoldenPath   = "../../turbostats/wire/schema/bundle.schema.json"
	responseGoldenPath = "../../turbostats/wire/schema/response.schema.json"
)

// The TurboStats schemas are generated, so the committed files are build
// artifacts and these are their golden tests. Regenerate with `make schema`.
func TestTurboStatsSchema_CommittedFilesMatchTheTypes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, tt := range []struct {
		path string
		gen  func(string) ([]byte, error)
	}{
		{bundleGoldenPath, GenerateTurboStatsBundle},
		{responseGoldenPath, GenerateTurboStatsResponse},
	} {
		generated, err := tt.gen(wireDir)
		assert.NoError(t, err)
		if os.Getenv("UPDATE_GOLDEN") == "1" {
			assert.NoError(t, os.WriteFile(tt.path, generated, 0o644))
			continue
		}
		committed, err := os.ReadFile(tt.path)
		assert.NoError(t, err)
		if !bytes.Equal(committed, generated) {
			t.Fatalf("%s is stale. Run `make schema`.", tt.path)
		}
	}
}

// The embedded documents are the committed files. A package that embedded
// something else would serve a schema no test checked.
func TestTurboStatsSchema_TheEmbeddedFilesAreTheCommittedOnes(t *testing.T) {
	committed, err := os.ReadFile(bundleGoldenPath)
	assert.NoError(t, err)
	assert.That(t, bytes.Equal(committed, tsschema.Bundle))
	committed, err = os.ReadFile(responseGoldenPath)
	assert.NoError(t, err)
	assert.That(t, bytes.Equal(committed, tsschema.Response))
}

// Every reader ignores unknown sections and fields, and a new one is an
// additive v1 change. A schema that forbade one would make a deployed
// validator reject a newer engine.
func TestTurboStatsSchema_NoObjectForbidsUnknownFields(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, doc := range turboStatsDocs(t) {
		walkJSON(doc, func(node map[string]any) {
			if v, ok := node["additionalProperties"]; ok && v == false {
				t.Errorf("an object forbids unknown fields: %v", node)
			}
		})
	}
}

// A new state, basis or runtime name is an additive v1 change. An enum
// would make an old validator reject it.
func TestTurboStatsSchema_CarriesNoEnum(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, doc := range turboStatsDocs(t) {
		walkJSON(doc, func(node map[string]any) {
			if _, ok := node["enum"]; ok {
				t.Errorf("a field carries an enum: %v", node)
			}
		})
	}
}

// Buckets are summed across a fleet, which works only while every process
// sends the same number of them. The count comes from the code.
func TestTurboStatsSchema_BucketsHaveTheContractsLength(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	want := float64(len(wire.DurationBounds) + 1)
	found := 0
	walkJSON(turboStatsDocs(t)[0], func(node map[string]any) {
		props, ok := node["properties"].(map[string]any)
		if !ok {
			return
		}
		buckets, ok := props["buckets"].(map[string]any)
		if !ok {
			return
		}
		found++
		assert.Equal(t, want, buckets["minItems"])
		assert.Equal(t, want, buckets["maxItems"])
	})
	// Batch, sink flush and serve request.
	assert.Equal(t, 3, found)
}

// required follows the json tags. A field without omitempty is required,
// and goroutines stopped being required so a JVM need not send it.
func TestTurboStatsSchema_RequiredFollowsTheTags(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := turboStatsDocs(t)[0]
	assert.DeepEqual(t, []any{"v", "sent_at", "instance", "process"}, doc["required"])
	process := doc["properties"].(map[string]any)["process"].(map[string]any)
	assert.DeepEqual(t, []any{"started_at"}, process["required"])
}

// turboStatsDocs is the bundle schema, then the response schema, decoded.
func turboStatsDocs(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, gen := range []func(string) ([]byte, error){GenerateTurboStatsBundle, GenerateTurboStatsResponse} {
		raw, err := gen(wireDir)
		assert.NoError(t, err)
		var doc map[string]any
		assert.NoError(t, json.Unmarshal(raw, &doc))
		out = append(out, doc)
	}
	return out
}

// walkJSON calls fn on every object in a decoded JSON document.
func walkJSON(v any, fn func(map[string]any)) {
	switch node := v.(type) {
	case map[string]any:
		fn(node)
		for _, child := range node {
			walkJSON(child, fn)
		}
	case []any:
		for _, child := range node {
			walkJSON(child, fn)
		}
	}
}
