package turbostats

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
	"github.com/zeebo/assert"
)

func compiled(t *testing.T, id string, doc []byte) *jsonschema.Schema {
	t.Helper()
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	assert.NoError(t, err)
	c := jsonschema.NewCompiler()
	assert.NoError(t, c.AddResource(id, parsed))
	s, err := c.Compile(id)
	assert.NoError(t, err)
	return s
}

// asJSON is v as the JSON data model a validator reads.
func asJSON(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	assert.NoError(t, err)
	var out any
	assert.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func validBundle(t *testing.T, v any) {
	t.Helper()
	if err := compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(asJSON(t, v)); err != nil {
		t.Fatalf("the schema rejects a bundle the contract allows:\n%v", err)
	}
}

// Every bundle the engine builds is one the published schema accepts: a
// run, a serve and a rollup process, and the widest bundle the contract
// allows. A schema that rejected one would fail a reporter in another
// language that copied the engine exactly.
func TestSchema_EveryBundleTheEngineBuildsValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()

	reader, m, _ := provider(t)
	m.MessageCount.Add(ctx, 7)
	m.ErrorRowsDropped.Add(ctx, 1)
	stats := func(context.Context) (*core.StateStats, error) {
		return &core.StateStats{SizeBytes: 4096}, nil
	}
	run, err := Collect(ctx, runSource(reader, stats))
	assert.NoError(t, err)
	run.Exit = &Exit{
		Reason: "SIGTERM",
		Code:   0,
	}

	serveReader, _, _ := provider(t)
	serve, err := Collect(ctx, Source{
		Static: static,
		Reader: serveReader,
		Serve:  &ServeSource{},
	})
	assert.NoError(t, err)

	rollupReader, _, _ := provider(t)
	rollup, err := Collect(ctx, Source{
		Static: static,
		Reader: rollupReader,
		Rollup: &RollupSource{
			Section: func() (*Rollup, *Freshness) {
				return &Rollup{Role: "standby"}, nil
			},
		},
	})
	assert.NoError(t, err)

	for name, b := range map[string]Bundle{
		"run":    run,
		"serve":  serve,
		"rollup": rollup,
		"widest": widestBundle(t),
	} {
		t.Run(name, func(t *testing.T) { validBundle(t, b) })
	}
}

// A fleet mixes engine versions. A bundle from an engine that predates
// every field added since the first receiver must still validate.
func TestSchema_AnOlderEnginesBundleValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	older := `{"v":1,"sent_at":"2026-09-01T00:00:00Z",
		"instance":{"version":"v1","commit":"abc","arch":"linux/arm64","config_hash":"sha256:00"},
		"process":{"started_at":"2026-09-01T00:00:00Z","goroutines":12},
		"pipeline":{"message_count":1,"handler_rows_read":1,"error_count":0,
			"sink_flush_count":0,"sink_rows_accepted":0,"sink_rows_written":0,
			"state_commit_count":0}}`
	var v any
	assert.NoError(t, json.Unmarshal([]byte(older), &v))
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(v))
}

// An engine newer than this schema adds a field, a section and a state
// value. Today's schema must accept all three, or a deployed validator
// rejects the next release.
func TestSchema_ANewerEnginesBundleValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := asJSON(t, widestBundle(t)).(map[string]any)
	doc["future_section"] = map[string]any{"x": 1}
	pipeline := doc["pipeline"].(map[string]any)
	pipeline["future_field"] = 2
	pipeline["state"] = "draining"
	pipeline["backfill"].(map[string]any)["state"] = "throttled"
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(doc))
}

// The schema is not vacuous. A document without the required sections is
// not a bundle; the signing vectors' {"v":1} body is one such document.
func TestSchema_ADocumentWithoutTheRequiredSectionsIsRejected(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var v any
	assert.NoError(t, json.Unmarshal([]byte(`{"v":1}`), &v))
	assert.Error(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(v))
}

// A JVM sends no goroutines. Its bundle must validate.
func TestSchema_ABundleWithoutGoroutinesValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := asJSON(t, widestBundle(t)).(map[string]any)
	delete(doc["process"].(map[string]any), "goroutines")
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(doc))
}

// The response a control plane answers with, with and without commands.
func TestSchema_TheResponseValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	s := compiled(t, tsschema.ResponseID, tsschema.Response)
	for _, body := range []string{
		`{"v":1,"commands":[]}`,
		`{"v":1,"commands":[{"id":"c1","verb":"restart"}],"later":true}`,
	} {
		var v any
		assert.NoError(t, json.Unmarshal([]byte(body), &v))
		assert.NoError(t, s.Validate(v))
	}
}
