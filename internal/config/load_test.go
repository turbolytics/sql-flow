package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

func renderString(t *testing.T, body string, overrides map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := RenderTemplate(path, overrides)
	if err != nil {
		t.Fatalf("render %q: %v", body, err)
	}
	return strings.TrimSpace(string(out))
}

// The config spec is Jinja2: filter arguments are parenthesized. pongo2's
// colon syntax is not interchangeable, and every example config uses Jinja.
func TestRenderTemplate_JinjaDefaultFilter(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		overrides map[string]string
		want      string
	}{
		{
			name: "default applies when var is absent",
			body: `brokers: {{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}`,
			want: `brokers: localhost:9092`,
		},
		{
			name:      "override wins over default",
			body:      `brokers: {{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}`,
			overrides: map[string]string{"SQLFLOW_KAFKA_BROKERS": "kafka1:19092"},
			want:      `brokers: kafka1:19092`,
		},
		{
			name: "numeric default",
			body: `batch_size: {{ SQLFLOW_BATCH_SIZE|default(1000) }}`,
			want: `batch_size: 1000`,
		},
		{
			name: "double-quoted default",
			body: `path: {{ SQLFLOW_ATTACH_DB_PATH|default("/tmp/sqlflow/test.db") }}`,
			want: `path: /tmp/sqlflow/test.db`,
		},
		{
			name:      "plain substitution",
			body:      `topic: {{ SQLFLOW_TOPIC }}`,
			overrides: map[string]string{"SQLFLOW_TOPIC": "events"},
			want:      `topic: events`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, renderString(t, tt.body, tt.overrides))
		})
	}
}

// The Python engine seeds the context with these, and configs reference them
// unqualified.
func TestRenderTemplate_ProvidesSettingsVars(t *testing.T) {
	out := renderString(t, `root: {{ STATIC_ROOT }}/x.csv`, nil)
	assert.Equal(t, "root: /tmp/sqlflow/static/x.csv", out)

	out = renderString(t, `cache: {{ SQL_RESULTS_CACHE_DIR }}`, nil)
	assert.Equal(t, "cache: /tmp/sqlflow/resultscache", out)
}

func TestRenderTemplate_SettingsVarsHonorEnvOverrides(t *testing.T) {
	t.Setenv("SQLFLOW_STATIC_ROOT", "/data/static")
	out := renderString(t, `root: {{ STATIC_ROOT }}`, nil)
	assert.Equal(t, "root: /data/static", out)
}

// Every shipped config must render and parse, so a config written for the
// Python engine runs on turbine unmodified.
func TestLoad_AllExampleConfigs(t *testing.T) {
	examples, err := filepath.Glob("../../dev/config/examples/*.yml")
	assert.NoError(t, err)
	assert.That(t, len(examples) > 0)

	for _, path := range examples {
		t.Run(filepath.Base(path), func(t *testing.T) {
			conf, err := Load(path, map[string]string{
				// Supplied by the integration tests for configs that have no
				// default for them.
				"catalog_name": "test_catalog",
				"table_name":   "default.test_table",
			})
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			assert.That(t, conf.Pipeline.Source.Type != "")
		})
	}
}

// The webhook block's keys are the Python engine's, see the WebhookSource and
// HMACConfig dataclasses in sqlflow/config.py.
func TestLoad_WebhookSourceKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "webhook.yml")
	body := `
pipeline:
  source:
    type: webhook
    webhook:
      signature_type: 'hmac'
      hmac:
        header: 'X-Hub-Signature-256'
        sig_key: 'sha256'
        secret: 'shhh'
  handler:
    type: 'handlers.InferredMemBatch'
    sql: SELECT 1
  sink:
    type: noop
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	conf, err := Load(path, nil)
	assert.NoError(t, err)
	assert.Equal(t, "webhook", conf.Pipeline.Source.Type)
	assert.NotNil(t, conf.Pipeline.Source.Webhook)
	assert.Equal(t, "hmac", conf.Pipeline.Source.Webhook.SignatureType)
	assert.NotNil(t, conf.Pipeline.Source.Webhook.HMAC)
	assert.Equal(t, "X-Hub-Signature-256", conf.Pipeline.Source.Webhook.HMAC.Header)
	assert.Equal(t, "sha256", conf.Pipeline.Source.Webhook.HMAC.SigKey)
	assert.Equal(t, "shhh", conf.Pipeline.Source.Webhook.HMAC.Secret)
}
