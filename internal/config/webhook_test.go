package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// An absent block and a zero field both take the default; a negative bound
// is a config error that names its field.
func TestConfigTemplating_WebhookMaxBodyBytes(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *WebhookSource
	n, err := absent.ResolvedMaxBodyBytes()
	assert.NoError(t, err)
	assert.Equal(t, int64(DefaultWebhookMaxBodyBytes), n)

	n, err = (&WebhookSource{}).ResolvedMaxBodyBytes()
	assert.NoError(t, err)
	assert.Equal(t, int64(DefaultWebhookMaxBodyBytes), n)

	n, err = (&WebhookSource{MaxBodyBytes: 1 << 20}).ResolvedMaxBodyBytes()
	assert.NoError(t, err)
	assert.Equal(t, int64(1<<20), n)

	_, err = (&WebhookSource{MaxBodyBytes: -1}).ResolvedMaxBodyBytes()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "max_body_bytes"))
}

func TestConfigTemplating_WebhookMaxConnections(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *WebhookSource
	n, err := absent.ResolvedMaxConnections()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookMaxConnections, n)

	n, err = (&WebhookSource{}).ResolvedMaxConnections()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookMaxConnections, n)

	n, err = (&WebhookSource{MaxConnections: 8}).ResolvedMaxConnections()
	assert.NoError(t, err)
	assert.Equal(t, 8, n)

	_, err = (&WebhookSource{MaxConnections: -1}).ResolvedMaxConnections()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "max_connections"))
}
