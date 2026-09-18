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

// An absent block and an empty field both take the default. An empty field is
// what a template renders for an unset variable, and it must not reach
// net.Listen, which reads "" as every interface on a port the kernel picks.
func TestConfigTemplating_WebhookAddr(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *WebhookSource
	addr, err := absent.ResolvedAddr()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookAddr, addr)

	addr, err = (&WebhookSource{}).ResolvedAddr()
	assert.NoError(t, err)
	assert.Equal(t, DefaultWebhookAddr, addr)

	for _, ok := range []string{"0.0.0.0:10000", "127.0.0.1:0", ":8001"} {
		addr, err = (&WebhookSource{Addr: ok}).ResolvedAddr()
		assert.NoError(t, err)
		assert.Equal(t, ok, addr)
	}

	for _, bad := range []string{"nonsense", "host", "0.0.0.0:http", "0.0.0.0:70000"} {
		_, err = (&WebhookSource{Addr: bad}).ResolvedAddr()
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "addr"))
		assert.That(t, strings.Contains(err.Error(), bad))
	}
}
