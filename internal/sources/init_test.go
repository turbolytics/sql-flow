package sources

import (
	"testing"

	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/webhook"
	"github.com/turbolytics/turbine/internal/websocket"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

func TestNew_Websocket(t *testing.T) {
	s, err := New(config.Source{
		Type:      "websocket",
		Websocket: &config.WebsocketSource{URI: "ws://localhost:1234/subscribe"},
	}, zap.NewNop())
	assert.NoError(t, err)

	_, ok := s.(*websocket.Source)
	assert.That(t, ok)
}

func TestNew_WebsocketRequiresURI(t *testing.T) {
	_, err := New(config.Source{Type: "websocket"}, zap.NewNop())
	assert.Error(t, err)
}

func TestNew_Webhook(t *testing.T) {
	s, err := New(config.Source{
		Type: "webhook",
		Webhook: &config.WebhookSource{
			SignatureType: "hmac",
			HMAC: &config.WebhookHMAC{
				Header: "X-Hub-Signature-256",
				SigKey: "sha256",
				Secret: "shhh",
			},
		},
	}, zap.NewNop())
	assert.NoError(t, err)

	src, ok := s.(*webhook.Source)
	assert.That(t, ok)
	assert.NoError(t, src.Close())
}

// signature_type is what turns HMAC verification on, matching
// sqlflow/sources/__init__.py.
func TestNew_WebhookWithoutSignatureType(t *testing.T) {
	s, err := New(config.Source{
		Type:    "webhook",
		Webhook: &config.WebhookSource{},
	}, zap.NewNop())
	assert.NoError(t, err)

	src := s.(*webhook.Source)
	assert.Nil(t, src.HMACConfig())
	assert.NoError(t, src.Close())
}

func TestNew_UnsupportedSource(t *testing.T) {
	_, err := New(config.Source{Type: "carrier-pigeon"}, zap.NewNop())
	assert.Error(t, err)
}
