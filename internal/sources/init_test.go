package sources

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/turbolytics/sql-flow/internal/websocket"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

func TestSinkRetry_NewWebsocket(t *testing.T) {
	s, err := New(config.Source{
		Type:      "websocket",
		Websocket: &config.WebsocketSource{URI: "ws://localhost:1234/subscribe"},
	}, zap.NewNop(), nil)
	assert.NoError(t, err)

	_, ok := s.(*websocket.Source)
	assert.That(t, ok)
}

func TestSinkRetry_NewWebsocketRequiresURI(t *testing.T) {
	_, err := New(config.Source{Type: "websocket"}, zap.NewNop(), nil)
	assert.Error(t, err)
}

func TestSinkRetry_NewWebhook(t *testing.T) {
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
	}, zap.NewNop(), nil)
	assert.NoError(t, err)

	src, ok := s.(*webhook.Source)
	assert.That(t, ok)
	assert.NoError(t, src.Close())
}

// signature_type is what turns HMAC verification on, matching
// sqlflow/sources/__init__.py.
func TestSinkRetry_NewWebhookWithoutSignatureType(t *testing.T) {
	s, err := New(config.Source{
		Type:    "webhook",
		Webhook: &config.WebhookSource{},
	}, zap.NewNop(), nil)
	assert.NoError(t, err)

	src := s.(*webhook.Source)
	assert.Nil(t, src.HMACConfig())
	assert.NoError(t, src.Close())
}

// The webhook source is the only one that records its own metrics, so the
// provider has to survive the trip through New.
func TestSinkRetry_NewWebhookRecordsRequestMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s, err := New(
		config.Source{Type: "webhook", Webhook: &config.WebhookSource{}},
		zap.NewNop(),
		sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	)
	assert.NoError(t, err)
	defer s.(*webhook.Source).Close()

	srv := httptest.NewServer(s.(*webhook.Source).Handler())
	defer srv.Close()

	go func() {
		for range s.Stream() {
		}
	}()

	resp, err := http.Post(srv.URL+"/events", "application/json", strings.NewReader(`{"a":1}`))
	assert.NoError(t, err)
	resp.Body.Close()

	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))

	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			found = found || m.Name == "webhook_requests_total"
		}
	}
	assert.That(t, found)
}

func TestSinkRetry_NewUnsupportedSource(t *testing.T) {
	_, err := New(config.Source{Type: "carrier-pigeon"}, zap.NewNop(), nil)
	assert.Error(t, err)
}
