package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect returns the named instrument, failing the test when the source never
// recorded it.
func collect(t *testing.T, r *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}

	t.Fatalf("instrument %q was never recorded", name)
	return metricdata.Metrics{}
}

// drain consumes the queue so requests never block on the pipeline.
func drain(s *Source) {
	go func() {
		for range s.Stream() {
		}
	}()
}

// Names, units and descriptions come from sqlflow/sources/webhook.py, where
// the meter is created as 'sqlflow.sources.http'.
func TestMetrics_MatchPythonInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s, err := NewSource(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.NoError(t, err)
	defer s.Close()
	drain(s)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "")
	resp.Body.Close()

	count := collect(t, reader, "webhook_requests_total")
	assert.Equal(t, "Total number of requests to the webhook source", count.Description)
	assert.Equal(t, "requests", count.Unit)

	duration := collect(t, reader, "webhook_request_duration_seconds")
	assert.Equal(t, "Duration of requests to the webhook source in seconds", duration.Description)
	assert.Equal(t, "seconds", duration.Unit)
}

// The Python middleware wraps the whole app and attributes every response by
// its status code, so rejected and unrouted requests are counted too.
func TestMetrics_CountRequestsByStatusCode(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s, err := NewSource(
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		WithHMAC(&HMAC{Header: "X-HMAC-Signature", SigKey: "sha256", Secret: "test_secret"}),
	)
	assert.NoError(t, err)
	defer s.Close()
	drain(s)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := []byte(`{"a":1}`)
	for i := 0; i < 2; i++ {
		resp := post(t, srv.URL+"/events", body, "X-HMAC-Signature", sign("test_secret", body))
		resp.Body.Close()
	}
	// Missing signature: 400.
	resp := post(t, srv.URL+"/events", body, "", "")
	resp.Body.Close()
	// Unrouted: 404, still seen by the middleware.
	resp = post(t, srv.URL+"/nope", body, "", "")
	resp.Body.Close()

	sum, ok := collect(t, reader, "webhook_requests_total").Data.(metricdata.Sum[int64])
	assert.That(t, ok)

	counts := map[string]int64{}
	for _, dp := range sum.DataPoints {
		code, found := dp.Attributes.Value(attribute.Key("status_code"))
		assert.That(t, found)
		counts[code.AsString()] = dp.Value
	}
	assert.Equal(t, int64(2), counts["200"])
	assert.Equal(t, int64(1), counts["400"])
	assert.Equal(t, int64(1), counts["404"])
}

func TestMetrics_RecordRequestDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s, err := NewSource(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.NoError(t, err)
	defer s.Close()
	drain(s)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "")
	resp.Body.Close()

	hist, ok := collect(t, reader, "webhook_request_duration_seconds").Data.(metricdata.Histogram[float64])
	assert.That(t, ok)
	assert.Equal(t, 1, len(hist.DataPoints))

	dp := hist.DataPoints[0]
	assert.Equal(t, uint64(1), dp.Count)
	code, found := dp.Attributes.Value(attribute.Key("status_code"))
	assert.That(t, found)
	assert.Equal(t, "200", code.AsString())
	// Recorded in seconds, so a local request is well under a second.
	assert.That(t, dp.Sum > 0 && dp.Sum < 1)
}

// A source built without a provider must still serve, so the request path
// needs no nil checks.
func TestMetrics_NoProviderRecordsNothing(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)
	defer s.Close()
	drain(s)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
