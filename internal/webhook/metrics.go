package webhook

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// meterName is the meter the Python source records under, see
// sqlflow/sources/webhook.py.
const meterName = "sqlflow.sources.http"

// Metrics holds the instruments the webhook source records, mirroring the
// names, descriptions and units of the Python MetricsMiddleware.
type Metrics struct {
	RequestCount    metric.Int64Counter
	RequestDuration metric.Float64Histogram
}

// NewMetrics builds the instruments from a meter provider. Passing a nil or
// noop provider yields instruments that record nothing, so the request path
// needs no nil checks.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter(meterName)

	var (
		m   Metrics
		err error
	)

	if m.RequestCount, err = meter.Int64Counter(
		"webhook_requests_total",
		metric.WithDescription("Total number of requests to the webhook source"),
		metric.WithUnit("requests"),
	); err != nil {
		return nil, fmt.Errorf("webhook_requests_total: %w", err)
	}

	if m.RequestDuration, err = meter.Float64Histogram(
		"webhook_request_duration_seconds",
		metric.WithDescription("Duration of requests to the webhook source in seconds"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("webhook_request_duration_seconds: %w", err)
	}

	return &m, nil
}

// statusRecorder captures the status code the middleware attributes a request
// by. Go only reveals it through the ResponseWriter.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// middleware records every response, routed or not, the way the Python
// BaseHTTPMiddleware wraps the whole application.
func (m *Metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		next.ServeHTTP(rec, r)

		// A handler that returns without writing anything has sent a 200.
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		attrs := metric.WithAttributes(
			attribute.String("status_code", strconv.Itoa(rec.status)),
		)

		m.RequestCount.Add(r.Context(), 1, attrs)
		m.RequestDuration.Record(r.Context(), time.Since(start).Seconds(), attrs)
	})
}
