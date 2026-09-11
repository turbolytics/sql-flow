package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestHandler_ServesOneBundleWithTheMediaType(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{V: Version, Instance: Instance{Version: "v9"}}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, MediaType, rec.Header().Get("Content-Type"))
	var got Bundle
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, 1, got.V)
	assert.Equal(t, "v9", got.Instance.Version)
}

// A collection failure is a server error, not an empty success: a monitoring
// system must see it rather than a healthy-looking blank.
func TestHandler_ReportsACollectionFailure(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{}, errors.New("reader closed")
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_RejectsAnythingButGET(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) { return Bundle{V: Version}, nil })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
