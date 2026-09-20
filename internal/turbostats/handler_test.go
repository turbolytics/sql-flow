package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestHandler_ServesOneBundleWithTheMediaType(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{V: Version, Instance: Instance{Version: "v9"}}, nil
	}, nil)

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
//
// It must not see why. The route takes no client id, and the error can be a
// database's: under `run`, Collect wraps the state backend's error, and a
// database error can carry its connection string. The caller gets a fixed
// body and the operator gets the error, through onError, to log as it sees
// fit.
func TestHandler_ReportsACollectionFailureWithoutItsText(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	const secret = `connecting to "postgresql://app:hunter2@db/state"`
	var logged error
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{}, errors.New(secret)
	}, func(err error) { logged = err })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.That(t, !strings.Contains(rec.Body.String(), "hunter2"))
	assert.That(t, !strings.Contains(rec.Body.String(), "postgresql"))
	assert.That(t, logged != nil)
	assert.Equal(t, secret, logged.Error())
}

// A caller with nowhere to log passes nil, and the failure is still a 500.
func TestHandler_ANilOnErrorIsAllowed(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) {
		return Bundle{}, errors.New("reader closed")
	}, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandler_RejectsAnythingButGET(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	h := Handler(func(context.Context) (Bundle, error) { return Bundle{V: Version}, nil }, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
