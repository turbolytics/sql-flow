package turbostats

import (
	"context"
	"encoding/json"
	"net/http"
)

// Handler serves one bundle per GET. The caller mounts it at /turbostats/v1;
// the path is the caller's, the media type is this package's.
//
// onError receives a collection failure, and may be nil. The response never
// carries it. The route takes no client id, and the error can be a database's:
// under `run`, Collect wraps the state backend's error, and a database error
// can carry its connection string, password included. The caller logs it, with
// whatever redaction it owns. This package cannot redact for it: the redactor
// lives in internal/serve, which imports this package.
func Handler(collect func(context.Context) (Bundle, error), onError func(error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, err := collect(r.Context())
		if err != nil {
			if onError != nil {
				onError(err)
			}
			// A 500 rather than an empty success: a monitoring system must
			// see the failure, not a healthy-looking blank. It learns that
			// the bundle failed and nothing about why.
			http.Error(w, "building bundle failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", MediaType)
		if err := json.NewEncoder(w).Encode(b); err != nil {
			return
		}
	})
}
