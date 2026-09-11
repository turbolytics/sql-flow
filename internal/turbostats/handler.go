package turbostats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Handler serves one bundle per GET. The run command mounts it at
// /turbostats/v1; the path is the caller's, the media type is this package's.
func Handler(collect func(context.Context) (Bundle, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, err := collect(r.Context())
		if err != nil {
			// A monitoring system must see the failure, not a healthy-looking
			// blank, which is the rule /stats already follows.
			http.Error(w, fmt.Sprintf("building bundle: %v", err),
				http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", MediaType)
		if err := json.NewEncoder(w).Encode(b); err != nil {
			return
		}
	})
}
