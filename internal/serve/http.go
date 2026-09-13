package serve

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"go.uber.org/zap"
)

const (
	shutdownTimeout   = 5 * time.Second
	readHeaderTimeout = 10 * time.Second
)

// Handler serves the routes. Every route is GET.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/v1/datasets", s.authed(s.listDatasets))
	mux.HandleFunc("/v1/datasets/{name}", s.authed(s.queryDataset))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, &apiError{http.StatusNotFound, "not_found", "no route " + r.URL.Path})
	})

	// Cheap rejections first: CORS, then the method, then auth inside the
	// routes, all before anything waits on the connection's lock.
	return s.logRequests(s.cors(getOnly(mux)))
}

// Serve answers on ln until ctx ends, then stops accepting and gives
// in-flight requests shutdownTimeout to finish.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: readHeaderTimeout}

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// A request still waiting on a query past the drain. Its handler
		// returns at its own deadline; the connection is cut now.
		s.logger.Warn("requests still running at shutdown", zap.Error(err))
		_ = srv.Close()
	}
	if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	_, err := s.exec.run(r.Context(), s.healthTimeout,
		func(ctx context.Context, conn adbc.Connection) (result, error) {
			stmt, err := conn.NewStatement()
			if err != nil {
				return result{}, err
			}
			defer stmt.Close()
			if err := stmt.SetSqlQuery("SELECT 1"); err != nil {
				return result{}, err
			}
			rdr, _, err := stmt.ExecuteQuery(ctx)
			if err != nil {
				return result{}, err
			}
			defer rdr.Release()
			return readRows(rdr, 1)
		})
	if err != nil {
		s.logger.Warn("health check failed", zap.String("error", Redact(err.Error())))
		writeJSON(w, r, http.StatusServiceUnavailable, []byte(`{"status":"unavailable"}`))
		return
	}
	writeJSON(w, r, http.StatusOK, []byte(`{"status":"ok"}`))
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, s.listing)
}

// rowsResponse is a data request's body.
type rowsResponse struct {
	Dataset   string          `json:"dataset"`
	Grain     string          `json:"grain,omitempty"`
	Columns   []column        `json:"columns"`
	Rows      json.RawMessage `json:"rows"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
	ElapsedMS int64           `json:"elapsed_ms"`
}

func (s *Server) queryDataset(w http.ResponseWriter, r *http.Request) {
	entry := entryFrom(r)
	name := r.PathValue("name")
	entry.dataset = name

	ds, ok := s.datasets[name]
	if !ok {
		writeError(w, r, &apiError{http.StatusNotFound, "unknown_dataset", "no dataset named " + name})
		return
	}

	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, r, &apiError{http.StatusBadRequest, "invalid_param",
			"the query string does not parse: " + err.Error()})
		return
	}

	st, apiErr := ds.resolveStatement(query)
	if apiErr != nil {
		writeError(w, r, apiErr)
		return
	}
	entry.grain = st.grain

	values, apiErr := parseParams(ds.conf.Params, query)
	if apiErr != nil {
		writeError(w, r, apiErr)
		return
	}

	start := time.Now()
	res, err := s.exec.run(r.Context(), ds.timeout,
		func(ctx context.Context, conn adbc.Connection) (result, error) {
			return st.query(ctx, conn, values, ds.maxRows)
		})
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, r, &apiError{http.StatusGatewayTimeout, "query_timeout",
			st.where() + " did not answer within " + ds.timeout.String()})
		return
	case errors.Is(err, context.Canceled):
		// The caller hung up. There is nobody to answer.
		entry.status, entry.code = 499, "client_closed"
		return
	case err != nil:
		// The caller gets no part of DuckDB's error. A token is public, and a
		// Postgres connection error carries the connection string, password
		// included. The log gets it, redacted.
		s.logger.Error("query failed", zap.String("dataset", name),
			zap.String("grain", st.grain), zap.String("error", Redact(err.Error())))
		writeError(w, r, &apiError{http.StatusInternalServerError, "query_failed",
			st.where() + " failed; the server log has the database's error"})
		return
	}

	body, err := json.Marshal(rowsResponse{
		Dataset:   name,
		Grain:     st.grain,
		Columns:   res.Columns,
		Rows:      res.Rows,
		RowCount:  res.RowCount,
		Truncated: res.Truncated,
		ElapsedMS: time.Since(start).Milliseconds(),
	})
	if err != nil {
		writeError(w, r, &apiError{http.StatusInternalServerError, "query_failed", err.Error()})
		return
	}
	entry.rows = res.RowCount
	writeJSON(w, r, http.StatusOK, body)
}

// authed requires a bearer token and records whose it is.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, ok := s.identify(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, r, &apiError{http.StatusUnauthorized, "unauthorized",
				"send a configured token as Authorization: Bearer <token>"})
			return
		}
		entryFrom(r).token = name
		next(w, r)
	}
}

// identify compares the presented token against every configured one in
// constant time, with no early return, so the time taken says nothing about
// which token came closest.
func (s *Server) identify(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}

	name := ""
	for _, t := range s.conf.Serve.Auth.Tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(t.Token)) == 1 {
			name = t.Name
		}
	}
	return name, name != ""
}

// cors answers preflights and marks allowed origins. Without a cors block it
// is not in the chain, and no CORS header is ever sent.
func (s *Server) cors(next http.Handler) http.Handler {
	if s.origins == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		allowed := origin != "" && s.origins[origin]
		if allowed {
			h.Set("Access-Control-Allow-Origin", origin)
		}

		if r.Method == http.MethodOptions {
			// A preflight carries no token, because browsers never send one.
			if allowed {
				h.Set("Access-Control-Allow-Methods", "GET")
				h.Set("Access-Control-Allow-Headers", "Authorization")
				h.Set("Access-Control-Max-Age", "600")
			}
			h.Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNoContent)
			entryFrom(r).status = http.StatusNoContent
			return
		}
		next.ServeHTTP(w, r)
	})
}

func getOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, r, &apiError{http.StatusMethodNotAllowed, "method_not_allowed",
				r.Method + " is not allowed; every route is GET"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// logEntry collects one request's log fields as the handlers learn them.
type logEntry struct {
	token, dataset, grain, code string
	status, rows                int
}

type entryKey struct{}

func entryFrom(r *http.Request) *logEntry {
	if e, ok := r.Context().Value(entryKey{}).(*logEntry); ok {
		return e
	}
	// A handler reached without the logging middleware, as in a test that
	// calls one directly. Its fields go nowhere.
	return &logEntry{}
}

// logRequests writes one line per request. The token's name goes in the
// line; its value never does.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		entry := &logEntry{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), entryKey{}, entry)))

		fields := []zap.Field{
			zap.String("path", r.URL.Path),
			zap.String("token", entry.token),
			zap.String("dataset", entry.dataset),
			zap.String("grain", entry.grain),
			zap.Int("status", entry.status),
			zap.Int("rows", entry.rows),
			zap.Int64("elapsed_ms", time.Since(start).Milliseconds()),
			zap.String("remote", r.RemoteAddr),
		}
		if entry.code != "" {
			fields = append(fields, zap.String("code", entry.code))
		}
		s.logger.Info("request", fields...)
	})
}

// writeJSON writes a JSON body. Every response says no-store, so nothing
// between the caller and the server holds an answer while caching is
// deferred.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	entryFrom(r).status = status
}

func writeError(w http.ResponseWriter, r *http.Request, e *apiError) {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]string{"code": e.code, "message": e.message},
	})
	entryFrom(r).code = e.code
	writeJSON(w, r, e.status, body)
}
