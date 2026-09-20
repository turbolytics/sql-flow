package serve

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/turbostats"
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
	if s.serveMetrics {
		// No client id: it carries no row data, and the listener is already
		// public. It is absent unless the config turns it on, because the
		// labels name every dataset and grain.
		mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	}
	if s.turbostats != nil {
		// No client id, for the reason /metrics has none: it carries no row
		// data. It is absent unless the command asks, because it names the
		// build and the process's memory on a listener that is public.
		mux.Handle("/turbostats/v1", turbostats.Handler(s.collectBundle))
	}
	mux.HandleFunc("/v1/datasets", s.authed(s.listDatasets))
	mux.HandleFunc("/v1/datasets/{name}", s.authed(s.queryDataset))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, &apiError{http.StatusNotFound, "not_found", "no route " + r.URL.Path})
	})

	// Cheap rejections first: CORS, then the method, then auth inside the
	// routes, all before anything waits for a session.
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
	ctx, cancel := context.WithTimeout(r.Context(), s.healthTimeout)
	defer cancel()

	_, _, err := query(ctx, s.exec, s.health, nil, 1)
	switch {
	case err == nil:
		writeJSON(w, r, http.StatusOK, []byte(`{"status":"ok"}`))
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrClosed):
		// Busy is not dead. A supervisor that cannot tell them apart restarts
		// a server that is merely loaded, at the worst possible moment.
		//
		// A request that timed out leaves its session busy until its query
		// ends, so a pool full of abandoned queries reports busy too. That is
		// the truth about the server.
		writeJSON(w, r, http.StatusServiceUnavailable, []byte(`{"status":"busy"}`))
	default:
		s.logger.Warn("health check failed", zap.String("error", Redact(err.Error())))
		writeJSON(w, r, http.StatusServiceUnavailable, []byte(`{"status":"unavailable"}`))
	}
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, s.listing)
}

// rowsResponse is a data request's body.
type rowsResponse struct {
	Dataset   string          `json:"dataset"`
	Grain     string          `json:"grain,omitempty"`
	Range     *window         `json:"range,omitempty"`
	Columns   []column        `json:"columns"`
	Rows      json.RawMessage `json:"rows"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
	// QueuedMS is how long the request waited for a session, and ElapsedMS is
	// the query alone. Before the pool, elapsed_ms silently included the wait,
	// so a 13 ms query on a loaded server reported a second and sent its
	// reader looking for a slow query that did not exist.
	QueuedMS  int64 `json:"queued_ms"`
	ElapsedMS int64 `json:"elapsed_ms"`
	// Cache and AgeMS are present only for a dataset that opted into the
	// cache. On a hit or a shared fill, queued_ms and elapsed_ms are this
	// request's own wait and work, never the filling request's: a reader
	// must not be sent looking for a slow query this request did not run.
	Cache string `json:"cache,omitempty"`
	AgeMS *int64 `json:"age_ms,omitempty"`
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

	// params, not query: query is the helper that runs one.
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, r, &apiError{http.StatusBadRequest, "invalid_param",
			"the query string does not parse: " + err.Error()})
		return
	}

	values, apiErr := parseParams(ds.conf.Params, params)
	if apiErr != nil {
		writeError(w, r, apiErr)
		return
	}

	// A ranged dataset needs the parsed since and until to choose a grain;
	// any other dataset takes the grain as named.
	var (
		st  datasetStatement
		win *window
	)
	if ds.span != nil {
		st, win, apiErr = ds.resolveRange(params, values, s.now())
	} else {
		st, apiErr = ds.resolveStatement(params)
	}
	if apiErr != nil {
		writeError(w, r, apiErr)
		return
	}
	entry.grain = st.grain

	// The deadline bounds this caller's wait: for a session, and then for the
	// query on it.
	ctx, cancel := context.WithTimeout(r.Context(), ds.timeout)
	defer cancel()

	start := time.Now()
	var (
		res     result
		queued  time.Duration
		work    time.Duration
		outcome cacheOutcome
		age     time.Duration
	)
	if ds.cacheTTL == 0 {
		res, queued, err = query(ctx, s.exec, st.stmt, values, ds.maxRows)
		work = time.Since(start) - queued
		entry.ran = true
	} else {
		// Check guarantees a bucket on every grain of a cached range. The
		// guard is for a Server built from a config nobody checked: it must
		// serve unrounded rather than divide by zero.
		if ds.span != nil {
			if bucket := ds.span.bucketOf(st.grain); bucket > 0 {
				alignRange(ds.span.since, ds.span.until, bucket, values, win)
			}
		}
		// The fill answers to its own deadline, not this caller's. It is the
		// dataset's timeout either way, so a fill nobody waits for is bounded
		// as a request is. values is not written again, so the fill reading
		// it after this request has gone is safe.
		var fillQueued, fillWork time.Duration
		res, outcome, age, err = s.cache.do(ctx, cacheKey(name, st.grain, ds.conf.Params, values), ds.ttlFor(st.grain),
			func() (result, error) {
				fctx, done := context.WithTimeout(context.Background(), ds.timeout)
				defer done()
				began := time.Now()
				r, q, e := query(fctx, s.exec, st.stmt, values, ds.maxRows)
				fillQueued, fillWork = q, time.Since(began)-q
				return r, e
			})
		switch {
		case outcome == cacheMiss && err == nil:
			// Read only after the fill is done, which do waited for.
			queued, work = fillQueued, fillWork
			entry.ran = true
		case outcome == cacheShared:
			queued = time.Since(start)
		}
		entry.cache = string(outcome)
		s.metrics.observeCache(name, outcome)
	}
	// Measured whatever the outcome: the error mix is what the counter is
	// for, so a timeout and a failure count too.
	entry.queryDur, entry.measured = work, true
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// An exhausted pool arrives here too: the wait for a session is the
		// same wait as far as the caller is concerned.
		writeError(w, r, &apiError{http.StatusGatewayTimeout, "query_timeout",
			st.stmt.Where() + " did not answer within " + ds.timeout.String()})
		return
	case errors.Is(err, context.Canceled):
		// The caller hung up. There is nobody to answer.
		entry.status, entry.code = 499, "client_closed"
		return
	case err != nil:
		// The caller gets no part of DuckDB's error. A client id is public, and a
		// Postgres connection error carries the connection string, password
		// included. The log gets it, redacted.
		s.logger.Error("query failed", zap.String("dataset", name),
			zap.String("grain", st.grain), zap.String("error", Redact(err.Error())))
		writeError(w, r, &apiError{http.StatusInternalServerError, "query_failed",
			st.stmt.Where() + " failed; the server log has the database's error"})
		return
	}

	resp := rowsResponse{
		Dataset:   name,
		Grain:     st.grain,
		Range:     win,
		Columns:   res.Columns,
		Rows:      res.Rows,
		RowCount:  res.RowCount,
		Truncated: res.Truncated,
		QueuedMS:  queued.Milliseconds(),
		ElapsedMS: work.Milliseconds(),
	}
	if ds.cacheTTL > 0 {
		ms := age.Milliseconds()
		resp.Cache, resp.AgeMS = string(outcome), &ms
	}
	body, err := json.Marshal(resp)
	if err != nil {
		writeError(w, r, &apiError{http.StatusInternalServerError, "query_failed", err.Error()})
		return
	}
	entry.rows = res.RowCount
	writeJSON(w, r, http.StatusOK, body)
}

// clientIDParam carries the caller's id. It is a query parameter and not a
// header for two reasons. The id identifies and does not authenticate, and an
// Authorization header says otherwise to everyone who reads the page's source.
// A GET with no custom header is also a CORS simple request, so a browser
// sends it without a preflight.
const clientIDParam = "client_id"

// authed requires a configured client id and records whose it is.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, ok := s.identify(r)
		if !ok {
			// Owed for as long as the deprecated bearer form is accepted: a
			// 401 must name a scheme.
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, r, &apiError{http.StatusUnauthorized, "unauthorized",
				"send a configured client id as ?" + clientIDParam + "=<id>"})
			return
		}
		entryFrom(r).client = name
		next(w, r)
	}
}

// identify names the caller from client_id, or from the deprecated
// Authorization: Bearer header when the request has no client_id. A client_id
// that matches nothing is refused without a look at the header: a request
// that answers to two ids would be logged under whichever happened to match.
func (s *Server) identify(r *http.Request) (string, bool) {
	if ids, given := r.URL.Query()[clientIDParam]; given {
		if len(ids) != 1 {
			return "", false
		}
		return s.clientNamed(ids[0])
	}

	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	name, ok := s.clientNamed(token)
	if ok {
		s.bearerWarned.Do(func() {
			s.logger.Warn("a client sent its id as Authorization: Bearer, which the next release refuses; send ?"+
				clientIDParam+"=<id>", zap.String("client", name))
		})
	}
	return name, ok
}

// clientNamed compares the presented id against every configured one in
// constant time, with no early return. An id is public, so the care buys
// little; it costs nothing, and a config author may treat an id as private.
func (s *Server) clientNamed(id string) (string, bool) {
	if id == "" {
		return "", false
	}
	name := ""
	for _, c := range s.clients {
		if subtle.ConstantTimeCompare([]byte(id), []byte(c.ID)) == 1 {
			name = c.Name
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
			// A request that sends client_id and no header needs no preflight.
			// Authorization stays allowed for a page that still sends the
			// deprecated bearer header, which a browser would otherwise block.
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

// getOnly refuses every method but GET, and HEAD on /healthz.
//
// Monitors send HEAD, and for /healthz the status line is the whole answer.
// Every other route stays GET-only on purpose: a HEAD of a dataset would run
// the query, borrow a session and throw the rows away, which spends the pool
// on nothing. net/http suppresses the body of a HEAD response, so the health
// handler needs no special case.
func getOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		health := r.URL.Path == "/healthz"
		if r.Method == http.MethodGet || (health && r.Method == http.MethodHead) {
			next.ServeHTTP(w, r)
			return
		}
		allowed, where := http.MethodGet, "every route is GET, and /healthz is also HEAD"
		if health {
			allowed, where = "GET, HEAD", "/healthz is GET or HEAD"
		}
		w.Header().Set("Allow", allowed)
		writeError(w, r, &apiError{http.StatusMethodNotAllowed, "method_not_allowed",
			r.Method + " is not allowed; " + where})
	})
}

// logEntry collects one request's log fields as the handlers learn them.
type logEntry struct {
	client, dataset, grain, code string
	// cache is how a cached dataset's request was answered, and empty for
	// every other request.
	cache        string
	status, rows int
	// queryDur is the query alone, which only the dataset handler knows. The
	// middleware records the metrics, because that is where the outcome and
	// the total are both known.
	queryDur time.Duration
	measured bool
	// ran is whether this request ran a query. A hit did not, and must not
	// drag the query histogram toward zero.
	ran bool
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

// logRequests writes one line per request. The client's name goes in the
// line. Its id does not: the line logs the path and never the query string.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		entry := &logEntry{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), entryKey{}, entry)))

		fields := []zap.Field{
			zap.String("path", r.URL.Path),
			zap.String("client", entry.client),
			zap.String("dataset", entry.dataset),
			zap.String("grain", entry.grain),
			zap.String("cache", entry.cache),
			zap.Int("status", entry.status),
			zap.Int("rows", entry.rows),
			zap.Int64("elapsed_ms", time.Since(start).Milliseconds()),
			zap.String("remote", r.RemoteAddr),
		}
		if entry.code != "" {
			fields = append(fields, zap.String("code", entry.code))
		}
		s.logger.Info("request", fields...)

		// Only a dataset request carries a query to measure. A listing, a
		// health probe or a refusal would otherwise report a query of zero
		// seconds and flatten the histogram.
		if entry.measured {
			code := entry.code
			if code == "" {
				code = "ok"
			}
			s.metrics.observeRequest(entry.dataset, entry.grain, code, entry.status, entry.queryDur, entry.ran, time.Since(start))
		}
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
