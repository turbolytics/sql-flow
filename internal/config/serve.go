package config

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sqlparams"
	"gopkg.in/yaml.v3"
)

// Defaults for a serve file. A zero value in the config means the default.
const (
	DefaultServeAddr           = "0.0.0.0:8080"
	DefaultServeMaxRows        = 10000
	DefaultServeTimeoutSeconds = 10
	// DefaultServePoolSize is the sessions a server holds when the config
	// names no number. Four fits a 256 MB box: idle sessions cost nothing
	// measurable, and a concurrent query costs about 15 MiB.
	DefaultServePoolSize = 4
	// MaxServePoolSize is the most this version accepts. A larger pool on a
	// small box fails queries with out-of-memory rather than queueing them,
	// because DuckDB's memory_limit is one budget shared by every session.
	MaxServePoolSize = 64
	// DefaultServeCacheMaxMB bounds the response cache when the config names
	// no number. Sixteen holds over a hundred of the demo's widest responses
	// and is small beside four sessions' 72 MiB peak on a 256 MB box.
	DefaultServeCacheMaxMB = 16
)

// ServeConf is a whole serve file: the commands that attach the data, and the
// serve block that exposes it.
type ServeConf struct {
	// SQL commands run once at startup, before the server listens. Attach the
	// backend here, READ_ONLY.
	Commands []SQLCommand `yaml:"commands,omitempty"`
	// What to serve, to whom, and within which limits.
	Serve Serve `yaml:"serve"`
}

// Serve declares the HTTP API.
type Serve struct {
	// Names this server in its logs.
	Name string `yaml:"name,omitempty"`
	// Where to listen, and which browser origins may call.
	HTTP *ServeHTTP `yaml:"http,omitempty"`
	// Who may call.
	Auth ServeAuth `yaml:"auth"`
	// Limits for every dataset. A dataset's own non-zero limit overrides one.
	Limits *ServeLimits `yaml:"limits,omitempty"`
	// How many requests the server runs at once. Omit for the default.
	Pool *ServePool `yaml:"pool,omitempty"`
	// Whether to serve Prometheus metrics at /metrics.
	Metrics *ServeMetrics `yaml:"metrics,omitempty"`
	// Bounds the response cache. It does not enable it: a dataset opts in
	// with its own cache block.
	Cache *ServeCache `yaml:"cache,omitempty"`
	// The datasets this server answers. Nothing else is reachable.
	Datasets []ServeDataset `yaml:"datasets"`
}

// ServeHTTP configures the listener.
type ServeHTTP struct {
	// host:port to listen on. Defaults to 0.0.0.0:8080.
	Addr string `yaml:"addr,omitempty"`
	// Browser origins allowed to call. Omit to send no CORS headers.
	CORS *ServeCORS `yaml:"cors,omitempty"`
}

// ServeCORS lists the browser origins allowed to call.
type ServeCORS struct {
	// Exact origins, such as https://turbolytics.io: a scheme, a host and an
	// optional port. No path and no wildcard.
	AllowedOrigins []string `yaml:"allowed_origins"`
}

// ServeAuth lists the bearer tokens the server accepts.
type ServeAuth struct {
	// One entry per client identity.
	Tokens []ServeToken `yaml:"tokens"`
}

// ServeToken names one client identity.
type ServeToken struct {
	// Names the client in the request log.
	Name string `yaml:"name"`
	// The value a client sends as Authorization: Bearer <token>. It is an
	// identifier, not a secret: a browser page ships it in plain sight.
	Token string `yaml:"token"`
}

// ServeLimits bounds one request. The same block appears at the top level
// and on a dataset.
type ServeLimits struct {
	// Most rows one response carries. 0 means 10000. A cut result says
	// truncated: true.
	MaxRows int `yaml:"max_rows,omitempty"`
	// Longest a caller waits for a query, in seconds. 0 means 10. The query
	// itself runs to completion: DuckDB cannot be cancelled.
	TimeoutSeconds int `yaml:"timeout_seconds,omitempty"`
	// Reserved. Parsed so a config written now keeps its shape; refused when
	// any field is non-zero.
	RateLimit *ServeRateLimit `yaml:"rate_limit,omitempty"`
}

// ServeRateLimit is reserved for a later version, which will enforce it.
type ServeRateLimit struct {
	// Reserved. Must be 0.
	RequestsPerSecond float64 `yaml:"requests_per_second,omitempty"`
	// Reserved. Must be 0.
	Burst int `yaml:"burst,omitempty"`
}

// ServePool sizes the sessions a server answers requests on.
type ServePool struct {
	// Sessions the server holds. Each is one backend session, and one
	// request uses one at a time, so this is the requests that run at once.
	// 0 means the default, 4.
	Size int `yaml:"size,omitempty"`
}

// ServeCache bounds the response cache every cached dataset shares.
type ServeCache struct {
	// The most the cache holds, in MiB. 0 means 16.
	MaxMB int `yaml:"max_mb,omitempty"`
}

// ServeDatasetCache opts one dataset into the response cache. For a dataset
// with a range it also asserts that the SQL reads the range as
// bucket >= $since AND bucket < $until, which is what makes rounding the
// range to the bucket exact.
type ServeDatasetCache struct {
	// How long an answer is served after the query that produced it
	// started, in seconds. At least 1.
	TTLSeconds int `yaml:"ttl_seconds" jsonschema:"minimum=1"`
}

// ServeMetrics turns on the Prometheus endpoint.
type ServeMetrics struct {
	// Serve GET /metrics on the same listener as the datasets, without a
	// token. Off by default: that listener is public, and the metric labels
	// name every dataset and grain.
	Enabled bool `yaml:"enabled,omitempty"`
}

// ServeDataset is one named, parameterized SQL statement, or one per grain.
type ServeDataset struct {
	// The URL path segment: /v1/datasets/<name>. Lowercase letters, digits
	// and underscores, starting with a letter.
	Name string `yaml:"name"`
	// Shown in /v1/datasets.
	Description string `yaml:"description,omitempty"`
	// Query parameters the SQL reads as $name. Every statement must use every
	// param. An absent param binds NULL; default it in SQL with coalesce.
	Params []ServeParam `yaml:"params,omitempty"`
	// Overrides the top-level limits for this dataset.
	Limits *ServeLimits `yaml:"limits,omitempty"`
	// Opts the dataset into the response cache. Absent, every request runs
	// its query.
	Cache *ServeDatasetCache `yaml:"cache,omitempty"`
	// The statement, for a dataset without grains. Set sql or grains.
	SQL string `yaml:"sql,omitempty"`
	// One statement per grain, selected by ?grain=<name>. Set sql or grains.
	Grains map[string]ServeGrain `yaml:"grains,omitempty"`
	// Names the params that bound a time range. With a range, a request may
	// omit grain: the server picks the finest grain whose max_range covers
	// the requested range. Requires grains.
	Range *ServeRange `yaml:"range,omitempty"`
}

// ServeRange names a dataset's time range params and the width a request
// gets when it does not give since.
type ServeRange struct {
	// The timestamp param that starts the range. Absent from a request, it
	// binds until minus default.
	Since string `yaml:"since"`
	// The timestamp param that ends the range. Absent from a request, it
	// binds the time the request arrived.
	Until string `yaml:"until"`
	// The width of a range a request does not bound with since, such as 24h.
	// Units: s, m, h, d.
	Default string `yaml:"default"`
}

// ServeParam declares one query parameter.
type ServeParam struct {
	// The query parameter name, and the $name the SQL reads.
	Name string `yaml:"name"`
	// How the value parses and binds: string as VARCHAR, integer as BIGINT,
	// timestamp (RFC 3339 with an offset) as TIMESTAMP WITH TIME ZONE.
	Type string `yaml:"type" jsonschema:"enum=string,enum=integer,enum=timestamp"`
	// The smallest value an integer param accepts. A request below it is
	// refused with 400 invalid_param, never clamped: a clamp answers with less
	// than the caller asked for and does not say so.
	Min *int64 `yaml:"min,omitempty"`
	// The largest value an integer param accepts, refused the same way.
	Max *int64 `yaml:"max,omitempty"`
}

// ServeGrain is one grain's statement.
type ServeGrain struct {
	// How wide one bucket of this grain is, such as 5m. Required on every
	// grain of a cached dataset with a range, and refused without a range.
	// It must divide one day. Units: s, m, h, d.
	Bucket string `yaml:"bucket,omitempty"`
	// The widest range this grain serves, such as 14d. Required when the
	// dataset declares a range, and refused otherwise. Units: s, m, h, d.
	MaxRange string `yaml:"max_range,omitempty"`
	// The statement for this grain.
	SQL string `yaml:"sql"`
}

// Addr is the listen address, defaulted.
func (s Serve) Addr() string {
	if s.HTTP == nil || s.HTTP.Addr == "" {
		return DefaultServeAddr
	}
	return s.HTTP.Addr
}

// MaxRows is the row cap for a dataset: its own, else the top level's, else
// the default.
func (s Serve) MaxRows(ds ServeDataset) int {
	if ds.Limits != nil && ds.Limits.MaxRows > 0 {
		return ds.Limits.MaxRows
	}
	if s.Limits != nil && s.Limits.MaxRows > 0 {
		return s.Limits.MaxRows
	}
	return DefaultServeMaxRows
}

// PoolSize is the sessions to hold, defaulted.
func (s Serve) PoolSize() int {
	if s.Pool != nil && s.Pool.Size > 0 {
		return s.Pool.Size
	}
	return DefaultServePoolSize
}

// MetricsEnabled reports whether to serve /metrics.
func (s Serve) MetricsEnabled() bool {
	return s.Metrics != nil && s.Metrics.Enabled
}

// CacheMaxBytes is the response cache's bound, defaulted.
func (s Serve) CacheMaxBytes() int64 {
	mb := DefaultServeCacheMaxMB
	if s.Cache != nil && s.Cache.MaxMB > 0 {
		mb = s.Cache.MaxMB
	}
	return int64(mb) << 20
}

// AnyCached reports whether any dataset opted into the cache. A server with
// none builds no cache at all.
func (s Serve) AnyCached() bool {
	for _, ds := range s.Datasets {
		if ds.Cache != nil {
			return true
		}
	}
	return false
}

// CacheTTL is how long a dataset's answers are served, or 0 for a dataset
// that did not opt in.
func (ds ServeDataset) CacheTTL() time.Duration {
	if ds.Cache == nil {
		return 0
	}
	return time.Duration(ds.Cache.TTLSeconds) * time.Second
}

// Timeout is how long a caller waits on a dataset, resolved the same way as
// MaxRows. The zero dataset gives the top-level timeout, which /healthz uses.
func (s Serve) Timeout(ds ServeDataset) time.Duration {
	seconds := DefaultServeTimeoutSeconds
	if s.Limits != nil && s.Limits.TimeoutSeconds > 0 {
		seconds = s.Limits.TimeoutSeconds
	}
	if ds.Limits != nil && ds.Limits.TimeoutSeconds > 0 {
		seconds = ds.Limits.TimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

// Statements returns a dataset's statements keyed by grain, in grain order.
// A dataset without grains has one statement under the empty grain.
func (ds ServeDataset) Statements() []ServeStatement {
	if len(ds.Grains) == 0 {
		return []ServeStatement{{SQL: ds.SQL}}
	}
	out := make([]ServeStatement, 0, len(ds.Grains))
	for _, name := range ds.GrainNames() {
		out = append(out, ServeStatement{Grain: name, SQL: ds.Grains[name].SQL})
	}
	return out
}

// GrainNames returns the grain names, sorted. Map order is random, and a
// message that lists grains must read the same on every run.
func (ds ServeDataset) GrainNames() []string {
	names := make([]string, 0, len(ds.Grains))
	for name := range ds.Grains {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GrainsByRange returns the grain names from the narrowest max_range to the
// widest: the order the server tries them in. A grain whose max_range does
// not parse sorts last; Check reports it.
func (ds ServeDataset) GrainsByRange() []string {
	names := ds.GrainNames()
	width := func(name string) time.Duration {
		d, err := ParseServeDuration(ds.Grains[name].MaxRange)
		if err != nil {
			return time.Duration(math.MaxInt64)
		}
		return d
	}
	sort.SliceStable(names, func(i, j int) bool { return width(names[i]) < width(names[j]) })
	return names
}

var serveDurationPattern = regexp.MustCompile(`^([1-9][0-9]*)(s|m|h|d)$`)

// ParseServeDuration reads a range width: a positive whole number and one
// unit, s, m, h or d. Go's own durations have no d, and a range is most
// naturally written in days.
func ParseServeDuration(s string) (time.Duration, error) {
	m := serveDurationPattern.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%q is not a duration such as 30m, 24h or 14d", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is too large", s)
	}
	unit := map[string]time.Duration{"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[m[2]]
	if n > int64(math.MaxInt64/unit) {
		return 0, fmt.Errorf("%q is too large", s)
	}
	return time.Duration(n) * unit, nil
}

// FormatServeDuration writes a duration in the largest whole unit, the form
// ParseServeDuration reads, so a message quotes a width the way a config
// author wrote it.
func FormatServeDuration(d time.Duration) string {
	switch {
	case d > 0 && d%(24*time.Hour) == 0:
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	case d > 0 && d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d > 0 && d%time.Minute == 0:
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	case d > 0 && d%time.Second == 0:
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return d.String()
}

// ServeStatement is one statement of a dataset. Grain is empty for a dataset
// without grains.
type ServeStatement struct {
	Grain string
	SQL   string
}

// LoadServeRendered renders a serve file and decodes it strictly, returning
// both.
func LoadServeRendered(path string, overrides map[string]string) (*ServeConf, []byte, error) {
	rendered, err := RenderTemplate(path, overrides)
	if err != nil {
		return nil, nil, err
	}
	conf, err := ParseServe(rendered)
	if err != nil {
		return nil, nil, err
	}
	return conf, rendered, nil
}

// ParseServe decodes rendered serve text strictly, as LoadRendered does for
// a pipeline.
func ParseServe(rendered []byte) (*ServeConf, error) {
	var conf ServeConf
	if err := decodeStrict(rendered, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// IsServe reports whether rendered text is a serve file: a top-level serve
// key and no pipeline key. Everything else is checked as a pipeline, so a
// file with both keys reports serve as an unknown property.
func IsServe(rendered []byte) bool {
	var top map[string]any
	if err := yaml.Unmarshal(rendered, &top); err != nil {
		return false
	}
	_, serve := top["serve"]
	_, pipeline := top["pipeline"]
	return serve && !pipeline
}

// Violation is one broken serve rule.
type Violation struct {
	Code errs.Code
	// Path is the YAML path to the offending key: mapping keys and sequence
	// indexes, the form validate resolves to a line.
	Path    []string
	Message string
}

var (
	serveNamePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	serveGrainPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_]*$`)
	serveParamTypes   = map[string]bool{"string": true, "integer": true, "timestamp": true}
)

// Check returns every rule the schema cannot express that the config breaks,
// in document order. validate reports each with a position; serve refuses to
// start on any.
func (c *ServeConf) Check() []Violation {
	var out []Violation
	add := func(code errs.Code, path []string, format string, args ...any) {
		out = append(out, Violation{Code: code, Path: path, Message: fmt.Sprintf(format, args...)})
	}

	s := c.Serve

	if s.HTTP != nil && s.HTTP.Addr != "" {
		if !validAddr(s.HTTP.Addr) {
			add(errs.CodeConfigInvalid, []string{"serve", "http", "addr"},
				"serve.http.addr %q is not a host:port", s.HTTP.Addr)
		}
	}
	if s.HTTP != nil && s.HTTP.CORS != nil {
		for i, origin := range s.HTTP.CORS.AllowedOrigins {
			if reason := originProblem(origin); reason != "" {
				add(errs.CodeConfigInvalid,
					[]string{"serve", "http", "cors", "allowed_origins", strconv.Itoa(i)},
					"allowed origin %q %s", origin, reason)
			}
		}
	}

	if len(s.Auth.Tokens) == 0 {
		add(errs.CodeConfigInvalid, []string{"serve", "auth", "tokens"},
			"serve.auth.tokens declares no token, so no request could authenticate")
	}
	names := map[string]bool{}
	values := map[string]string{}
	for i, tok := range s.Auth.Tokens {
		path := []string{"serve", "auth", "tokens", strconv.Itoa(i)}
		switch {
		case tok.Name == "":
			add(errs.CodeConfigInvalid, at(path, "name"), "token %d has no name", i)
		case names[tok.Name]:
			add(errs.CodeConfigInvalid, at(path, "name"), "token name %s is used twice", tok.Name)
		}
		names[tok.Name] = true

		switch {
		case tok.Token == "":
			add(errs.CodeConfigInvalid, at(path, "token"), "token %s has an empty value", tok.Name)
		case values[tok.Token] != "":
			add(errs.CodeConfigInvalid, at(path, "token"),
				"tokens %s and %s have the same value; one identity takes one token",
				values[tok.Token], tok.Name)
		default:
			values[tok.Token] = tok.Name
		}
	}

	checkLimits(s.Limits, []string{"serve", "limits"}, add)

	if s.Pool != nil {
		switch {
		case s.Pool.Size < 0:
			add(errs.CodeConfigInvalid, []string{"serve", "pool", "size"},
				"pool.size is %d; it must not be negative, and 0 means the default", s.Pool.Size)
		case s.Pool.Size > MaxServePoolSize:
			add(errs.CodeConfigInvalid, []string{"serve", "pool", "size"},
				"pool.size %d sessions is more than the %d this version allows; DuckDB's memory limit is one budget shared by every session, so a pool past the box fails queries rather than queueing them",
				s.Pool.Size, MaxServePoolSize)
		}
	}

	if s.Cache != nil && s.Cache.MaxMB < 0 {
		add(errs.CodeConfigInvalid, []string{"serve", "cache", "max_mb"},
			"cache.max_mb is %d; it must not be negative, and 0 means the default", s.Cache.MaxMB)
	}

	if len(s.Datasets) == 0 {
		add(errs.CodeConfigInvalid, []string{"serve", "datasets"},
			"serve.datasets declares no dataset, so the server would answer nothing")
	}
	datasets := map[string]bool{}
	for i, ds := range s.Datasets {
		checkDataset(ds, []string{"serve", "datasets", strconv.Itoa(i)}, datasets, add)
	}

	return out
}

// CheckError folds Check into one error for a process that must refuse to
// start. It carries the first violation's code and lists every violation.
func (c *ServeConf) CheckError() error {
	violations := c.Check()
	if len(violations) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("the serve config is invalid")
	for _, v := range violations {
		fmt.Fprintf(&b, "\n  %s: %s", strings.Join(v.Path, "."), v.Message)
	}
	return errs.New(violations[0].Code, "%s", b.String())
}

type addFunc func(code errs.Code, path []string, format string, args ...any)

func checkLimits(l *ServeLimits, path []string, add addFunc) {
	if l == nil {
		return
	}
	if l.MaxRows < 0 {
		add(errs.CodeConfigInvalid, at(path, "max_rows"),
			"max_rows is %d; it must not be negative, and 0 means the default", l.MaxRows)
	}
	if l.TimeoutSeconds < 0 {
		add(errs.CodeConfigInvalid, at(path, "timeout_seconds"),
			"timeout_seconds is %d; it must not be negative, and 0 means the default", l.TimeoutSeconds)
	}
	if l.RateLimit != nil && (l.RateLimit.RequestsPerSecond != 0 || l.RateLimit.Burst != 0) {
		add(errs.CodeConfigServeReserved, at(path, "rate_limit"),
			"rate_limit is not enforced in this version; remove it or set it to 0")
	}
}

func checkDataset(ds ServeDataset, path []string, seen map[string]bool, add addFunc) {
	child := func(keys ...string) []string { return at(path, keys...) }
	code := errs.CodeConfigServeDataset

	switch {
	case !serveNamePattern.MatchString(ds.Name):
		add(code, child("name"),
			"dataset name %q must be lowercase letters, digits and underscores, starting with a letter", ds.Name)
	case seen[ds.Name]:
		add(code, child("name"), "dataset %s is declared twice", ds.Name)
	}
	seen[ds.Name] = true

	checkLimits(ds.Limits, child("limits"), add)

	declared := map[string]bool{}
	for j, p := range ds.Params {
		ppath := child("params", strconv.Itoa(j))
		switch {
		case !serveNamePattern.MatchString(p.Name):
			add(code, at(ppath, "name"),
				"dataset %s: param name %q must be lowercase letters, digits and underscores, starting with a letter",
				ds.Name, p.Name)
		case p.Name == "grain":
			add(code, at(ppath, "name"),
				"dataset %s: a param cannot be named grain, because grain selects the grain", ds.Name)
		case declared[p.Name]:
			add(code, at(ppath, "name"), "dataset %s: param %s is declared twice", ds.Name, p.Name)
		}
		declared[p.Name] = true
		if !serveParamTypes[p.Type] {
			add(code, at(ppath, "type"),
				"dataset %s: param %s has type %q; use string, integer or timestamp", ds.Name, p.Name, p.Type)
		}
		switch {
		case (p.Min != nil || p.Max != nil) && p.Type != "integer":
			key := "min"
			if p.Min == nil {
				key = "max"
			}
			add(code, at(ppath, key),
				"dataset %s: param %s is a %s; min and max bound integer params only", ds.Name, p.Name, p.Type)
		case p.Min != nil && p.Max != nil && *p.Min > *p.Max:
			add(code, at(ppath, "max"),
				"dataset %s: param %s has min %d above max %d, so no value is accepted", ds.Name, p.Name, *p.Min, *p.Max)
		}
	}

	switch {
	case ds.SQL != "" && len(ds.Grains) > 0:
		add(code, path, "dataset %s sets both sql and grains; set one", ds.Name)
		return
	case ds.SQL == "" && len(ds.Grains) == 0:
		add(code, path, "dataset %s sets neither sql nor grains; set one", ds.Name)
		return
	}

	for _, grain := range ds.GrainNames() {
		if !serveGrainPattern.MatchString(grain) {
			add(code, child("grains", grain),
				"dataset %s: grain name %q must be lowercase letters, digits and underscores", ds.Name, grain)
		}
	}

	checkRange(ds, path, declaredTypes(ds.Params), add)
	checkCache(ds, path, add)

	for _, st := range ds.Statements() {
		spath, where := child("sql"), "dataset "+ds.Name
		if st.Grain != "" {
			spath, where = child("grains", st.Grain, "sql"), where+" grain "+st.Grain
		}

		rw, err := sqlparams.Rewrite(st.SQL)
		if err != nil {
			add(code, spath, "%s: %v", where, err)
			continue
		}
		used := map[string]bool{}
		for _, name := range rw.Names {
			used[name] = true
			if !declared[name] {
				add(code, spath, "%s: $%s is not a declared param", where, name)
			}
		}
		// Declared order, so the message is stable.
		for _, p := range ds.Params {
			if !used[p.Name] {
				add(code, spath,
					"%s does not use param %s; every statement must use every param, "+
						"or a request filtering on it gets unfiltered rows", where, p.Name)
			}
		}
	}
}

// at extends a YAML path into a new slice. Appending to a shared parent
// would let a later sibling overwrite an earlier violation's stored path.
func at(path []string, keys ...string) []string {
	return slices.Concat(path, keys)
}

func declaredTypes(params []ServeParam) map[string]string {
	out := map[string]string{}
	for _, p := range params {
		out[p.Name] = p.Type
	}
	return out
}

// checkRange holds a dataset's range and its grains' max_range to the rules
// grain selection depends on: two timestamp params, a width for each grain
// with no two alike, and a default some grain can serve.
func checkRange(ds ServeDataset, path []string, types map[string]string, add addFunc) {
	code := errs.CodeConfigServeDataset

	if ds.Range == nil {
		for _, grain := range ds.GrainNames() {
			if ds.Grains[grain].MaxRange != "" {
				add(code, at(path, "grains", grain, "max_range"),
					"dataset %s grain %s: max_range needs a range on the dataset naming its since and until params",
					ds.Name, grain)
			}
		}
		return
	}

	rpath := at(path, "range")
	for _, ref := range []struct{ key, name string }{{"since", ds.Range.Since}, {"until", ds.Range.Until}} {
		switch typ, ok := types[ref.name]; {
		case !ok:
			add(code, at(rpath, ref.key), "dataset %s: range.%s names %q, which is not a declared param",
				ds.Name, ref.key, ref.name)
		case typ != "timestamp":
			add(code, at(rpath, ref.key), "dataset %s: range.%s names param %s, which is a %s; it must be a timestamp",
				ds.Name, ref.key, ref.name, typ)
		}
	}
	if ds.Range.Since != "" && ds.Range.Since == ds.Range.Until {
		add(code, at(rpath, "until"), "dataset %s: range.since and range.until both name %s", ds.Name, ds.Range.Since)
	}

	def, defErr := ParseServeDuration(ds.Range.Default)
	if defErr != nil {
		add(code, at(rpath, "default"), "dataset %s: range.default %v", ds.Name, defErr)
	}

	if len(ds.Grains) == 0 {
		add(code, rpath, "dataset %s: a range selects among grains, and the dataset has none", ds.Name)
		return
	}

	widest := time.Duration(0)
	seen := map[time.Duration]string{}
	for _, grain := range ds.GrainNames() {
		gpath := at(path, "grains", grain, "max_range")
		raw := ds.Grains[grain].MaxRange
		if raw == "" {
			add(code, gpath, "dataset %s grain %s: a dataset with a range needs max_range on every grain", ds.Name, grain)
			continue
		}
		d, err := ParseServeDuration(raw)
		if err != nil {
			add(code, gpath, "dataset %s grain %s: max_range %v", ds.Name, grain, err)
			continue
		}
		if other, dup := seen[d]; dup {
			add(code, gpath, "dataset %s: grains %s and %s both have max_range %s, so neither is the finer choice",
				ds.Name, other, grain, FormatServeDuration(d))
			continue
		}
		seen[d] = grain
		if d > widest {
			widest = d
		}
	}

	if defErr == nil && widest > 0 && def > widest {
		add(code, at(rpath, "default"), "dataset %s: range.default %s is wider than the widest grain's max_range %s",
			ds.Name, FormatServeDuration(def), FormatServeDuration(widest))
	}
}

// checkCache holds a dataset's cache block and its grains' buckets to the
// rules the cache key depends on. The key rounds a range up to the bucket,
// which is exact only for buckets aligned to the epoch, and a width that
// divides one day is aligned the way time_bucket and date_trunc align it. A
// week is not: date_trunc('week') starts on a Monday and the epoch was a
// Thursday.
func checkCache(ds ServeDataset, path []string, add addFunc) {
	code := errs.CodeConfigServeDataset

	if ds.Cache != nil && ds.Cache.TTLSeconds < 1 {
		add(code, at(path, "cache", "ttl_seconds"),
			"dataset %s: cache.ttl_seconds is %d; it must be at least 1", ds.Name, ds.Cache.TTLSeconds)
	}

	for _, grain := range ds.GrainNames() {
		gpath := at(path, "grains", grain, "bucket")
		raw := ds.Grains[grain].Bucket
		switch {
		case raw == "" && ds.Cache != nil && ds.Range != nil:
			add(code, gpath, "dataset %s grain %s: a cached dataset with a range needs bucket on every grain",
				ds.Name, grain)
		case raw == "":
		case ds.Range == nil:
			add(code, gpath, "dataset %s grain %s: bucket needs a range on the dataset; without one there is nothing to align",
				ds.Name, grain)
		default:
			d, err := ParseServeDuration(raw)
			if err != nil {
				add(code, gpath, "dataset %s grain %s: bucket %v", ds.Name, grain, err)
				continue
			}
			if (24*time.Hour)%d != 0 {
				add(code, gpath, "dataset %s grain %s: bucket %s does not divide one day, so its buckets are not aligned to the epoch",
					ds.Name, grain, raw)
			}
		}
	}
}

func validAddr(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 0 && n <= 65535
}

// originProblem returns why an allowed_origins entry can never match a
// browser's Origin header, or "" when it can.
func originProblem(origin string) string {
	u, err := url.Parse(origin)
	switch {
	case err != nil:
		return "does not parse as a URL"
	case u.Scheme != "http" && u.Scheme != "https":
		return "must start with http:// or https://"
	case u.Host == "" || strings.Contains(u.Host, "*"):
		return "must name one host; wildcards are not supported"
	case u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || strings.HasSuffix(origin, "?"):
		return "must be a scheme, a host and an optional port, with no path"
	case origin != strings.ToLower(origin):
		return "must be lowercase, because browsers send origins lowercase and they are compared exactly"
	}
	return ""
}
