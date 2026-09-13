package config

import (
	"fmt"
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
	// The statement, for a dataset without grains. Set sql or grains.
	SQL string `yaml:"sql,omitempty"`
	// One statement per grain, selected by ?grain=<name>. Set sql or grains.
	Grains map[string]ServeGrain `yaml:"grains,omitempty"`
}

// ServeParam declares one query parameter.
type ServeParam struct {
	// The query parameter name, and the $name the SQL reads.
	Name string `yaml:"name"`
	// How the value parses and binds: string as VARCHAR, integer as BIGINT,
	// timestamp (RFC 3339 with an offset) as TIMESTAMP WITH TIME ZONE.
	Type string `yaml:"type" jsonschema:"enum=string,enum=integer,enum=timestamp"`
}

// ServeGrain is one grain's statement.
type ServeGrain struct {
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
