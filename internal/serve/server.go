// Package serve answers HTTP requests with the rows of named, parameterized
// SQL statements a serve config declares.
//
// Nothing in a request becomes SQL text. A request names a dataset, a grain
// and param values; the server binds those values into a statement the
// config fixed at startup.
package serve

import (
	"context"
	"encoding/json"
	"fmt"
	prom "github.com/prometheus/client_golang/prometheus"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"go.uber.org/zap"
)

// Server serves one serve config's datasets over a pool of executor sessions.
type Server struct {
	conf   *config.ServeConf
	logger *zap.Logger
	exec   Executor
	// cache is nil unless some dataset opted in. Only a cached dataset's
	// requests reach it.
	cache *cache
	// health is the statement /healthz runs. It is prepared at startup like
	// every other one, so the probe costs a bind and nothing more.
	health Statement

	datasets map[string]*dataset
	// listing is the /v1/datasets body, built once: the config does not change.
	listing []byte
	// origins is nil without a cors block, which sends no CORS header at all.
	origins map[string]bool
	// healthTimeout bounds /healthz, which waits for a session like a query.
	healthTimeout time.Duration
	// registry and metrics are nil unless the config asks for /metrics. Every
	// metrics method tolerates a nil receiver, so the request path records
	// unconditionally.
	registry *prom.Registry
	metrics  *metrics
	// now is when a request arrived: the until a ranged request does not
	// give. A test fixes it.
	now func() time.Time
}

// dataset is one config dataset with its statements prepared.
type dataset struct {
	conf config.ServeDataset
	// single is the statement of a dataset without grains.
	single datasetStatement
	// grains holds one statement per grain.
	grains  map[string]datasetStatement
	maxRows int
	timeout time.Duration
	// cacheTTL is 0 for a dataset that did not opt into the cache.
	cacheTTL time.Duration
	// span is nil for a dataset without a range.
	span *span
}

// datasetStatement is one prepared statement with the grain it answers. A
// Statement belongs to the executor and carries no grain of its own, so the
// grain -- which the response and the log line both name -- travels beside
// it.
type datasetStatement struct {
	stmt Statement
	// grain is empty for a dataset without grains.
	grain string
}

// span is a dataset's range: the params that bound it, the width a request
// gets without since, and each grain's widest range, narrowest first.
type span struct {
	since, until string
	def          time.Duration
	grains       []spanGrain
}

type spanGrain struct {
	name string
	max  time.Duration
	// bucket is 0 for a grain that declares none.
	bucket time.Duration
}

// bucketOf is the width of one of grain's buckets, or 0.
func (sp *span) bucketOf(grain string) time.Duration {
	for _, g := range sp.grains {
		if g.name == grain {
			return g.bucket
		}
	}
	return 0
}

// Option configures a Server.
type Option func(*Server)

// WithLogger sets the logger the request lines go to.
func WithLogger(l *zap.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// WithMetrics registers serve's instruments on reg and serves them at
// /metrics. New builds the instruments, because they read the executor's
// session counts, and installs the wait hook on the executor afterwards.
func WithMetrics(reg *prom.Registry) Option {
	return func(s *Server) { s.registry = reg }
}

// waitObserver is an executor whose pool can report how long an Acquire
// waited. The DuckDB one does; a future one need not, and then the wait
// histogram is simply empty rather than the server failing to start.
type waitObserver interface {
	setOnWait(func(time.Duration))
}

// New checks the config's rules and prepares every statement against ex.
//
// ex must already carry whatever the config's commands attach. Any rule
// violation or statement that fails to prepare is returned, and the server
// does not start: a dataset that cannot answer is a config error, and
// finding it on the first request would find it in production.
func New(ctx context.Context, conf *config.ServeConf, ex Executor, opts ...Option) (*Server, error) {
	if err := conf.CheckError(); err != nil {
		return nil, err
	}

	s := &Server{
		conf:          conf,
		logger:        zap.NewNop(),
		exec:          ex,
		datasets:      map[string]*dataset{},
		healthTimeout: conf.Serve.Timeout(config.ServeDataset{}),
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}

	// Before the instruments, whose gauges read it. A server where no dataset
	// opted in holds no cache at all.
	if conf.Serve.AnyCached() {
		s.cache = newCache(conf.Serve.CacheMaxBytes(), time.Now)
	}

	// The instruments read the executor's session counts, so they are built
	// here rather than by the caller, and the wait hook is installed on the
	// pool before the server listens.
	if s.registry != nil && conf.Serve.MetricsEnabled() {
		m, err := newMetrics(s.registry, ex.Stats)
		if err != nil {
			return nil, err
		}
		s.metrics = m
		if wo, ok := ex.(waitObserver); ok {
			wo.setOnWait(m.observeWait)
		}
	}

	health, err := ex.Prepare(ctx, StatementSpec{Dataset: "healthz", SQL: "SELECT 1"})
	if err != nil {
		return nil, err
	}
	s.health = health

	if http := conf.Serve.HTTP; http != nil && http.CORS != nil {
		s.origins = map[string]bool{}
		for _, origin := range http.CORS.AllowedOrigins {
			s.origins[origin] = true
		}
	}

	listing := datasetListing{Datasets: []datasetDoc{}}
	for _, dc := range conf.Serve.Datasets {
		ds := &dataset{
			conf:     dc,
			maxRows:  conf.Serve.MaxRows(dc),
			timeout:  conf.Serve.Timeout(dc),
			cacheTTL: dc.CacheTTL(),
		}
		doc := datasetDoc{Name: dc.Name, Description: dc.Description, Params: []paramDoc{}}
		for _, p := range dc.Params {
			doc.Params = append(doc.Params, paramDoc{Name: p.Name, Type: p.Type, Min: p.Min, Max: p.Max})
		}

		for _, sc := range dc.Statements() {
			st, err := ex.Prepare(ctx, StatementSpec{
				Dataset: dc.Name, Grain: sc.Grain, SQL: sc.SQL, Params: dc.Params,
			})
			if err != nil {
				return nil, err
			}
			if sc.Grain == "" {
				ds.single = datasetStatement{stmt: st}
				doc.SQL = sc.SQL
				continue
			}
			if ds.grains == nil {
				ds.grains = map[string]datasetStatement{}
				doc.Grains = map[string]grainDoc{}
			}
			ds.grains[sc.Grain] = datasetStatement{stmt: st, grain: sc.Grain}
			doc.Grains[sc.Grain] = grainDoc{SQL: sc.SQL}
		}

		if dc.Range != nil {
			sp, err := newSpan(dc)
			if err != nil {
				return nil, err
			}
			ds.span = sp
			doc.Range = &rangeDoc{Since: dc.Range.Since, Until: dc.Range.Until, Default: dc.Range.Default}
			for _, g := range sp.grains {
				gd := doc.Grains[g.name]
				gd.MaxRange = config.FormatServeDuration(g.max)
				if g.bucket > 0 {
					gd.Bucket = config.FormatServeDuration(g.bucket)
				}
				doc.Grains[g.name] = gd
			}
		}

		if dc.Cache != nil {
			doc.Cache = &cacheDoc{TTLSeconds: dc.Cache.TTLSeconds}
		}

		s.datasets[dc.Name] = ds
		listing.Datasets = append(listing.Datasets, doc)
	}

	body, err := json.Marshal(listing)
	if err != nil {
		return nil, err
	}
	s.listing = body

	return s, nil
}

// Close waits for every running query, then closes the executor's sessions.
// Call it after Serve returns and before closing the database.
func (s *Server) Close() {
	s.exec.Close()
}

// datasetListing is the /v1/datasets body. The SQL is as the config wrote
// it, not as the server numbered it.
type datasetListing struct {
	Datasets []datasetDoc `json:"datasets"`
}

type datasetDoc struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Params      []paramDoc          `json:"params"`
	SQL         string              `json:"sql,omitempty"`
	Grains      map[string]grainDoc `json:"grains,omitempty"`
	Range       *rangeDoc           `json:"range,omitempty"`
	Cache       *cacheDoc           `json:"cache,omitempty"`
}

// cacheDoc tells a caller how stale a dataset's answer may be.
type cacheDoc struct {
	TTLSeconds int `json:"ttl_seconds"`
}

type rangeDoc struct {
	Since   string `json:"since"`
	Until   string `json:"until"`
	Default string `json:"default"`
}

type paramDoc struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Min  *int64 `json:"min,omitempty"`
	Max  *int64 `json:"max,omitempty"`
}

type grainDoc struct {
	Bucket   string `json:"bucket,omitempty"`
	MaxRange string `json:"max_range,omitempty"`
	SQL      string `json:"sql"`
}

// newSpan reads a dataset's range. Check has already held it to the rules,
// so an error here is a config New was handed without checking.
func newSpan(dc config.ServeDataset) (*span, error) {
	def, err := config.ParseServeDuration(dc.Range.Default)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: range.default: %w", dc.Name, err)
	}
	sp := &span{since: dc.Range.Since, until: dc.Range.Until, def: def}
	for _, name := range dc.GrainsByRange() {
		max, err := config.ParseServeDuration(dc.Grains[name].MaxRange)
		if err != nil {
			return nil, fmt.Errorf("dataset %s grain %s: max_range: %w", dc.Name, name, err)
		}
		var bucket time.Duration
		if raw := dc.Grains[name].Bucket; raw != "" {
			if bucket, err = config.ParseServeDuration(raw); err != nil {
				return nil, fmt.Errorf("dataset %s grain %s: bucket: %w", dc.Name, name, err)
			}
		}
		sp.grains = append(sp.grains, spanGrain{name: name, max: max, bucket: bucket})
	}
	return sp, nil
}
