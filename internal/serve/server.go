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
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"go.uber.org/zap"
)

// Server serves one serve config's datasets over one DuckDB connection.
type Server struct {
	conf   *config.ServeConf
	logger *zap.Logger
	exec   *executor

	datasets map[string]*dataset
	// listing is the /v1/datasets body, built once: the config does not change.
	listing []byte
	// origins is nil without a cors block, which sends no CORS header at all.
	origins map[string]bool
	// healthTimeout bounds /healthz, which waits on the same lock as a query.
	healthTimeout time.Duration
	// now is when a request arrived: the until a ranged request does not
	// give. A test fixes it.
	now func() time.Time
}

// dataset is one config dataset with its statements prepared.
type dataset struct {
	conf config.ServeDataset
	// single is the statement of a dataset without grains.
	single *statement
	// grains holds one statement per grain.
	grains  map[string]*statement
	maxRows int
	timeout time.Duration
	// span is nil for a dataset without a range.
	span *span
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
}

// Option configures a Server.
type Option func(*Server)

// WithLogger sets the logger the request lines go to.
func WithLogger(l *zap.Logger) Option {
	return func(s *Server) { s.logger = l }
}

// New checks the config's rules and prepares every statement against conn.
//
// conn must already carry whatever the config's commands attach. Any rule
// violation or statement that fails to prepare is returned, and the server
// does not start: a dataset that cannot answer is a config error, and
// finding it on the first request would find it in production.
func New(ctx context.Context, conf *config.ServeConf, conn adbc.Connection, opts ...Option) (*Server, error) {
	if err := conf.CheckError(); err != nil {
		return nil, err
	}

	s := &Server{
		conf:          conf,
		logger:        zap.NewNop(),
		exec:          &executor{conn: conn},
		datasets:      map[string]*dataset{},
		healthTimeout: conf.Serve.Timeout(config.ServeDataset{}),
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}

	if http := conf.Serve.HTTP; http != nil && http.CORS != nil {
		s.origins = map[string]bool{}
		for _, origin := range http.CORS.AllowedOrigins {
			s.origins[origin] = true
		}
	}

	listing := datasetListing{Datasets: []datasetDoc{}}
	for _, dc := range conf.Serve.Datasets {
		ds := &dataset{
			conf:    dc,
			maxRows: conf.Serve.MaxRows(dc),
			timeout: conf.Serve.Timeout(dc),
		}
		doc := datasetDoc{Name: dc.Name, Description: dc.Description, Params: []paramDoc{}}
		for _, p := range dc.Params {
			doc.Params = append(doc.Params, paramDoc{Name: p.Name, Type: p.Type})
		}

		for _, sc := range dc.Statements() {
			st, err := prepare(ctx, conn, dc.Name, sc.Grain, sc.SQL, dc.Params)
			if err != nil {
				return nil, err
			}
			if sc.Grain == "" {
				ds.single = st
				doc.SQL = sc.SQL
				continue
			}
			if ds.grains == nil {
				ds.grains = map[string]*statement{}
				doc.Grains = map[string]grainDoc{}
			}
			ds.grains[sc.Grain] = st
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
				doc.Grains[g.name] = gd
			}
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

// Close waits for a running query and refuses every later one. Call it after
// Serve returns and before closing the connection.
func (s *Server) Close() {
	s.exec.close()
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
}

type rangeDoc struct {
	Since   string `json:"since"`
	Until   string `json:"until"`
	Default string `json:"default"`
}

type paramDoc struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type grainDoc struct {
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
		sp.grains = append(sp.grains, spanGrain{name: name, max: max})
	}
	return sp, nil
}
