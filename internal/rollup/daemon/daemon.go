package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/rollup"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// defaultInterval is how often the daemon checks its leadership and its
// pending tables when it has nothing to fill.
const defaultInterval = 15 * time.Second

// defaultAddr is where `sqlflow run` serves /metrics and /healthz, so a
// scrape config carries over.
const defaultAddr = ":8000"

// chunkTimeout bounds one chunk once SIGTERM has arrived. A chunk finishes
// rather than rolls back, and it never waits on a lock, so this only stops a
// chunk the database never answers.
const chunkTimeout = 5 * time.Minute

// Options configures a Daemon.
type Options struct {
	Version string
	Logger  *zap.Logger
	// Metrics is the exporter served at /metrics beside /healthz: empty for
	// none, which serves neither, or "prometheus".
	Metrics string
	// Addr is where /metrics and /healthz listen. Defaults to :8000.
	Addr string
	// Interval defaults to 15 seconds.
	Interval time.Duration
	// OnListen receives the bound address. Tests bind port 0.
	OnListen func(net.Addr)
}

// Daemon is one `sqlflow rollup run` process.
type Daemon struct {
	conf     *config.RollupsConf
	dsn      string
	opts     Options
	log      *zap.Logger
	interval time.Duration
	health   *healthState
	m        *instruments
	registry *prom.Registry
}

// New builds a daemon. It connects to nothing: Run does.
func New(conf *config.RollupsConf, dsn string, opts Options) (*Daemon, error) {
	mp, registry, err := newProvider(opts.Metrics)
	if err != nil {
		return nil, err
	}
	m, err := newInstruments(mp)
	if err != nil {
		return nil, err
	}
	d := &Daemon{
		conf: conf, dsn: dsn, opts: opts, log: opts.Logger, interval: opts.Interval,
		health: newHealthState(time.Now()), m: m, registry: registry,
	}
	if d.log == nil {
		d.log = zap.NewNop()
	}
	if d.interval <= 0 {
		d.interval = defaultInterval
	}
	if d.opts.Addr == "" {
		d.opts.Addr = defaultAddr
	}
	return d, nil
}

// Health is the status /healthz reports, and its reason.
func (d *Daemon) Health() (status, reason string) {
	status, reason, _ = healthStatus(d.health.get(), time.Now(), d.interval)
	return status, reason
}

// Run installs what the file declares, then competes for the leader lock
// and, while it leads, fills pending tables chunk by chunk. It returns nil
// once ctx ends and the chunk in progress has committed. It returns an error
// only when the first connection fails or install refuses the file: a
// supervisor's restart loop makes either visible, and neither heals by
// waiting.
func (d *Daemon) Run(ctx context.Context) error {
	if d.registry != nil {
		if err := d.serveHTTP(ctx); err != nil {
			return err
		}
	}
	work, err := rollup.Connect(ctx, d.dsn)
	if err != nil {
		d.m.errors.Add(ctx, 1, phase("install"))
		return err
	}
	defer func() {
		if work != nil {
			_ = work.Close(context.Background())
		}
	}()
	d.health.touch(time.Now())
	report, err := rollup.Install(ctx, work, d.conf, d.opts.Version)
	if err != nil {
		d.m.errors.Add(ctx, 1, phase("install"))
		return err
	}
	d.health.touch(time.Now())
	d.logInstall(report)

	lead := &session{}
	defer lead.close()
	var checked time.Time
	for {
		if time.Since(checked) >= d.interval {
			d.keepLead(ctx, lead)
			checked = time.Now()
		}
		more := false
		if lead.leading {
			work = d.keepWork(ctx, work)
			if work != nil {
				more = d.fillOne(ctx, work)
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.interval):
		}
	}
}

// session is the connection that holds, or competes for, the leader locks.
type session struct {
	conn    *pgx.Conn
	leading bool
}

func (s *session) close() {
	if s.conn != nil {
		_ = s.conn.Close(context.Background())
	}
	s.conn, s.leading = nil, false
}

// keepLead keeps or takes the leader locks, once an interval. A failed
// ping means the session, and its locks with it, may be gone, so the daemon
// steps down and competes again on the next interval.
func (d *Daemon) keepLead(ctx context.Context, s *session) {
	if s.conn != nil {
		if err := s.conn.Ping(ctx); err != nil {
			if ctx.Err() == nil {
				d.log.Warn("lost the leader session; standing down", zap.Error(err))
				d.m.errors.Add(ctx, 1, phase("lock"))
			}
			s.close()
			d.health.setRole(roleStandby)
			return
		}
		d.health.touch(time.Now())
	} else {
		conn, err := rollup.Connect(ctx, d.dsn)
		if err != nil {
			if ctx.Err() == nil {
				d.log.Warn("cannot reach the database for the leader lock", zap.Error(err))
				d.m.errors.Add(ctx, 1, phase("lock"))
			}
			return
		}
		s.conn = conn
		d.health.touch(time.Now())
	}
	if s.leading {
		return
	}
	ok, err := tryLead(ctx, s.conn, d.conf)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("competing for the leader lock", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("lock"))
		}
		s.close()
		return
	}
	if !ok {
		d.health.setRole(roleStandby)
		d.health.setPending(0)
		return
	}
	s.leading = true
	d.m.leaderAcquired.Add(ctx, 1)
	d.health.setRole(roleLeader)
	d.log.Info("leading")
}

// keepWork reopens the work connection after the database dropped it.
func (d *Daemon) keepWork(ctx context.Context, work *pgx.Conn) *pgx.Conn {
	if work != nil && !work.IsClosed() {
		return work
	}
	conn, err := rollup.Connect(ctx, d.dsn)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("cannot reach the database to fill tables", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("backfill"))
		}
		return nil
	}
	d.health.touch(time.Now())
	return conn
}

// fillOne fills one chunk of the first pending table. It reports whether
// another chunk is due at once: false when nothing is pending, or when the
// chunk failed and the next try waits an interval.
func (d *Daemon) fillOne(ctx context.Context, work *pgx.Conn) bool {
	pending, err := rollup.PendingBackfills(ctx, work, d.conf)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("reading pending tables", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("backfill"))
		}
		return false
	}
	d.health.touch(time.Now())
	d.health.setPending(len(pending))
	if len(pending) == 0 {
		return false
	}

	// The chunk finishes even after SIGTERM: stopping waits for it.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chunkTimeout)
	defer cancel()
	p := pending[0]
	attrs := metric.WithAttributes(attribute.String("rollup", p.Rollup.Name), attribute.String("table", p.Table))
	start := time.Now()
	step, err := rollup.BackfillStep(cctx, work, p)
	if errors.Is(err, rollup.ErrChunkBusy) {
		// Live writes held its buckets on every try. Nothing failed: the
		// chunk tries again at once, and its tries already waited.
		d.health.touch(time.Now())
		d.log.Info("a live write held the chunk's buckets; trying again", zap.String("table", p.Table))
		return true
	}
	if err != nil {
		d.log.Warn("backfill chunk failed; retrying next interval", zap.String("table", p.Table), zap.Error(err))
		d.m.errors.Add(cctx, 1, phase("backfill"))
		return false
	}
	d.health.touch(time.Now())
	d.m.chunkDuration.Record(cctx, time.Since(start).Seconds(), attrs)
	d.m.backfillBuckets.Add(cctx, step.Buckets, attrs)
	d.health.setFilling(step.Table, step.Remaining)
	d.log.Info("backfill chunk", zap.String("rollup", step.Rollup), zap.String("table", step.Table),
		zap.Time("from", step.From), zap.Time("to", step.To), zap.Int64("buckets", step.Buckets),
		zap.Bool("complete", step.Complete))
	return true
}

func (d *Daemon) logInstall(rep *rollup.InstallReport) {
	for _, r := range rep.Rollups {
		d.log.Info("installed", zap.String("rollup", r.Name), zap.Bool("adopted", r.Adopted),
			zap.Strings("created", r.Created), zap.Strings("backfill", r.Plan.Backfill),
			zap.Strings("restored", r.Plan.Restore), zap.Strings("dropped", r.Dropped),
			zap.Int("attempts", rep.Attempts))
		for _, t := range r.Plan.Retain {
			d.log.Warn("no longer declared; its table and triggers stay", zap.String("rollup", r.Name), zap.String("table", t.Table))
		}
	}
	for _, n := range rep.Undeclared {
		d.log.Warn("rollup no longer declared; its tables and triggers stay", zap.String("rollup", n))
	}
}

// serveHTTP serves /metrics and /healthz on Addr until ctx ends.
func (d *Daemon) serveHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(d.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status, reason, code := healthStatus(d.health.get(), time.Now(), d.interval)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "reason": reason})
	})
	ln, err := net.Listen("tcp", d.opts.Addr)
	if err != nil {
		return err
	}
	if d.opts.OnListen != nil {
		d.opts.OnListen(ln.Addr())
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.log.Error("http server stopped", zap.Error(err))
		}
	}()
	d.log.Info("serving http", zap.String("addr", ln.Addr().String()), zap.Strings("routes", []string{"/metrics", "/healthz"}))
	return nil
}
