package turbostats

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.uber.org/zap"
)

// postTimeout bounds one post. Nothing the reporter does waits longer,
// including the final bundle on a shutdown: a reporter that delays a drain is
// a reporter an operator turns off.
const postTimeout = 10 * time.Second

// jitterFraction spreads a fleet that started together. Without it, a rack
// that lost power reports in lockstep forever and the receiver sees every
// instance in the same second of every minute.
const jitterFraction = 0.1

// ReporterConfig is everything the reporter needs. Now and Client are for
// tests; both have working defaults.
type ReporterConfig struct {
	ReportTo string
	Key      ed25519.PrivateKey
	Interval time.Duration
	Collect  func(context.Context) (Bundle, error)
	Log      *zap.Logger
	Now      func() time.Time
	Client   *http.Client
}

// Reporter posts the process's own state outbound, on an interval.
//
// It exists because nothing can dial an instance. A control plane cannot
// scrape a process behind NAT, so the process reports itself, and a missed
// interval is the only outage signal there is. That is why nothing here
// retries or queues: a gap is the message.
type Reporter struct {
	url      string
	key      ed25519.PrivateKey
	interval time.Duration
	collect  func(context.Context) (Bundle, error)
	log      *zap.Logger
	now      func() time.Time
	client   *http.Client
	rand     *rand.Rand

	// failing is whether the last post failed. It turns a run of failures
	// into one line at each edge rather than one per attempt: a log that
	// buries itself reporting an outage is no log.
	failing bool
}

func NewReporter(conf ReporterConfig) (*Reporter, error) {
	if conf.ReportTo == "" {
		return nil, errors.New("turbostats: a reporter needs somewhere to report to")
	}
	if len(conf.Key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("turbostats: a signing key is %d bytes, not %d",
			ed25519.PrivateKeySize, len(conf.Key))
	}
	if conf.Collect == nil {
		return nil, errors.New("turbostats: a reporter needs something to collect")
	}
	r := &Reporter{
		url: conf.ReportTo, key: conf.Key, interval: conf.Interval,
		collect: conf.Collect, log: conf.Log, now: conf.Now, client: conf.Client,
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if r.interval <= 0 {
		r.interval = time.Minute
	}
	if r.log == nil {
		r.log = zap.NewNop()
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.client == nil {
		r.client = &http.Client{Timeout: postTimeout}
	}
	return r, nil
}

// Run posts until ctx ends.
//
// The first bundle goes immediately. An instance that waited a full interval
// would be missing from a fleet page for its first minute, which is exactly
// when someone is watching a deploy.
func (r *Reporter) Run(ctx context.Context) {
	r.post(ctx, nil)
	for {
		timer := time.NewTimer(r.nextInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			r.post(ctx, nil)
		}
	}
}

// Final sends the last bundle, carrying how the process ended.
//
// This is what lets a receiver tell a clean stop from a crash: a process that
// died cannot send one, and the absence is the whole signal. It is bounded by
// the same timeout as any other post, so a receiver that hangs cannot hold a
// shutdown open.
func (r *Reporter) Final(ctx context.Context, exit Exit) {
	r.post(ctx, &exit)
}

// nextInterval jitters by up to ten percent either way.
func (r *Reporter) nextInterval() time.Duration {
	spread := float64(r.interval) * jitterFraction
	return r.interval + time.Duration((r.rand.Float64()*2-1)*spread)
}

// post builds one bundle, signs it, and sends it. It returns nothing: a
// caller cannot act on a failure, which is the point.
func (r *Reporter) post(ctx context.Context, exit *Exit) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postTimeout)
	defer cancel()

	b, err := r.collect(ctx)
	if err != nil {
		// Sending a blank bundle would put a healthy-looking document on a
		// page that should show a gap.
		r.fail("collecting the bundle", err)
		return
	}
	b.Exit = exit

	body, err := json.Marshal(b)
	if err != nil {
		r.fail("encoding the bundle", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		r.fail("building the request", err)
		return
	}
	req.Header.Set("Content-Type", MediaType)
	if err := wire.SignRequest(req, r.key, body, r.now()); err != nil {
		r.fail("signing the request", err)
		return
	}

	resp, err := r.client.Do(req)
	if err != nil {
		r.fail("posting the bundle", err)
		return
	}
	// Drained and closed whatever the status, or the connection is not reused
	// and an instance reporting for weeks leaks sockets.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.fail("posting the bundle", fmt.Errorf("receiver answered %s", resp.Status))
		return
	}
	r.succeed()
}

// fail logs a failed post. One warning when a run begins and debug for the
// rest: an unregistered fleet posts forever, and a line per attempt would
// bury every other line in the log.
func (r *Reporter) fail(what string, err error) {
	if !r.failing {
		r.failing = true
		r.log.Warn("turbostats reporting is failing", zap.String("step", what),
			zap.String("report_to", r.url), zap.Error(err))
		return
	}
	r.log.Debug("turbostats post failed", zap.String("step", what), zap.Error(err))
}

// succeed notes a post that worked, and says so only if the last one did not.
func (r *Reporter) succeed() {
	if r.failing {
		r.failing = false
		r.log.Info("turbostats reporting recovered", zap.String("report_to", r.url))
	}
}
