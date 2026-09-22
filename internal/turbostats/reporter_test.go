package turbostats

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// receiver records what arrived and answers however a test asks it to.
type receiver struct {
	mu      sync.Mutex
	bodies  [][]byte
	headers []http.Header
	status  int
	reply   string
	hang    chan struct{}
}

func newReceiver() *receiver {
	return &receiver{status: http.StatusOK, reply: `{"v":1,"commands":[]}`}
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rc.mu.Lock()
	rc.bodies = append(rc.bodies, body)
	rc.headers = append(rc.headers, r.Header.Clone())
	hang, status, reply := rc.hang, rc.status, rc.reply
	rc.mu.Unlock()
	if hang != nil {
		<-hang
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(reply))
}

func (rc *receiver) count() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.bodies)
}

func testKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	priv, err := wire.ParseCredential("sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
	assert.NoError(t, err)
	return priv
}

func testReporter(t *testing.T, url string, key ed25519.PrivateKey) *Reporter {
	t.Helper()
	r, err := NewReporter(ReporterConfig{
		ReportTo: url, Key: key, Interval: 10 * time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{V: Version, Instance: Instance{ID: "one"}}, nil
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)
	return r
}

// The first bundle goes at start, not one interval later. An instance that
// waited would be missing from a fleet page for its first minute, which is
// exactly when someone is watching a deploy.
//
// The interval is an hour on purpose. A short one would let this pass
// whether or not the first post is immediate, which is what a shorter
// version of this test did.
func TestReporter_PostsAtStart(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: time.Hour,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{V: Version, Instance: Instance{ID: "one"}}, nil
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	waitFor(t, func() bool { return rc.count() >= 1 })
}

// And it keeps posting. One bundle would leave a page frozen at whatever the
// instance looked like when it started.
func TestReporter_PostsOnTheInterval(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, testKey(t)).Run(ctx)

	waitFor(t, func() bool { return rc.count() >= 3 })
}

// The receiver must be able to verify what arrived, with the key id it can
// look the public half up by.
func TestReporter_SignsEveryPost(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()
	key := testKey(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, key).Run(ctx)
	waitFor(t, func() bool { return rc.count() >= 1 })

	rc.mu.Lock()
	body, header := rc.bodies[0], rc.headers[0]
	rc.mu.Unlock()

	assert.Equal(t, MediaType, header.Get("Content-Type"))
	keyID, ts, sig, err := wire.ParseHeaders(header)
	assert.NoError(t, err)
	assert.Equal(t, wire.KeyID(key.Public().(ed25519.PublicKey)), keyID)
	// The path is the receiver's, and the query is not signed.
	assert.That(t, wire.Verify(key.Public().(ed25519.PublicKey),
		http.MethodPost, "/", ts, body, sig))
}

// The rule that keeps deployed instances alive the day commands ship. A
// reporter that failed on an unknown field would strand every one of them.
func TestReporter_TreatsAny2xxAsSuccess(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	for _, reply := range []string{
		`{"v":1,"commands":[]}`,
		`{"v":1,"commands":[{"id":"c1","verb":"restart"}],"later":true}`,
		`{"v":2,"unknown":{"deeply":"nested"}}`,
		`not json at all`,
		``,
	} {
		rc := newReceiver()
		rc.reply = reply
		rc.status = http.StatusAccepted
		srv := httptest.NewServer(rc)

		ctx, cancel := context.WithCancel(context.Background())
		go testReporter(t, srv.URL, testKey(t)).Run(ctx)
		waitFor(t, func() bool { return rc.count() >= 2 })
		cancel()
		srv.Close()
	}
}

// A receiver that is down, refusing, or hanging must not stop the reporter
// posting. The control plane reads the gap; the instance keeps working.
func TestReporter_KeepsGoingThroughFailures(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	rc.status = http.StatusUnauthorized
	srv := httptest.NewServer(rc)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go testReporter(t, srv.URL, testKey(t)).Run(ctx)

	// An unregistered fleet posts forever and never gives up: the operator
	// registers the key later and the instance appears with no restart.
	waitFor(t, func() bool { return rc.count() >= 3 })
}

// A hung receiver must not hold a shutdown. This is the rule that keeps the
// reporter from being the reason a pod takes thirty seconds to die.
func TestReporter_AHungReceiverDoesNotDelayTheCaller(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	rc.hang = make(chan struct{})
	srv := httptest.NewServer(rc)
	defer func() { close(rc.hang); srv.Close() }()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: time.Hour,
		Collect: func(context.Context) (Bundle, error) { return Bundle{V: Version}, nil },
		Log:     zap.NewNop(),
		Client:  &http.Client{Timeout: 50 * time.Millisecond},
	})
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		r.Final(context.Background(), Exit{Reason: "SIGTERM"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Final waited on a hung receiver")
	}
}

// The last bundle is how a control plane tells a clean stop from a crash. A
// process that died cannot send one, which is the whole signal.
func TestReporter_TheFinalBundleCarriesTheExit(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	testReporter(t, srv.URL, testKey(t)).Final(context.Background(),
		Exit{Reason: "SIGTERM", Code: 0})
	assert.Equal(t, 1, rc.count())

	rc.mu.Lock()
	body := rc.bodies[0]
	rc.mu.Unlock()
	var b Bundle
	assert.NoError(t, json.Unmarshal(body, &b))
	assert.That(t, b.Exit != nil)
	assert.Equal(t, "SIGTERM", b.Exit.Reason)
}

// A collect that fails is not a post. Sending a blank bundle would put a
// healthy-looking document on a page that should show a gap.
func TestReporter_ACollectFailureSendsNothing(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: 10 * time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{}, errors.New("reader closed")
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)
	r.Final(context.Background(), Exit{Reason: "SIGTERM"})
	assert.Equal(t, 0, rc.count())
}

// waitFor polls until cond holds or the test gives up. A sleep long enough to
// be reliable is a sleep that slows every run.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition never held")
}
