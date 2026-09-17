package serve

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fakeClock is a clock a test moves. The cache never sleeps, so nothing here
// waits on real time.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
}
func (f *fakeClock) now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// sized is a result whose stored size is exactly n bytes: no columns, n
// bytes of rows.
func sized(n int) result {
	return result{Rows: json.RawMessage(make([]byte, n)), RowCount: n}
}

// counted returns a fill that counts its calls and returns res.
func counted(calls *atomic.Int64, res result) func() (result, error) {
	return func() (result, error) { calls.Add(1); return res, nil }
}

func TestCliServe_CacheHitsInsideTheTTLAndFillsAgainAfterIt(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(1<<20, clk.now)
	var calls atomic.Int64
	ctx := context.Background()

	_, out, age, err := c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, time.Duration(0), age)

	clk.advance(29 * time.Second)
	res, out, age, err := c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, 29*time.Second, age)
	assert.Equal(t, 10, res.RowCount)
	assert.Equal(t, int64(1), calls.Load())

	clk.advance(time.Second)
	_, out, _, err = c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, int64(2), calls.Load())
}

// The age runs from when the fill started, because that is when the query
// read its data. A fill that takes two seconds leaves TTL minus two.
func TestCliServe_CacheExpiryRunsFromTheStartOfTheFill(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(1<<20, clk.now)
	var calls atomic.Int64
	ctx := context.Background()

	slow := func() (result, error) { calls.Add(1); clk.advance(2 * time.Second); return sized(10), nil }
	_, _, _, err := c.do(ctx, "k", 10*time.Second, slow)
	assert.NoError(t, err)

	clk.advance(7 * time.Second) // 9 s after the start
	_, out, age, _ := c.do(ctx, "k", 10*time.Second, slow)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, 9*time.Second, age)

	clk.advance(time.Second) // 10 s after the start, 8 s after the end
	_, out, _, _ = c.do(ctx, "k", 10*time.Second, slow)
	assert.Equal(t, cacheMiss, out)
}

func TestCliServe_CacheRunsOneFillForConcurrentCallers(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	var calls atomic.Int64
	release := make(chan struct{})
	fill := func() (result, error) { calls.Add(1); <-release; return sized(10), nil }

	const n = 16
	outs := make(chan cacheOutcome, n)
	for i := 0; i < n; i++ {
		go func() {
			_, out, _, err := c.do(context.Background(), "k", time.Minute, fill)
			if err != nil {
				out = cacheOutcome("error: " + err.Error())
			}
			outs <- out
		}()
	}
	// Every caller is inside do before the fill is let go.
	for c.waiting("k") < n {
		time.Sleep(time.Millisecond)
	}
	close(release)

	got := map[cacheOutcome]int{}
	for i := 0; i < n; i++ {
		got[<-outs]++
	}
	assert.Equal(t, int64(1), calls.Load())
	assert.Equal(t, 1, got[cacheMiss])
	assert.Equal(t, n-1, got[cacheShared])
}

// The first caller leaving fails nobody, and every caller leaving still
// stores the result: DuckDB would have finished the query anyway.
func TestCliServe_CacheFillOutlivesItsCallers(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	var calls atomic.Int64
	release := make(chan struct{})
	fill := func() (result, error) { calls.Add(1); <-release; return sized(10), nil }

	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() { _, _, _, err := c.do(ctx, "k", time.Minute, fill); left <- err }()
	for c.waiting("k") < 1 {
		time.Sleep(time.Millisecond)
	}

	stayed := make(chan cacheOutcome, 1)
	go func() { _, out, _, _ := c.do(context.Background(), "k", time.Minute, fill); stayed <- out }()
	for c.waiting("k") < 2 {
		time.Sleep(time.Millisecond)
	}

	cancel()
	assert.That(t, errors.Is(<-left, context.Canceled))
	close(release)
	assert.Equal(t, cacheShared, <-stayed)

	_, out, _, _ := c.do(context.Background(), "k", time.Minute, fill)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, int64(1), calls.Load())
}

func TestCliServe_CacheStoresNoError(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	boom := errors.New("postgres went away")
	var calls atomic.Int64

	_, _, _, err := c.do(context.Background(), "k", time.Minute,
		func() (result, error) { calls.Add(1); return result{}, boom })
	assert.That(t, errors.Is(err, boom))

	_, out, _, err := c.do(context.Background(), "k", time.Minute, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, int64(2), calls.Load())
	_, entries := c.stats()
	assert.Equal(t, 1, entries)
}

func TestCliServe_CacheEvictsTheColdestAndAHitWarms(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(100, newFakeClock().now) // a quarter is 25
	var evicted []string
	c.onEvict = func(reason string) { evicted = append(evicted, reason) }
	var calls atomic.Int64
	ctx := context.Background()
	put := func(k string) { _, _, _, _ = c.do(ctx, k, time.Minute, counted(&calls, sized(25))) }

	put("a")
	put("b")
	put("c")
	put("d") // 100 bytes: full
	put("a") // a hit: a is now the warmest
	put("e") // evicts b, the coldest

	_, out, _, _ := c.do(ctx, "a", time.Minute, counted(&calls, sized(25)))
	assert.Equal(t, cacheHit, out)
	_, out, _, _ = c.do(ctx, "b", time.Minute, counted(&calls, sized(25)))
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, "size", evicted[0])
	bytes, _ := c.stats()
	assert.That(t, bytes <= 100)
}

// Recency order is not expiry order. Without the heap the bound would evict
// an answer still good while an expired one kept its bytes.
func TestCliServe_CacheReclaimsExpiredEntriesBeforeLiveOnes(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(100, clk.now)
	var reasons []string
	c.onEvict = func(reason string) { reasons = append(reasons, reason) }
	var calls atomic.Int64
	ctx := context.Background()

	_, _, _, _ = c.do(ctx, "live", time.Hour, counted(&calls, sized(25))) // the coldest
	for _, k := range []string{"x", "y", "z"} {
		_, _, _, _ = c.do(ctx, k, time.Second, counted(&calls, sized(25)))
	}
	clk.advance(2 * time.Second) // x, y and z are expired; live is not

	_, _, _, _ = c.do(ctx, "new", time.Hour, counted(&calls, sized(25)))
	_, out, _, _ := c.do(ctx, "live", time.Hour, counted(&calls, sized(25)))
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, []string{"expired", "expired", "expired"}, reasons)
}

func TestCliServe_CacheDoesNotStoreAResultOverAQuarterOfTheBound(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(100, newFakeClock().now)
	var calls atomic.Int64
	ctx := context.Background()

	res, out, _, err := c.do(ctx, "big", time.Minute, counted(&calls, sized(26)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, 26, res.RowCount)

	_, out, _, _ = c.do(ctx, "big", time.Minute, counted(&calls, sized(26)))
	assert.Equal(t, cacheMiss, out)
	_, entries := c.stats()
	assert.Equal(t, 0, entries)
}

// A seeded run of every operation, with the structure checked after each
// step: the byte total is the sum of the entries and never over the bound,
// and the map, the list and the heap hold the same entries.
func TestCliServe_CacheInvariantsHoldAcrossARandomRun(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(400, clk.now)
	rng := rand.New(rand.NewSource(1))
	var calls atomic.Int64
	ctx := context.Background()

	for step := 0; step < 5000; step++ {
		key := "k" + strconv.Itoa(rng.Intn(40))
		switch rng.Intn(10) {
		case 0:
			clk.advance(time.Duration(rng.Intn(5)) * time.Second)
		case 1:
			_, _, _, _ = c.do(ctx, key, time.Second, func() (result, error) { return result{}, errors.New("x") })
		default:
			ttl := time.Duration(1+rng.Intn(10)) * time.Second
			_, _, _, _ = c.do(ctx, key, ttl, counted(&calls, sized(1+rng.Intn(120))))
		}
		if problem := c.check(); problem != "" {
			t.Fatalf("step %d: %s", step, problem)
		}
	}
}
