package serve

import (
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// cacheOutcome says how a cached dataset's request was answered. It is what
// the response, the log line and the counter all name.
type cacheOutcome string

const (
	// cacheMiss ran the query.
	cacheMiss cacheOutcome = "miss"
	// cacheHit was answered from a stored result.
	cacheHit cacheOutcome = "hit"
	// cacheShared waited on a query another request had already started.
	cacheShared cacheOutcome = "shared"
)

// cache holds encoded results by key, bounded by bytes, each for its TTL,
// and runs one fill for concurrent requests of one key.
//
// Nothing is invalidated, because serve is never told that the backend
// changed. Every entry is bounded instead: it expires its TTL after its fill
// started, and nothing gives an entry a longer life than that.
//
// It knows nothing of HTTP, datasets or DuckDB.
type cache struct {
	mu  sync.Mutex
	max int64
	now func() time.Time

	bytes   int64
	entries map[string]*cacheEntry
	// lru is recency order, warmest at the front.
	lru *list.List
	// byExpiry is expiry order, soonest at the top. Recency order is not
	// expiry order, so the bound alone would evict an answer still good
	// while an expired one kept its bytes.
	byExpiry expiryHeap
	flights  map[string]*flight

	// onEvict is told why an entry left: "size" or "expired". Set before the
	// cache is used; nil records nothing.
	onEvict func(reason string)
}

type cacheEntry struct {
	key     string
	res     result
	size    int64
	started time.Time
	expires time.Time
	elem    *list.Element
	index   int
}

// flight is one fill in progress and everyone waiting on it.
type flight struct {
	done    chan struct{}
	started time.Time
	waiters int
	res     result
	err     error
}

func newCache(maxBytes int64, now func() time.Time) *cache {
	return &cache{
		max:     maxBytes,
		now:     now,
		entries: map[string]*cacheEntry{},
		lru:     list.New(),
		flights: map[string]*flight{},
	}
}

// do returns the result for key: a stored one that has not expired, the
// result of a fill already running, or the result of fill, which it starts.
//
// ctx bounds this caller's wait and nothing else. The fill runs to its own
// end whoever leaves: the engine finishes an abandoned query anyway, and a
// result that reaches the cache is the next caller's hit rather than work
// thrown away. fill therefore carries its own deadline.
//
// age is how long ago the result's fill started, and 0 for the caller that
// started it.
func (c *cache) do(ctx context.Context, key string, ttl time.Duration, fill func() (result, error)) (result, cacheOutcome, time.Duration, error) {
	c.mu.Lock()
	now := c.now()
	if e, ok := c.entries[key]; ok {
		if now.Before(e.expires) {
			c.lru.MoveToFront(e.elem)
			res, age := e.res, now.Sub(e.started)
			c.mu.Unlock()
			return res, cacheHit, age, nil
		}
		c.remove(e, "expired")
	}

	f, running := c.flights[key]
	outcome := cacheShared
	if !running {
		outcome = cacheMiss
		f = &flight{done: make(chan struct{}), started: now}
		c.flights[key] = f
		go c.run(key, ttl, f, fill)
	}
	f.waiters++
	c.mu.Unlock()

	select {
	case <-f.done:
		if f.err != nil {
			return result{}, outcome, 0, f.err
		}
		age := time.Duration(0)
		if outcome == cacheShared {
			age = c.now().Sub(f.started)
		}
		return f.res, outcome, age, nil
	case <-ctx.Done():
		return result{}, outcome, 0, ctx.Err()
	}
}

// run is the fill's goroutine. The mutex is never held across fill.
func (c *cache) run(key string, ttl time.Duration, f *flight, fill func() (result, error)) {
	res, err := fill()

	c.mu.Lock()
	delete(c.flights, key)
	f.res, f.err = res, err
	if err == nil {
		c.store(key, res, f.started, ttl)
	}
	c.mu.Unlock()
	close(f.done)
}

// store keeps res, making room in two steps and in this order: everything
// expired, then the coldest. The caller holds the mutex.
func (c *cache) store(key string, res result, started time.Time, ttl time.Duration) {
	size := int64(len(res.Rows))
	for _, col := range res.Columns {
		size += int64(len(col.Name) + len(col.Type))
	}
	// One response must not empty the cache.
	if size > c.max/4 {
		return
	}
	now := c.now()
	expires := started.Add(ttl)
	// A fill that outran its own TTL produced an answer already too old.
	if !now.Before(expires) {
		return
	}

	for len(c.byExpiry) > 0 && !now.Before(c.byExpiry[0].expires) {
		c.remove(c.byExpiry[0], "expired")
	}
	if old, ok := c.entries[key]; ok {
		c.remove(old, "expired")
	}
	for c.bytes+size > c.max {
		c.remove(c.lru.Back().Value.(*cacheEntry), "size")
	}

	e := &cacheEntry{key: key, res: res, size: size, started: started, expires: expires}
	e.elem = c.lru.PushFront(e)
	heap.Push(&c.byExpiry, e)
	c.entries[key] = e
	c.bytes += size
}

// remove drops e from all three structures. The caller holds the mutex.
func (c *cache) remove(e *cacheEntry, reason string) {
	delete(c.entries, e.key)
	c.lru.Remove(e.elem)
	heap.Remove(&c.byExpiry, e.index)
	c.bytes -= e.size
	if c.onEvict != nil {
		c.onEvict(reason)
	}
}

// stats is what the gauges publish: the bytes and entries held. After a
// quiet spell that includes expired entries, until the next store.
func (c *cache) stats() (int64, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, len(c.entries)
}

// waiting is how many callers are waiting on key's fill. A test uses it to
// know every caller has arrived before it lets the fill go.
func (c *cache) waiting(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.flights[key]; ok {
		return f.waiters
	}
	return 0
}

// check names the first broken structural invariant, or returns "". A test
// calls it after every step of a random run.
func (c *cache) check() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != c.lru.Len() || len(c.entries) != len(c.byExpiry) {
		return fmt.Sprintf("map %d, list %d, heap %d entries", len(c.entries), c.lru.Len(), len(c.byExpiry))
	}
	var sum int64
	for i, e := range c.byExpiry {
		if e.index != i {
			return fmt.Sprintf("entry %s says index %d at heap slot %d", e.key, e.index, i)
		}
		if c.entries[e.key] != e {
			return "heap holds an entry the map does not: " + e.key
		}
		sum += e.size
	}
	if sum != c.bytes {
		return fmt.Sprintf("byte total %d, entries sum to %d", c.bytes, sum)
	}
	if c.bytes > c.max {
		return fmt.Sprintf("%d bytes held over a bound of %d", c.bytes, c.max)
	}
	return ""
}

// expiryHeap is a min-heap of entries by expiry. Each entry tracks its own
// index so remove can take it from the middle.
type expiryHeap []*cacheEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expires.Before(h[j].expires) }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*cacheEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}
