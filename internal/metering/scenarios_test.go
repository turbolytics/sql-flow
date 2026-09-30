package metering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// Every scenario feeds a fixed event set whose answer is computed here, and
// waits for Postgres to hold exactly that answer summed over partitions.
// A count that is off by one fails.

const exactWithin = 3 * time.Minute

// 1. Steady state: three workers up throughout.
func TestIntegrationMetering_SteadyState(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(1, 200, 20, recent(8), 5*time.Minute)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	s.awaitMembers(3, 90*time.Second)
	s.produce(events)
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 2. A worker joins mid-stream, forcing a rebalance while minutes are open.
func TestIntegrationMetering_WorkerJoins(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(2, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	s.startCount("count-0", "state-0")
	s.startCount("count-1", "state-1")
	s.awaitMembers(2, 90*time.Second)
	s.produce(events[:half])
	time.Sleep(10 * time.Second) // rows are in open windows now
	s.startCount("count-2", "state-2")
	// The join must have moved partitions before the second half is
	// produced, or the two workers already there consume it all.
	s.awaitMembers(3, 90*time.Second)
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 20*time.Second)
}

// 3. A worker leaves gracefully mid-stream.
func TestIntegrationMetering_WorkerLeaves(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(3, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w0 := s.startCount("count-0", "state-0")
	s.startCount("count-1", "state-1")
	s.startCount("count-2", "state-2")
	s.awaitMembers(3, 90*time.Second)
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w0.stop(t)
	// The group settles on two members before the stream moves on. Producing
	// the rest at once would carry the survivors' watermark minutes ahead
	// before the leaver's partitions reached them, and the replayed minute
	// would be refused as late. In real time a rebalance takes seconds and
	// event time moves at wall speed, so the watermark cannot jump like
	// that; the limit is that allowed_lateness_seconds must exceed the time
	// a rebalance takes.
	s.awaitMembers(2, 90*time.Second)
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 20*time.Second)
}

// 4. kill -9, state kept. One worker, so the only recovery path is the
// restart on the same disk; with three, the partitions move after the
// session timeout and this becomes scenario 3.
func TestIntegrationMetering_CrashStateKept(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(4, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w := s.startCount("count-0", "state-0")
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-0")
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 5. kill -9, state lost: the restart has a new state path.
func TestIntegrationMetering_CrashStateLost(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(5, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w := s.startCount("count-0", "state-0")
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-new")
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 6. Every request is sent twice, as a client retries a lost 200, through
// the webhook and the Kafka sink. The duplicate must land on its original's
// partition to be deduplicated there.
func TestIntegrationMetering_RetriedDuplicates(t *testing.T) {
	coverage.Covers(t, "source.webhook", "sink.kafka")
	s := newStack(t, current)
	events := workload(6, 100, 10, recent(8), 5*time.Minute)
	_, url := s.startIngest("ingest", 50, 1)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	s.awaitMembers(3, 90*time.Second)
	var took []time.Duration
	for _, b := range batches(events, 25) {
		for attempt := 0; attempt < 2; attempt++ {
			t0 := time.Now()
			code := postEvents(t, url, b)
			took = append(took, time.Since(t0))
			assert.Equal(t, http.StatusOK, code)
		}
	}
	// What a lone sender waits for a 200. With after_flush it is the wait
	// for the batch to flush, bounded by flush_interval_seconds.
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	t.Logf("ingest_200_latency ack=%v requests=%d p50=%s p99=%s", current.Ack, len(took),
		took[len(took)/2], took[len(took)*99/100])
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 7. Late events within allowed_lateness_seconds republish their minute.
func TestIntegrationMetering_LateWithinLateness(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	start := recent(8)
	events := workload(7, 100, 10, start, 5*time.Minute)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	s.awaitMembers(3, 90*time.Second)
	s.produce(events)
	// 30 s late for minute 3: its end is 90 s before the newest event, and
	// the watermark is newest - grace, so it closed and is within 60 s of
	// lateness only if it arrives before the stream moves on. Sent while
	// the stream is still at minute 4.
	late := workload(70, 100, 1, start.Add(3*time.Minute+30*time.Second), time.Second)
	s.produce(late)
	s.awaitExact(expected(append(events, late...)), exactWithin, 10*time.Second)
}

// 8. Kill ingest while requests are in flight. Every event answered 200
// must be in Kafka.
func TestIntegrationMetering_IngestKilledBeforeFlush(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s := newStack(t, current)
	// A batch that only the interval flushes. Filled by size, a batch
	// flushes every fraction of a second under this load, and whether the
	// kill lands between an answer and its flush is a race.
	w, url := s.startIngest("ingest", 1000000, 1)
	var (
		mu       sync.Mutex
		answered []string
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				e := event{ID: name(name("evt", g), i), Customer: "user_1", Meter: "requests", Quantity: 1,
					TSMillis: time.Now().UnixMilli()}
				if code := postEventsNoFail([]event{e}, url); code == http.StatusOK {
					mu.Lock()
					answered = append(answered, e.ID)
					mu.Unlock()
				}
			}
		}(g)
	}
	time.Sleep(3500 * time.Millisecond)
	w.kill9()
	close(stop)
	wg.Wait()
	if len(answered) == 0 {
		t.Fatal("no event was answered 200 before the kill, so nothing was tested")
	}
	inKafka := s.readIDs()
	var missing []string
	for _, id := range answered {
		if !inKafka[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d of %d events answered 200 are not in Kafka, first: %v", len(missing), len(answered), first(missing, 10))
	}
	t.Logf("%d events answered 200, all in Kafka", len(answered))
}

// 10. Rebalance under load on one hot partition. Exact totals, and the time
// to reach them against the same load with no rebalance.
func TestIntegrationMetering_RebalanceUnderLoad(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	for _, join := range []bool{false, true} {
		join := join
		t.Run(map[bool]string{false: "control", true: "join"}[join], func(t *testing.T) {
			s := newStack(t, current)
			// 80% of events for one customer: one partition carries most of
			// the load, which is the partition whose replay costs the most.
			hot := workload(10, 1, 160000, recent(6), 4*time.Minute)
			cold := workload(11, 400, 100, recent(6), 4*time.Minute)
			// In time order, as an ingest writes them. Hot then cold would put
			// every cold row minutes behind its partition's newest, and the
			// engine refuses those as late.
			events := append(hot, cold...)
			sort.SliceStable(events, func(i, j int) bool { return events[i].TSMillis < events[j].TSMillis })
			s.startCount("count-0", "state-0")
			s.startCount("count-1", "state-1")
			s.awaitMembers(2, 90*time.Second)
			t0 := time.Now()
			s.produce(events[:len(events)/2])
			if join {
				s.startCount("count-2", "state-2")
				s.awaitMembers(3, 90*time.Second)
			}
			s.produce(events[len(events)/2:])
			s.awaitExact(expected(events), 5*time.Minute, 20*time.Second)
			t.Logf("rebalance_under_load join=%v events=%d exact_after=%s", join, len(events), time.Since(t0))
		})
	}
}

// 11. A worker refuses a late event, then loses its disk while a later
// minute is still open. The fresh worker replays from the open minute's
// records, which sit before the late event in the log, so it reads the late
// event again. The minute it was late for must keep its count.
func TestIntegrationMetering_ReplayPastARefusedLateEvent(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	// Long enough that minute 7 stays open through the waits below; a
	// shorter idle close publishes it before the kill, and nothing is lost.
	s.idleClose = 30
	start := recent(10)
	early := workload(12, 50, 10, start, 2*time.Minute)                    // minutes 0 and 1
	later := workload(13, 50, 10, start.Add(5*time.Minute), 3*time.Minute) // minutes 5 to 7
	w := s.startCount("count-0", "state-0")
	s.produce(early)
	// Minute 1's events carry the watermark past minute 0's end, so minute 0
	// publishes: the worker is consuming.
	s.awaitMinute(start, 90*time.Second)
	s.produce(later)
	// Minute 5 publishing proves the watermark is past minute 6, so minute 0
	// is far past 60 s of lateness. Sent before this, the straggler can share
	// a batch with the rows that move the watermark and be accepted.
	s.awaitMinute(start.Add(5*time.Minute), 90*time.Second)
	// Same customer as early[0], so same partition as that customer's minute
	// 7 records, which the low watermark holds the commit behind.
	straggler := []event{{ID: "straggler", Customer: early[0].Customer, Meter: "requests", Quantity: 1,
		TSMillis: start.Add(10 * time.Second).UnixMilli()}}
	s.produce(straggler)
	// One flush and commit, with minute 7 still open when the worker dies.
	time.Sleep(3 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-new")
	s.awaitExact(expected(append(early, later...)), exactWithin, 20*time.Second)
}

func name(prefix string, i int) string { return prefix + "-" + itoa(i) }

func itoa(i int) string { return fmt.Sprintf("%d", i) }

func batches(events []event, n int) [][]event {
	var out [][]event
	for len(events) > 0 {
		k := min(n, len(events))
		out = append(out, events[:k])
		events = events[k:]
	}
	return out
}

func postEvents(t *testing.T, url string, events []event) int {
	t.Helper()
	code := postEventsNoFail(events, url)
	if code == 0 {
		t.Fatalf("posting to %s failed", url)
	}
	return code
}

// postEventsNoFail returns 0 when the request did not complete, which is
// what a sender sees when ingest dies under it.
func postEventsNoFail(events []event, url string) int {
	body, _ := json.Marshal(map[string]any{"events": events})
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0
	}
	// Read to the end so the connection is reused. A body closed unread
	// costs a connection per request, and 16,000 requests use every
	// ephemeral port: the senders then stop long before the kill.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// readIDs reads the topic to its end with a fresh consumer.
func (s *stack) readIDs() map[string]bool {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker), kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	assert.NoError(s.t, err)
	defer cl.Close()
	ids := map[string]bool{}
	idle := 0
	for idle < 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		f := cl.PollFetches(ctx)
		cancel()
		if f.NumRecords() == 0 {
			idle++
			continue
		}
		idle = 0
		f.EachRecord(func(r *kgo.Record) {
			var e event
			if json.Unmarshal(r.Value, &e) == nil {
				ids[e.ID] = true
			}
		})
	}
	return ids
}
