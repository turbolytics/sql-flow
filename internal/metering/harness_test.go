// Package metering holds the usage-metering stack to exact counts: webhook
// ingest into Kafka keyed by customer, three count workers in one consumer
// group, a one-minute window with allowed lateness, and a Postgres upsert
// keyed by (minute, customer, meter, kafka_partition). The stack is
// processes, not goroutines, because the failures it exists for are a
// process killed mid-minute and a disk that did not come back.
//
// Tests only. The scenarios read current, so each fix turns its feature on
// here and the same scenario that failed before it must pass after it.
package metering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/turbolytics/sql-flow/internal/pgtest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/zeebo/assert"
)

// Features are the engine changes a scenario's configs use. Task 1 lands
// with none; each fix turns its own on.
type Features struct {
	Ack            bool // ingest: webhook ack: after_flush
	Key            bool // ingest: kafka sink key: customer
	PartitionOwned bool // count: window partition_owned: true
}

var current = Features{Key: true, Ack: true, PartitionOwned: true}

var sharedPostgres = pgtest.New(pgtest.Options{User: "metering", Password: "metering"})

func TestMain(m *testing.M) {
	code := sharedPostgres.Run(m)
	if brokerCtr != nil {
		_ = brokerCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// brokerImage matches internal/kafka's, so a machine that ran those tests
// has the image.
const brokerImage = "confluentinc/confluent-local:7.5.0"

var (
	brokerOnce sync.Once
	brokerAddr string
	brokerErr  error
	brokerCtr  *tckafka.KafkaContainer
)

func broker(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	if addr := os.Getenv("SQLFLOW_KAFKA_BROKERS"); addr != "" {
		return addr
	}
	brokerOnce.Do(func() {
		ctx := context.Background()
		brokerCtr, brokerErr = tckafka.Run(ctx, brokerImage)
		if brokerErr != nil {
			return
		}
		var addrs []string
		addrs, brokerErr = brokerCtr.Brokers(ctx)
		if brokerErr == nil && len(addrs) == 0 {
			brokerErr = fmt.Errorf("kafka container reported no brokers")
		}
		if brokerErr == nil {
			brokerAddr = addrs[0]
		}
	})
	if brokerErr != nil {
		t.Fatalf("starting kafka: %v", brokerErr)
	}
	return brokerAddr
}

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

// sqlflowBinary builds this checkout's sqlflow once per test binary. The
// scenarios test the engine the branch builds, never an installed one.
func sqlflowBinary(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sqlflow-metering-bin")
		if err != nil {
			binErr = err
			return
		}
		binPath = filepath.Join(dir, "sqlflow")
		cmd := exec.Command("go", "build", "-o", binPath, "github.com/turbolytics/sql-flow/cmd/sqlflow")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			binErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

// stack is one scenario's world: its own topic, group and database.
type stack struct {
	t        *testing.T
	bin      string
	broker   string
	topic    string
	group    string
	dsn      string
	pg       *pgx.Conn
	dir      string
	features Features
	// partitions is the topic's partition count.
	partitions int32
	// idleClose is the count window's idle_close_seconds.
	idleClose int
	// lateness is the count window's allowed_lateness_seconds.
	lateness int
}

func newStack(t *testing.T, f Features) *stack {
	t.Helper()
	b := broker(t)
	dsn, _ := sharedPostgres.Database(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	_, err = pg.Exec(ctx, `SET TIME ZONE 'UTC'`)
	assert.NoError(t, err)
	_, err = pg.Exec(ctx, `CREATE TABLE usage_per_minute (
		minute          TIMESTAMPTZ NOT NULL,
		customer        TEXT        NOT NULL,
		meter           TEXT        NOT NULL,
		kafka_partition INTEGER     NOT NULL,
		quantity        BIGINT      NOT NULL,
		PRIMARY KEY (minute, customer, meter, kafka_partition))`)
	assert.NoError(t, err)

	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	s := &stack{
		t:          t,
		bin:        sqlflowBinary(t),
		broker:     b,
		topic:      fmt.Sprintf("usage-%s-%d", name, time.Now().UnixNano()),
		group:      fmt.Sprintf("count-%s-%d", name, time.Now().UnixNano()),
		dsn:        dsn,
		pg:         pg,
		dir:        scenarioDir(t),
		features:   f,
		partitions: 6,
		idleClose:  5,
		lateness:   60,
	}
	s.createTopic()
	return s
}

func (s *stack) createTopic() {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker))
	assert.NoError(s.t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = s.topic, s.partitions, 1
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = 20000
	resp, err := req.RequestWith(ctx, cl)
	assert.NoError(s.t, err)
	assert.Equal(s.t, int16(0), resp.Topics[0].ErrorCode)
}

// event is one metered quantity, after ingest unnests it: one row per meter.
type event struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Meter    string `json:"meter"`
	Quantity int64  `json:"quantity"`
	TSMillis int64  `json:"ts_ms"`
}

// workload is a fixed event set: customers x perCustomer events, each with
// three meters, spread evenly over span from start. The seed makes a
// failure reproducible.
func workload(seed int64, customers, perCustomer int, start time.Time, span time.Duration) []event {
	r := rand.New(rand.NewSource(seed))
	var out []event
	n := customers * perCustomer
	for i := 0; i < n; i++ {
		c := fmt.Sprintf("user_%05d", i%customers)
		at := start.Add(time.Duration(int64(span) * int64(i) / int64(n)))
		id := fmt.Sprintf("evt_%d_%d", seed, i)
		for _, m := range []struct {
			meter string
			q     int64
		}{{"requests", 1}, {"input_tokens", 100 + r.Int63n(900)}, {"output_tokens", 50 + r.Int63n(400)}} {
			out = append(out, event{ID: id, Customer: c, Meter: m.meter, Quantity: m.q, TSMillis: at.UnixMilli()})
		}
	}
	return out
}

type totalKey struct {
	Minute   time.Time
	Customer string
	Meter    string
}

// expected is the exact answer: the sum of quantity per (minute, customer,
// meter) over distinct (id, meter). A duplicate in events counts once.
func expected(events []event) map[totalKey]int64 {
	seen := map[[2]string]bool{}
	out := map[totalKey]int64{}
	for _, e := range events {
		k := [2]string{e.ID, e.Meter}
		if seen[k] {
			continue
		}
		seen[k] = true
		minute := time.UnixMilli(e.TSMillis).UTC().Truncate(time.Minute)
		out[totalKey{minute, e.Customer, e.Meter}] += e.Quantity
	}
	return out
}

// produce writes events to the topic keyed by customer, as a keyed ingest
// does, synchronously.
func (s *stack) produce(events []event) {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker), kgo.ProducerLinger(5*time.Millisecond))
	assert.NoError(s.t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	recs := make([]*kgo.Record, len(events))
	for i, e := range events {
		v, err := json.Marshal(e)
		assert.NoError(s.t, err)
		recs[i] = &kgo.Record{Topic: s.topic, Key: []byte(e.Customer), Value: v}
	}
	assert.NoError(s.t, cl.ProduceSync(ctx, recs...).FirstErr())
}

// totals reads Postgres summed over partitions, which is what a reader of
// the stack sees.
func (s *stack) totals() map[totalKey]int64 {
	s.t.Helper()
	rows, err := s.pg.Query(context.Background(),
		`SELECT minute, customer, meter, sum(quantity)::BIGINT FROM usage_per_minute GROUP BY 1, 2, 3`)
	assert.NoError(s.t, err)
	defer rows.Close()
	out := map[totalKey]int64{}
	for rows.Next() {
		var k totalKey
		var q int64
		assert.NoError(s.t, rows.Scan(&k.Minute, &k.Customer, &k.Meter, &q))
		k.Minute = k.Minute.UTC()
		out[k] = q
	}
	assert.NoError(s.t, rows.Err())
	return out
}

// awaitExact waits until Postgres holds exactly want, then holds it for
// settle: a partial republish landing after the right answer is the
// failure the rebalance scenarios exist for, so reaching it once is not
// enough. On timeout it fails with the first differences.
func (s *stack) awaitExact(want map[totalKey]int64, timeout, settle time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := s.totals()
		if diff := diffTotals(want, got); len(diff) == 0 {
			hold := time.Now().Add(settle)
			for time.Now().Before(hold) {
				time.Sleep(500 * time.Millisecond)
				if d := diffTotals(want, s.totals()); len(d) > 0 {
					s.t.Fatalf("exact, then not: %d keys changed after the right answer, first: %s",
						len(d), strings.Join(first(d, 10), "; "))
				}
			}
			return
		} else if time.Now().After(deadline) {
			s.t.Fatalf("totals not exact after %s: %d keys differ\nby minute: %s\nby partition: %s\nfirst: %s\nworker logs: %s",
				timeout, len(diff), strings.Join(byMinute(want, got), "; "), strings.Join(s.byPartition(), "; "),
				strings.Join(first(diff, 10), "; "), s.dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// awaitMembers waits until the count group is stable with n members that
// each own a partition. Starting a worker is not joining: a process boots,
// connects and waits out two cooperative rebalances first, and what is
// produced before then is consumed by the workers already there.
func (s *stack) awaitMembers(n int, timeout time.Duration) {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker))
	assert.NoError(s.t, err)
	defer cl.Close()
	deadline := time.Now().Add(timeout)
	for !s.groupOwnedBy(cl, n) {
		if time.Now().After(deadline) {
			s.t.Fatalf("group %s did not settle on %d members owning partitions within %s; logs: %s",
				s.group, n, timeout, s.dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *stack) groupOwnedBy(cl *kgo.Client, n int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{s.group}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Groups) != 1 {
		return false
	}
	g := resp.Groups[0]
	if g.State != "Stable" || len(g.Members) != n {
		return false
	}
	for _, m := range g.Members {
		var a kmsg.ConsumerMemberAssignment
		if a.ReadFrom(m.MemberAssignment) != nil {
			return false
		}
		owned := 0
		for _, t := range a.Topics {
			owned += len(t.Partitions)
		}
		if owned == 0 {
			return false
		}
	}
	return true
}

// awaitMinute waits until Postgres holds a row for minute: the window
// manager has closed and published it.
func (s *stack) awaitMinute(minute time.Time, timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for k := range s.totals() {
			if k.Minute.Equal(minute) {
				return
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("minute %s not published within %s; worker logs: %s", minute.Format(time.RFC3339), timeout, s.dir)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// awaitAny waits until Postgres holds any row: some window has published.
func (s *stack) awaitAny(timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for len(s.totals()) == 0 {
		if time.Now().After(deadline) {
			s.t.Fatalf("nothing published within %s; worker logs: %s", timeout, s.dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func diffTotals(want, got map[totalKey]int64) []string {
	var out []string
	for k, w := range want {
		if g := got[k]; g != w {
			out = append(out, fmt.Sprintf("%s %s %s want %d got %d", k.Minute.Format(time.RFC3339), k.Customer, k.Meter, w, g))
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			out = append(out, fmt.Sprintf("%s %s %s want nothing got %d", k.Minute.Format(time.RFC3339), k.Customer, k.Meter, g))
		}
	}
	sort.Strings(out)
	return out
}

// byPartition reads what each partition holds per minute in Postgres: rows
// and the sum of the requests meter, which is one per event.
func (s *stack) byPartition() []string {
	s.t.Helper()
	rows, err := s.pg.Query(context.Background(),
		`SELECT minute, kafka_partition, count(*)::BIGINT, sum(quantity) FILTER (WHERE meter = 'requests')::BIGINT
		 FROM usage_per_minute GROUP BY 1, 2 ORDER BY 1, 2`)
	if err != nil {
		return []string{err.Error()}
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var minute time.Time
		var part int32
		var n, req *int64
		if err := rows.Scan(&minute, &part, &n, &req); err != nil {
			return append(out, err.Error())
		}
		r := int64(0)
		if req != nil {
			r = *req
		}
		out = append(out, fmt.Sprintf("%s p%d rows=%d requests=%d", minute.UTC().Format("15:04"), part, *n, r))
	}
	return out
}

// byMinute summarizes a mismatch per minute: keys that differ and the sums
// wanted and got, so a loss reads as which minutes and how much.
func byMinute(want, got map[totalKey]int64) []string {
	type agg struct{ keys, want, got int64 }
	m := map[time.Time]*agg{}
	at := func(t time.Time) *agg {
		if m[t] == nil {
			m[t] = &agg{}
		}
		return m[t]
	}
	for k, w := range want {
		a := at(k.Minute)
		a.want += w
		a.got += got[k]
		if got[k] != w {
			a.keys++
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			a := at(k.Minute)
			a.got += g
			a.keys++
		}
	}
	var minutes []time.Time
	for t := range m {
		minutes = append(minutes, t)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })
	var out []string
	for _, t := range minutes {
		a := m[t]
		out = append(out, fmt.Sprintf("%s keys_differ=%d want=%d got=%d", t.Format("15:04"), a.keys, a.want, a.got))
	}
	return out
}

func first(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

const countConfig = `
tables:
  sql:
    - name: usage_window
      sql: |
        CREATE TABLE IF NOT EXISTS usage_window (
          minute TIMESTAMPTZ,
          customer TEXT,
          meter TEXT,
          kafka_partition INTEGER,
          id_hash UHUGEINT,
          quantity BIGINT
        );
      window:
        time_column: minute
        size_seconds: 60
        grace_seconds: 5
        idle_close_seconds: {{ .IdleClose }}
        allowed_lateness_seconds: {{ .Lateness }}
{{- if .Features.PartitionOwned }}
        partition_owned: true
{{- end }}
        emit_sql: |
          SELECT minute, customer, meter, kafka_partition, sum(quantity)::BIGINT AS quantity
          FROM (
            SELECT minute, customer, meter, kafka_partition, id_hash, any_value(quantity) AS quantity
            FROM closed
            GROUP BY ALL
          )
          GROUP BY ALL
        sink:
          type: postgres
          postgres:
            dsn: "{{ .DSN }}"
            table: usage_per_minute
            mode: upsert
            key: [minute, customer, meter, kafka_partition]
pipeline:
  name: count
  batch_size: 500
  flush_interval_seconds: 1
  state:
    path: {{ .StatePath }}
  source:
    type: kafka
    kafka:
      brokers: ["{{ .Broker }}"]
      group_id: {{ .Group }}
      auto_offset_reset: earliest
      topics: ["{{ .Topic }}"]
      event_time:
        path: ts_ms
        format: unix_ms
  handler:
    type: handlers.InferredMemBatch
    sql: |
      INSERT INTO usage_window
      SELECT
        time_bucket(INTERVAL '60 seconds', event_time) AS minute,
        customer,
        meter,
        kafka_partition,
        md5_number(id) AS id_hash,
        quantity
      FROM batch
  sink:
    type: noop
`

const ingestConfig = `
pipeline:
  name: ingest
  batch_size: {{ .BatchSize }}
  flush_interval_seconds: {{ .FlushSeconds }}
  source:
    type: webhook
    webhook:
      addr: "{{ .Addr }}"
{{- if .Features.Ack }}
      ack: after_flush
{{- end }}
  handler:
    type: handlers.InferredMemBatch
    sql: |
      SELECT e.id, e.customer, e.meter, e.quantity, e.ts_ms
      FROM (SELECT unnest(events) AS e FROM batch)
      WHERE e.id IS NOT NULL AND e.customer IS NOT NULL
  sink:
    type: kafka
    kafka:
      brokers: ["{{ .Broker }}"]
      topic: {{ .Topic }}
{{- if .Features.Key }}
      key: customer
{{- end }}
`

// worker is one sqlflow run process.
type worker struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan error
}

func (s *stack) render(name, tmpl string, vars map[string]any) string {
	s.t.Helper()
	vars["Features"] = s.features
	vars["Broker"] = s.broker
	vars["Topic"] = s.topic
	var buf bytes.Buffer
	assert.NoError(s.t, template.Must(template.New(name).Parse(tmpl)).Execute(&buf, vars))
	path := filepath.Join(s.dir, name+".yml")
	assert.NoError(s.t, os.WriteFile(path, buf.Bytes(), 0o644))
	return path
}

// startCount starts a count worker whose state lives at state, a name under
// the scenario's directory. A new name is a lost disk.
func (s *stack) startCount(name, state string) *worker {
	s.t.Helper()
	cfg := s.render(name, countConfig, map[string]any{
		"DSN": s.dsn, "Group": s.group, "StatePath": filepath.Join(s.dir, state+".duckdb"),
		"IdleClose": s.idleClose, "Lateness": s.lateness,
	})
	return s.start(name, cfg)
}

// startIngest starts the webhook ingest and returns its URL.
func (s *stack) startIngest(name string, batchSize, flushSeconds int) (*worker, string) {
	s.t.Helper()
	addr := freeAddr(s.t)
	cfg := s.render(name, ingestConfig, map[string]any{
		"Addr": addr, "BatchSize": batchSize, "FlushSeconds": flushSeconds,
	})
	w := s.start(name, cfg)
	url := "http://" + addr + "/events"
	waitHTTP(s.t, "http://"+addr+"/healthz", 60*time.Second)
	return w, url
}

func (s *stack) start(name, cfg string) *worker {
	s.t.Helper()
	logPath := filepath.Join(s.dir, name+".log")
	f, err := os.Create(logPath)
	assert.NoError(s.t, err)
	cmd := exec.Command(s.bin, "run", cfg)
	cmd.Stdout, cmd.Stderr = f, f
	assert.NoError(s.t, cmd.Start())
	w := &worker{name: name, cmd: cmd, log: logPath, done: make(chan error, 1)}
	go func() { w.done <- cmd.Wait(); f.Close() }()
	s.t.Cleanup(func() { w.kill9() })
	return w
}

// kill9 is a crash: no drain, no final pass, no commit.
func (w *worker) kill9() {
	if w.cmd.ProcessState == nil {
		_ = w.cmd.Process.Signal(syscall.SIGKILL)
		<-w.done
	}
}

// stop is a graceful leave: SIGTERM and wait for the drain.
func (w *worker) stop(t *testing.T) {
	t.Helper()
	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-w.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("%s did not exit within 60s of SIGTERM; log: %s", w.name, w.log)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := ln.Addr().String()
	assert.NoError(t, ln.Close())
	return addr
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := httpClient.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not answer 200 within %s", url, timeout)
}

// scenarioDir holds a scenario's configs, state and worker logs. Set
// SQLFLOW_METERING_KEEP to keep it after the test, for reading the logs of
// a failure.
func scenarioDir(t *testing.T) string {
	t.Helper()
	if os.Getenv("SQLFLOW_METERING_KEEP") == "" {
		return t.TempDir()
	}
	dir, err := os.MkdirTemp("", "metering-"+strings.ReplaceAll(t.Name(), "/", "-"))
	assert.NoError(t, err)
	t.Logf("keeping %s", dir)
	return dir
}

// meteringTier gates a scenario by SQLFLOW_METERING, the way the growth tier
// follows the ref. Unset (a developer running the package) and "full" run
// every scenario. "bounded" runs only the core and skips the slow rebalance,
// replay and duplicate scenarios that push the package past CI's 600s
// per-package budget. "off" skips all. CI sets bounded on a branch and full
// on main, so the full proof gates main and every release while a branch
// push stays fast.
func meteringTier(t *testing.T, slow bool) {
	t.Helper()
	switch os.Getenv("SQLFLOW_METERING") {
	case "off":
		t.Skip("SQLFLOW_METERING=off")
	case "bounded":
		if slow {
			t.Skip("SQLFLOW_METERING=bounded runs the core scenarios only; this one runs on main")
		}
	}
}

// recent is the start of a workload: whole minutes in the past, inside the
// engine's placement window and far enough back that every minute can close.
func recent(minutesAgo int) time.Time {
	return time.Now().UTC().Truncate(time.Minute).Add(-time.Duration(minutesAgo) * time.Minute)
}

// httpClient bounds every request: an after_flush 200 waits up to a flush
// interval, and a sender under a killed ingest must not hang the test.
var httpClient = &http.Client{Timeout: 30 * time.Second}
