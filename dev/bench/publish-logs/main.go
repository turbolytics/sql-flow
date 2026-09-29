// Command publish-logs streams a plain-text log file into Kafka, one JSON
// message per line, shaped the way a log shipper forwards a line it has not
// parsed: the raw line under a single field.
//
//	go run ./dev/bench/publish-logs -file access.log -topic logs
//	go run ./dev/bench/publish-logs -file jul95.log.gz,aug95.log.gz -topic nasa-http
//
// It exists for logs.archive.parquet.yml and logs.rollup.clickhouse.yml, whose
// handlers parse the line in SQL. Parsing in the pipeline rather than in the
// publisher is the point: it is what a reader is evaluating.
//
// Files may be gzipped, and several are published in order, so a corpus split
// across monthly files loads in one command.
//
// A windowing pipeline needs the line's own time rather than the moment the
// replay ran, and -event-time-field publishes it as its own field for a source
// to declare:
//
//	go run ./dev/bench/publish-logs -file jul95.gz,aug95.gz -topic nasa-http \
//	  -event-time-field ts -shift-to-now
//
// -shift-to-now moves the newest record to the start of the run and every
// other record with it, preserving the intervals. Two things make that
// necessary rather than cosmetic for an archived trace: the engine refuses an
// event time older than its floor, so an unshifted 1995 corpus publishes
// nothing at all; and a broker applies time-based retention to a record's own
// timestamp, so a trace stamped with its true age is deleted before a consumer
// reaches it. Intervals are preserved, so the buckets, their counts and the
// reduction ratio are the trace's own. Only the labels move.
//
// Records are counted, not newlines. A log file whose final line was truncated
// mid-write -- which is normal for a corpus captured from a running server --
// has one more record than it has newlines, and `wc -l` disagrees with this
// tool by one. The count printed here is the number of messages produced.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	files := flag.String("file", "", "log file(s) to publish, comma separated; .gz is decompressed in stream")
	topic := flag.String("topic", "", "destination topic (required)")
	brokers := flag.String("brokers", "localhost:9092", "kafka bootstrap servers, comma separated")
	field := flag.String("field", "message", "JSON field the raw line is published under")
	limit := flag.Int64("limit", 0, "stop after this many records; 0 publishes everything")
	timeField := flag.String("event-time-field", "", "publish each line's Common Log Format time under this JSON field, as RFC 3339, so a source can declare event_time: {path: <field>, format: rfc3339}")
	shift := flag.Bool("shift-to-now", false, "shift every timestamp so the newest record lands at the start of this run, preserving every interval between records; needs -event-time-field")
	flag.Parse()
	if *files == "" || *topic == "" {
		flag.Usage()
		log.Fatal("-file and -topic are required")
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		// Under the broker's default message.max.bytes, which is about 1 MiB.
		// A larger producer batch is rejected wholesale with MESSAGE_TOO_LARGE
		// and the whole run fails, so this stays below it rather than assuming
		// the broker was reconfigured.
		kgo.ProducerBatchMaxBytes(900_000),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	if *shift && *timeField == "" {
		log.Fatal("-shift-to-now needs -event-time-field: there is no timestamp to shift unless one is published")
	}

	ctx := context.Background()

	// The offset that lands the newest record at the moment this run starts.
	// A trace is usually older than the engine's event-time floor, and a
	// window cannot rest a watermark on a time it refuses, so replaying one
	// unshifted publishes nothing. Shifting preserves every interval between
	// records, so the bucket count, the rows per bucket and the reduction
	// ratio are the trace's own; only the labels move.
	var offset time.Duration
	if *shift {
		newest, err := newestTime(strings.Split(*files, ","))
		if err != nil {
			log.Fatalf("reading timestamps: %v", err)
		}
		// Whole days, rounded down. A shift of some arbitrary number of
		// hours and minutes would land the trace's traffic across different
		// hour boundaries than it originally fell on, and the bucket count
		// and rows per bucket would drift from the corpus's own. A whole
		// number of days preserves hour-of-day and day-of-week alignment, so
		// the rollup reproduces the figures the unshifted trace produced.
		// Rounding down also keeps every record at or before now, which
		// matters because the engine refuses an event time ahead of its own
		// clock.
		offset = time.Since(newest).Truncate(24 * time.Hour)
		fmt.Printf("shifting %s forward by %d days: newest record lands at %s\n",
			newest.UTC().Format("2006-01-02"), int(offset.Hours()/24),
			newest.Add(offset).UTC().Format(time.RFC3339))
	}
	// queued counts records handed to the client; produced counts those the
	// broker acknowledged. -limit has to bound the former, because the ack
	// callback runs far behind the read loop -- bounding produced overshot by
	// twentyfold.
	var queued, produced, failed, bytesOut int64
	var firstErr atomic.Value
	start := time.Now()

	for _, path := range strings.Split(*files, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if err := publishFile(ctx, cl, path, *field, *timeField, offset, *limit, &queued, &produced, &failed, &bytesOut, &firstErr, start); err != nil {
			log.Fatalf("%s: %v", path, err)
		}
	}

	if err := cl.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}

	elapsed := time.Since(start).Seconds()
	fmt.Printf("published=%d failed=%d json_bytes=%d elapsed=%.1fs rate=%.0f/s\n",
		produced, failed, bytesOut, elapsed, float64(produced)/elapsed)
	if v := firstErr.Load(); v != nil {
		// Surfaced rather than swallowed: a silent partial publish is the
		// worst outcome, because the pipeline downstream still looks healthy.
		log.Fatalf("first produce error: %s", v.(string))
	}
}

// clfTime reads the bracketed Common Log Format time out of a line:
// `199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET / HTTP/1.0" 200 6245`.
// A line whose time does not parse is published without the field rather than
// dropped; the archive keeps every line whatever its timestamp.
func clfTime(line string) (time.Time, bool) {
	i := strings.IndexByte(line, '[')
	if i < 0 {
		return time.Time{}, false
	}
	j := strings.IndexByte(line[i:], ']')
	if j < 0 {
		return time.Time{}, false
	}
	t, err := time.Parse("02/Jan/2006:15:04:05 -0700", line[i+1:i+j])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// newestTime reads every file once for the latest timestamp any line carries.
// A second pass costs a few seconds on a compressed corpus and is what lets
// the shift be computed without the caller doing arithmetic about a trace it
// has not read.
func newestTime(paths []string) (time.Time, error) {
	var newest time.Time
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			return time.Time{}, err
		}
		var r io.Reader = f
		var gz *gzip.Reader
		if strings.HasSuffix(path, ".gz") {
			if gz, err = gzip.NewReader(f); err != nil {
				f.Close()
				return time.Time{}, fmt.Errorf("gzip: %w", err)
			}
			r = gz
		}
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		for sc.Scan() {
			if t, ok := clfTime(sc.Text()); ok && t.After(newest) {
				newest = t
			}
		}
		err = sc.Err()
		if gz != nil {
			gz.Close()
		}
		f.Close()
		if err != nil {
			return time.Time{}, err
		}
	}
	if newest.IsZero() {
		return time.Time{}, fmt.Errorf("no line carried a Common Log Format timestamp")
	}
	return newest, nil
}

func publishFile(
	ctx context.Context,
	cl *kgo.Client,
	path, field, timeField string,
	offset time.Duration,
	limit int64,
	queued, produced, failed, bytesOut *int64,
	firstErr *atomic.Value,
	start time.Time,
) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		r = gz
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		// Marshalled rather than concatenated: a log line carries quotes and
		// backslashes, and the NASA traces carry raw control bytes from
		// clients that sent garbage. Hand-built JSON corrupts on those.
		rec := map[string]string{field: line}
		if timeField != "" {
			// A shipper that has already read the line forwards its time as
			// its own field. The raw line still carries everything else, so
			// the parsing the pipeline is being evaluated on is unchanged.
			// The Kafka record timestamp is deliberately left alone: a broker
			// applies time-based retention to it, and a trace stamped with
			// its own age is deleted before a consumer reaches it.
			if t, ok := clfTime(line); ok {
				rec[timeField] = t.Add(offset).Format(time.RFC3339)
			}
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		atomic.AddInt64(bytesOut, int64(len(b)))
		atomic.AddInt64(queued, 1)
		cl.Produce(ctx, &kgo.Record{Value: b}, func(_ *kgo.Record, err error) {
			if err != nil {
				if atomic.AddInt64(failed, 1) == 1 {
					firstErr.Store(err.Error())
				}
				return
			}
			if n := atomic.AddInt64(produced, 1); n%500_000 == 0 {
				fmt.Printf("  ... %d published (%.0fs)\n", n, time.Since(start).Seconds())
			}
		})
		if limit > 0 && atomic.LoadInt64(queued) >= limit {
			break
		}
	}
	return sc.Err()
}
