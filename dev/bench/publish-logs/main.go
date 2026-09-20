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
// across monthly files loads in one command. Event time comes from the line
// itself, never from wall clock, so an event-time window sees the timestamps
// the log recorded and a replay behaves the same way every run.
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

	ctx := context.Background()
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
		if err := publishFile(ctx, cl, path, *field, *limit, &queued, &produced, &failed, &bytesOut, &firstErr, start); err != nil {
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

func publishFile(
	ctx context.Context,
	cl *kgo.Client,
	path, field string,
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
		b, err := json.Marshal(map[string]string{field: line})
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
