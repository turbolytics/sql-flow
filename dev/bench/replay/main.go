// Command replay serves a recorded Jetstream capture over a websocket, paced by
// the events' own time_us, so a pipeline can consume hours of the real
// firehose in minutes without touching the public service.
//
//	go run ./dev/bench/replay -file posts.ndjson.gz -speed 10
//
// Every connection replays the file from its first line. -speed 10 sends an
// event recorded one second after the first at 100ms, so the stream clock the
// pipeline sees advances ten times faster than wall clock while every ratio
// between messages, batches and windows stays what it was live. -speed 0 sends
// as fast as the client reads. At the end of the file the connection stays
// open and idle, the way a quiet firehose looks, until the client leaves.
//
// The capture comes from dev/bench/record.py.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	ws "github.com/coder/websocket"
)

func main() {
	file := flag.String("file", "", "recorded Jetstream events, one JSON object per line, optionally gzipped")
	addr := flag.String("addr", ":8765", "listen address")
	speed := flag.Float64("speed", 10, "replay pace as a multiple of the recorded pace; 0 sends as fast as the client reads")
	limit := flag.Int64("limit", 0, "stop after this many events per connection; 0 replays the whole file")
	flag.Parse()
	if *file == "" {
		log.Fatal("-file is required")
	}
	if _, err := os.Stat(*file); err != nil {
		log.Fatal(err)
	}

	var conns atomic.Int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := ws.Accept(w, r, nil)
		if err != nil {
			log.Printf("accept: %v", err)
			return
		}
		id := conns.Add(1)
		defer c.CloseNow()
		log.Printf("conn %d: %s from %s", id, r.URL.RequestURI(), r.RemoteAddr)
		sent, err := replay(r.Context(), c, *file, *speed, *limit, id)
		if err != nil {
			log.Printf("conn %d: stopped after %d events: %v", id, sent, err)
			return
		}
		log.Printf("conn %d: end of file after %d events; holding the connection open", id, sent)
		// Read until the client goes away, so its close is noticed.
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	})
	log.Printf("serving %s on %s at speed %g", *file, *addr, *speed)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func open(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{gz, f}, nil
}

func replay(ctx context.Context, c *ws.Conn, path string, speed float64, limit, id int64) (int64, error) {
	rc, err := open(path)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 1<<20), 32<<20)

	var (
		sent       int64
		firstUS    int64
		start      = time.Now()
		lastReport = start
	)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if speed > 0 {
			us, err := jsonparser.GetInt(line, "time_us")
			if err != nil {
				return sent, fmt.Errorf("line %d: time_us: %w", sent+1, err)
			}
			if firstUS == 0 {
				firstUS = us
			}
			due := start.Add(time.Duration(float64(us-firstUS) / speed * float64(time.Microsecond)))
			if wait := time.Until(due); wait > time.Millisecond {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return sent, ctx.Err()
				}
			}
		}
		if err := c.Write(ctx, ws.MessageText, line); err != nil {
			return sent, err
		}
		sent++
		if now := time.Now(); now.Sub(lastReport) >= 30*time.Second {
			lastReport = now
			log.Printf("conn %d: %d events, %.0f/s", id, sent, float64(sent)/now.Sub(start).Seconds())
		}
		if limit > 0 && sent >= limit {
			return sent, nil
		}
	}
	return sent, sc.Err()
}
