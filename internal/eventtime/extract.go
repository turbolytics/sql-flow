// Package eventtime reads a record's own time out of its payload.
//
// This is the timestamp assigner for a source whose protocol carries no
// event time of its own. Kafka stamps every record; a websocket frame or a
// webhook body carries a time only where the producer put one, at a path of
// the producer's choosing, in a unit of the producer's choosing. The
// operator names both, and from then on the record's time is the record's,
// not the moment it arrived.
//
// The extractor decides only whether a value is present and parseable.
// Whether the time it yields is believable -- before 2020, or ahead of the
// engine's clock -- is the engine's placement rule, applied afterwards, so a
// producer whose clock is wrong is refused by the same rule whatever its
// protocol.
package eventtime

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
)

// Format is how a field encodes an instant.
type Format string

const (
	UnixSeconds      Format = "unix_s"
	UnixMilliseconds Format = "unix_ms"
	UnixMicroseconds Format = "unix_us"
	UnixNanoseconds  Format = "unix_ns"
	RFC3339          Format = "rfc3339"
)

// Formats is every encoding the extractor understands, in the order a
// message should list them.
var Formats = []Format{UnixSeconds, UnixMilliseconds, UnixMicroseconds, UnixNanoseconds, RFC3339}

// Extractor reads one field of a JSON payload as an instant.
type Extractor struct {
	// path is the field, split on dots: "time_us", or
	// "commit.record.createdAt".
	path   []string
	format Format
	// basis is the configured path as written, which is what a lag reading
	// names as the clock it was measured on.
	basis string
}

// New builds an extractor for a dotted path and a format. A path with no
// segments or a format not in Formats is refused here, so a bad
// configuration fails at start rather than on the first frame.
func New(path string, format Format) (*Extractor, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("event_time: path is required")
	}
	segments := strings.Split(path, ".")
	for _, s := range segments {
		if s == "" {
			return nil, fmt.Errorf("event_time: path %q has an empty segment", path)
		}
	}
	known := false
	for _, f := range Formats {
		if f == format {
			known = true
			break
		}
	}
	if !known {
		return nil, fmt.Errorf("event_time: format %q is not one of %s", format, joinFormats())
	}
	return &Extractor{path: segments, format: format, basis: path}, nil
}

// Basis is the name a lag reading reports for the clock this extractor reads:
// the path, as configured.
func (e *Extractor) Basis() string { return e.basis }

// Extract returns the field's instant in Unix nanoseconds. An error means the
// record has no usable event time: the path is absent, the value is the wrong
// type, or it does not parse in the configured format. The caller decides
// what a record without one means; on a windowing pipeline it is refused.
func (e *Extractor) Extract(payload []byte) (int64, error) {
	val, kind, _, err := jsonparser.Get(payload, e.path...)
	if err != nil || kind == jsonparser.NotExist {
		return 0, fmt.Errorf("event_time: %s is absent", e.basis)
	}
	switch e.format {
	case RFC3339:
		if kind != jsonparser.String {
			return 0, fmt.Errorf("event_time: %s is %s, not a string", e.basis, kind)
		}
		t, err := time.Parse(time.RFC3339Nano, string(val))
		if err != nil {
			return 0, fmt.Errorf("event_time: %s is not RFC 3339: %w", e.basis, err)
		}
		return t.UnixNano(), nil
	default:
		// A number, or a string holding one: producers that serialise
		// 64-bit integers as strings to survive JavaScript are common.
		if kind != jsonparser.Number && kind != jsonparser.String {
			return 0, fmt.Errorf("event_time: %s is %s, not a number", e.basis, kind)
		}
		n, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("event_time: %s is not an integer: %w", e.basis, err)
		}
		return toNanos(n, e.format), nil
	}
}

func toNanos(n int64, f Format) int64 {
	switch f {
	case UnixSeconds:
		return n * int64(time.Second)
	case UnixMilliseconds:
		return n * int64(time.Millisecond)
	case UnixMicroseconds:
		return n * int64(time.Microsecond)
	default:
		return n
	}
}

func joinFormats() string {
	names := make([]string, len(Formats))
	for i, f := range Formats {
		names[i] = string(f)
	}
	return strings.Join(names, ", ")
}
