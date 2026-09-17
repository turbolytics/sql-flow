package serve

import (
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// ceilTo rounds t up to the next multiple of bucket since the Unix epoch, in
// UTC, and leaves a t already on one alone. Microseconds, because that is
// what a timestamp param binds.
func ceilTo(t time.Time, bucket time.Duration) time.Time {
	us, b := t.UnixMicro(), bucket.Microseconds()
	r := us % b
	if r < 0 {
		r += b
	}
	if r != 0 {
		us += b - r
	}
	return time.UnixMicro(us).UTC()
}

// alignRange rounds a cached request's since and until up to the grain's
// bucket, in the values the statement binds and in the range the response
// echoes.
//
// Rounding up is exact for the half-open range a cached dataset asserts.
// Buckets are aligned, so for an aligned b, b >= since holds exactly when
// b >= ceil(since), and b < until exactly when b < ceil(until). Every request
// in one bucket-wide window therefore selects the same buckets, and they can
// share an answer. It is not exact for b <= until, which is why caching is
// the dataset author's claim and not the server's guess.
//
// The grain was chosen from the width as sent. Rounding can widen a range by
// less than a bucket, and a request that fit max_range must not be refused
// for it.
func alignRange(sinceName, untilName string, bucket time.Duration, values map[string]any, win *window) {
	since := ceilTo(values[sinceName].(time.Time), bucket)
	until := ceilTo(values[untilName].(time.Time), bucket)
	values[sinceName], values[untilName] = since, until
	win.Since, win.Until = since.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano)
}

// cacheKey spells one request: the dataset, the grain, and each declared
// param's value in declared order. Query-string order and the spelling of a
// timestamp's offset never reach it.
//
// Each value is tagged and a string is length-prefixed, so no value can
// spell another's encoding, and an absent param differs from an empty one:
// one binds NULL and the other an empty string.
func cacheKey(dataset, grain string, params []config.ServeParam, values map[string]any) string {
	var b strings.Builder
	b.WriteString(dataset)
	b.WriteByte(0)
	b.WriteString(grain)
	for _, p := range params {
		b.WriteByte(0)
		switch v := values[p.Name].(type) {
		case time.Time:
			b.WriteByte('t')
			b.WriteString(strconv.FormatInt(v.UnixMicro(), 10))
		case int64:
			b.WriteByte('i')
			b.WriteString(strconv.FormatInt(v, 10))
		case string:
			b.WriteByte('s')
			b.WriteString(strconv.Itoa(len(v)))
			b.WriteByte(':')
			b.WriteString(v)
		default:
			b.WriteByte('n')
		}
	}
	return b.String()
}
