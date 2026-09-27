package core

import "time"

// bucketOrigin is where DuckDB's time_bucket aligns its buckets: 2000-01-03,
// a Monday, so that week-sized buckets start on Mondays. The engine buckets
// from the same origin so that the bucket it decides a record's lateness
// against is the bucket the handler's time_bucket puts the row in.
//
// Go's time.Truncate aligns to the zero time, year 1 -- also a Monday, which
// is why the two agree for any size that divides a day and for multiples of
// a week, and disagree for sizes like 25 hours or 3 days. Using DuckDB's
// origin explicitly makes them agree for every size; BucketStart's test
// proves it against DuckDB itself.
var bucketOrigin = time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)

// BucketStart is time_bucket(INTERVAL 'size', at): the start of the bucket
// of length size that holds at, aligned to bucketOrigin. at is after the
// origin for every event time the engine places (EventTimeFloor is 2020).
func BucketStart(at time.Time, size time.Duration) time.Time {
	since := at.Sub(bucketOrigin)
	return bucketOrigin.Add(since - since%size)
}

// BucketEnd is the end of the bucket holding at: BucketStart + size. A
// bucket is closed when its end is at or before the watermark.
func BucketEnd(at time.Time, size time.Duration) time.Time {
	return BucketStart(at, size).Add(size)
}
