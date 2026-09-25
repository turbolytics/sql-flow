package rollup

import (
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// maxChunkLocks bounds the advisory locks one backfill chunk takes: the
// buckets of its table it touches, and the buckets its upserts' triggers
// lock in every coarser grain built from it. The default lock table holds
// 6,400 locks for the whole server, and a chunk shares it with the
// pipeline's writes.
const maxChunkLocks = 1000

// binOrigin is the instant the generated SQL bins from, so a chunk boundary
// computed here and a bucket computed in Postgres agree.
var binOrigin = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// floorTo is the start of t's span-wide bin from binOrigin, as date_bin
// computes it, before the origin as well as after.
func floorTo(t time.Time, span time.Duration) time.Time {
	d := t.Sub(binOrigin)
	q := d / span
	if d < 0 && d%span != 0 {
		q--
	}
	return binOrigin.Add(q * span)
}

// cascadeWidths is e's width, then the width of each grain its upserts
// reach through the triggers: every grain built from e's grain, and every
// grain built from those. The ladder runs narrowest first, so a grain's
// from is decided before the grain.
func cascadeWidths(r config.Rollup, e edge) []time.Duration {
	fed := map[string]bool{e.Grain.Name: true}
	out := []time.Duration{e.Grain.Width}
	for _, g := range r.Ladder() {
		if fed[g.From] && !fed[g.Name] {
			fed[g.Name] = true
			out = append(out, g.Width)
		}
	}
	return out
}

// chunkLocks is the most advisory locks a chunk spanning span takes: at
// each width, the buckets the span covers, plus one it may straddle.
func chunkLocks(span time.Duration, widths []time.Duration) int {
	n := 0
	for _, w := range widths {
		n += int((span+w-1)/w) + 1
	}
	return n
}

// chunkSpan is how much source time one chunk of e's table covers: a UTC
// day, halved until the chunk fits maxChunkLocks. It never drops below the
// table's own width, where a chunk locks about one bucket per grain.
func chunkSpan(r config.Rollup, e edge) time.Duration {
	widths := cascadeWidths(r, e)
	span := 24 * time.Hour
	for span/2 >= e.Grain.Width && chunkLocks(span, widths) > maxChunkLocks {
		span /= 2
	}
	return span
}

// fromWidth is the width of the rows e's table is built from: the source
// grain's, or the finer rollup grain's.
func fromWidth(r config.Rollup, e edge) time.Duration {
	if e.Grain.From == r.Source.Grain {
		w, err := config.ParseServeDuration(r.Source.Grain)
		if err == nil {
			return w
		}
	}
	for _, g := range r.Ladder() {
		if g.Name == e.Grain.From {
			return g.Width
		}
	}
	return e.Grain.Width
}

// findEdge is the declared table named table.
func findEdge(r config.Rollup, table string) (edge, bool) {
	for _, e := range edges(r) {
		if e.Table == table {
			return e, true
		}
	}
	return edge{}, false
}
