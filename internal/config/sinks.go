package config

import (
	"iter"
	"strconv"
)

// EachSink yields every sink a config declares, with the path of its block
// from the document root: the pipeline's sink, the dead-letter queue, and
// each window's sink. config.Sink is the shape of all three, so a rule about
// a sink is a rule about each of them, and a walk written by hand is where
// one gets missed. A sink a rule forgets is a sink that writes JSON under
// format: avro.
//
// A sequence index is its number, the form validate resolves to a line.
// Each path is the caller's to keep.
func (c *Conf) EachSink() iter.Seq2[[]string, Sink] {
	return func(yield func([]string, Sink) bool) {
		if !yield([]string{"pipeline", "sink"}, c.Pipeline.Sink) {
			return
		}
		if e := c.Pipeline.OnError; e != nil && e.DLQ != nil {
			if !yield([]string{"pipeline", "on_error", "dlq"}, *e.DLQ) {
				return
			}
		}
		if c.Tables == nil {
			return
		}
		for i, table := range c.Tables.SQL {
			if table.Window == nil {
				continue
			}
			if !yield([]string{"tables", "sql", strconv.Itoa(i), "window", "sink"}, table.Window.Sink) {
				return
			}
		}
	}
}
