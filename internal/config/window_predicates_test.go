package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// HasWindow and ReadsLastArrival answer different questions and must disagree
// exactly where a window has no idle_close_seconds: it needs a watermark
// store, and it never reads the arrival clock. Getting the second one wrong in
// the permissive direction closes windows early; in the other it pays a
// statement per commit for a column nothing reads.
func TestManagerWindow_ConfigPredicatesSplitOnIdleClose(t *testing.T) {
	coverage.Covers(t, "manager.window")

	table := func(name string, w *Window) TableSQL { return TableSQL{Name: name, Window: w} }

	for _, tc := range []struct {
		name         string
		conf         Conf
		hasWindow    bool
		readsArrival bool
	}{
		{"no tables block", Conf{}, false, false},
		{"tables without a window", Conf{Tables: &Tables{SQL: []TableSQL{table("t", nil)}}}, false, false},
		{"a window without idle_close_seconds",
			Conf{Tables: &Tables{SQL: []TableSQL{table("t", &Window{SizeSeconds: 60})}}}, true, false},
		{"a window with idle_close_seconds",
			Conf{Tables: &Tables{SQL: []TableSQL{table("t", &Window{SizeSeconds: 60, IdleCloseSeconds: 10})}}}, true, true},
		{"one of each: the reader wins",
			Conf{Tables: &Tables{SQL: []TableSQL{
				table("a", &Window{SizeSeconds: 60}),
				table("b", &Window{SizeSeconds: 60, IdleCloseSeconds: 10}),
			}}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.hasWindow, tc.conf.HasWindow())
			assert.Equal(t, tc.readsArrival, tc.conf.ReadsLastArrival())
		})
	}
}
