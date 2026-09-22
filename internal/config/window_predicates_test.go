package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// HasWindow decides whether the watermark store is created. A window with no
// idle_close_seconds still needs it: the store holds the watermark, and the
// idle close is only one of the two ways it moves.
func TestManagerWindow_HasWindowIsAnyWindow(t *testing.T) {
	coverage.Covers(t, "manager.window")

	table := func(name string, w *Window) TableSQL { return TableSQL{Name: name, Window: w} }

	for _, tc := range []struct {
		name string
		conf Conf
		want bool
	}{
		{"no tables block", Conf{}, false},
		{"tables without a window", Conf{Tables: &Tables{SQL: []TableSQL{table("t", nil)}}}, false},
		{"a window without idle_close_seconds",
			Conf{Tables: &Tables{SQL: []TableSQL{table("t", &Window{SizeSeconds: 60})}}}, true},
		{"a window with idle_close_seconds",
			Conf{Tables: &Tables{SQL: []TableSQL{table("t", &Window{SizeSeconds: 60, IdleCloseSeconds: 10})}}}, true},
		{"one among plain tables",
			Conf{Tables: &Tables{SQL: []TableSQL{table("a", nil), table("b", &Window{SizeSeconds: 60})}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.conf.HasWindow())
		})
	}
}
