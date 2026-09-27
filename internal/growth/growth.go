// Package growth runs the growth tier: tests that drive a component for many
// iterations and fail if memory grows with them.
//
// The tiers run in order: unit, which is near instant; integration, which
// proves the function against a real service; growth, which proves a
// non-functional property once the function is proven; release, which proves
// the shipped image. A leak is linear in the work, so a growth check needs
// the iterations, and it does not shrink to fit the unit pass. It is named
// TestGrowth<Feature>_<Behaviour>, skips under -short, and runs in its own
// pass: `go test -run '^TestGrowth' ./...`, or `make test-growth`.
//
// A growth check runs at one of two lengths, from SQLFLOW_GROWTH. Bounded,
// the default, keeps a branch's growth pass to about ten or twenty seconds a
// test and must still catch the defect the check guards. Full runs at least
// as long as the check ran before the tier existed, and runs on main and
// before a release, where a slow leak has the time to show.
package growth

import (
	"os"
	"testing"
)

// Env names the variable that picks the length: bounded or full.
const Env = "SQLFLOW_GROWTH"

// Mode is the length a growth check runs at.
type Mode string

const (
	Bounded Mode = "bounded"
	Full    Mode = "full"
)

// Current is the mode SQLFLOW_GROWTH asks for. Unset is bounded, so a
// developer's run is quick. Any other value fails the test: a typo that fell
// back to bounded would pass a release on the short run.
func Current(t testing.TB) Mode {
	t.Helper()
	switch v := os.Getenv(Env); v {
	case "", string(Bounded):
		return Bounded
	case string(Full):
		return Full
	default:
		t.Fatalf("%s=%q: want %q or %q", Env, v, Bounded, Full)
		return ""
	}
}

// Check skips a growth check under -short, so the unit pass never runs one.
// Call it after coverage.Covers: the -short report then carries the test's
// markers, and the skip there is not mistaken for a test that covers nothing.
func Check(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("growth check: runs in the growth pass, make test-growth")
	}
	t.Logf("growth check, %s run", Current(t))
}

// Budget returns bounded or full, whichever the run asked for. Each check
// reads its iteration count or its duration through it, so the two lengths
// share one code path.
func Budget[T any](t testing.TB, bounded, full T) T {
	t.Helper()
	if Current(t) == Full {
		return full
	}
	return bounded
}
