// Package coverage attributes a test to the features it covers, beyond the
// one its name already claims.
//
// Attribution is by test name first, and that is deliberate: a test called
// TestSinkClickhouse_InsertsRows costs nothing to attribute and says what it
// covers to anyone reading it. One test per feature stays the goal.
//
// Some tests genuinely cover several features at once. An end-to-end run
// reads a Kafka source, drives a handler, writes a sink and commits state --
// naming it for one of those and calling the rest uncovered produces false
// gaps, and a gate that cries wolf gets ignored. Covers records the others.
//
// Reach for it only when a test really does prove several features. If a
// feature needs a marker to be covered at all, it wants its own test.
package coverage

import "testing"

// Covers records additional features this test proves.
//
// The marker goes to the test log, which `go test -json` carries as output
// events, so the matrix reads it from ordinary suite output with no plugin
// and no build tag.
func Covers(t *testing.T, features ...string) {
	t.Helper()
	for _, feature := range features {
		t.Logf("COVERS %s", feature)
	}
}
