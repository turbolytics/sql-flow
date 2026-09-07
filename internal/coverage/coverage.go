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
func Covers(t testing.TB, features ...string) {
	t.Helper()
	for _, feature := range features {
		t.Logf("COVERS %s", feature)
	}
}

// Invariant records that this test proved one invariant for one integration.
//
// A feature attributes by test name. An invariant cannot: the conformance
// harness runs the same code for every integration, so the name says nothing
// about which one this run was. Only the marker knows, so it carries both
// ids.
func Invariant(t testing.TB, invariant, integration string) {
	t.Helper()
	t.Logf("COVERS invariant=%s integration=%s", invariant, integration)
}

// PipelineInvariant records that this test proved one invariant of the engine
// itself.
//
// A pipeline invariant is a property of the consume loop rather than of
// anything a config names, so there is no integration to credit. It carries
// "pipeline" in that slot, and the generator gives it a single cell.
func PipelineInvariant(t testing.TB, invariant string) {
	t.Helper()
	Invariant(t, invariant, "pipeline")
}
