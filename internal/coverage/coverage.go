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

import (
	"testing"

	"github.com/turbolytics/temper"
)

// toTemper forwards a claim to Temper, which records it as evidence when the
// test runs under `temper run` and does nothing otherwise. Only a real test
// forwards: the tests of this package pass a stand-in that records log lines.
func toTemper(t testing.TB, ref string, opts ...temper.Option) {
	switch t.(type) {
	case *testing.T, *testing.B:
		temper.Covers(t, ref, opts...)
	}
}

// Covers records additional features this test proves.
//
// It also states the claim to Temper (see the temper/ directory), which reads
// it from evidence rather than from the log. The log line stays: the matrix
// generator in scripts/coverage_matrix still gates merges.
//
// The marker goes to the test log, which `go test -json` carries as output
// events, so the matrix reads it from ordinary suite output with no plugin
// and no build tag.
func Covers(t testing.TB, features ...string) {
	t.Helper()
	for _, feature := range features {
		t.Logf("COVERS %s", feature)
		toTemper(t, feature)
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
	toTemper(t, invariant, temper.Subject(integration))
}
