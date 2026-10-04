// Package schema is the TurboStats contract as JSON Schema, for a reporter
// or receiver that is not written in Go.
//
// The documents are generated from the types in turbostats/wire by `make
// schema`, and a golden test fails when they fall behind. The Go types are
// the contract; these are an artifact of them.
//
// The control plane serves them at their IDs, from the version of this
// module it builds against, so the schema it serves is the schema of what
// it accepts. Like wire, this package imports only the standard library.
//
// The schema checks shape. It cannot check the contract's rules about how a
// process's reports relate over time: that a field set never changes, that
// counters only rise within one pipeline.started_at, that backfill is
// present from the first report. Those live in the specs and the tests.
package schema

import _ "embed"

// The IDs are the URLs the documents are published at. They carry the
// contract's version, as the media type and the ingest route do, so a v2
// contract gets new URLs and v1's stay.
const (
	BundleID   = "https://control.turbolytics.io/v1/turbostats/bundle.schema.json"
	ResponseID = "https://control.turbolytics.io/v1/turbostats/response.schema.json"
)

// Bundle is the schema of one report.
//
//go:embed bundle.schema.json
var Bundle []byte

// Response is the schema of the body of every 2xx answer to a report.
//
//go:embed response.schema.json
var Response []byte
