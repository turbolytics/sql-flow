// Package buildinfo carries what the build stamped into the binary.
//
// A leaf package on purpose: internal/cli prints these and internal/cli/run
// reports them in the TurboStats bundle, and run cannot import cli without a
// cycle. Both import this.
//
// Stamped at link time rather than read from the environment, because the
// version is a fact about the artifact, not a claim about the environment it
// was started in. An operator who upgrades a binary and forgets to update an
// environment variable would otherwise report the old version forever, and
// that is the number a fleet page can least afford to have wrong.
package buildinfo

// Version and Commit are stamped at link time by the Makefile, the Dockerfile
// and the release script:
//
//	go build -ldflags "-X github.com/turbolytics/sql-flow/internal/buildinfo.Version=v1.0.0"
//
// A plain `go build ./cmd/sqlflow/` leaves the defaults below, which is how an
// unreleased local binary identifies itself.
var (
	Version = "dev"
	Commit  = "unknown"
)
