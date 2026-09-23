package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/turbostats/wire"
)

// DefaultTurboStatsInterval is how often an instance reports when its config
// does not say. A minute: the receiver derives rates from consecutive
// bundles, and a minute is dense enough to see one change.
const DefaultTurboStatsInterval = 60

// TurboStats configures the outbound reporter.
//
// Reporting and the local endpoint are independent. A fleet instance usually
// sets only report_to and leaves --turbostats off: the control plane cannot
// reach it, so serving a route would listen on a port for nobody.
type TurboStats struct {
	// ID is the operator's name for this instance. The receiver files a
	// bundle under it, so reporting without one cannot be filed at all.
	ID string `yaml:"id,omitempty"`
	// ReportTo is where bundles are posted. Empty means reporting is off,
	// which is the ordinary case for an instance with no control plane.
	ReportTo string `yaml:"report_to,omitempty"`
	// Key is the credential the control plane issued. It is the private half
	// of an Ed25519 keypair and belongs in an environment variable, which the
	// template renders in.
	Key string `yaml:"key,omitempty"`
	// IntervalSeconds is how often to report. Absent means the default.
	IntervalSeconds int `yaml:"interval_seconds,omitempty"`
	// Labels are the operator's own, copied into every report. They are
	// fixed for the life of the process: a receiver stores a series per
	// field, and a set that changed mid-run would split one instance's
	// history in two.
	Labels map[string]string `yaml:"labels,omitempty"`
}

// The limits on labels. A report has to stay a fixed shape and a bounded
// size: these hold on a cellular uplink at a report a minute, and they stop
// a loop over a data value from growing the bundle.
const (
	MaxLabels        = 10
	MaxLabelKeyLen   = 32
	MaxLabelValueLen = 64
)

// labelKey is what a receiver can use as a column or a filter name without
// escaping it.
var labelKey = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// reservedLabels are the names this contract already defines. A label that
// shadows one makes two different things share a name.
var reservedLabels = map[string]bool{
	"id": true, "name": true, "version": true, "commit": true, "arch": true,
	"config_hash": true, "source_type": true, "sink_type": true, "handler_type": true,
}

// Enabled reports whether this instance posts anywhere.
func (t *TurboStats) Enabled() bool { return t != nil && t.ReportTo != "" }

// Interval is how often to report, defaulted.
func (t *TurboStats) Interval() time.Duration {
	if t == nil || t.IntervalSeconds <= 0 {
		return DefaultTurboStatsInterval * time.Second
	}
	return time.Duration(t.IntervalSeconds) * time.Second
}

// Check validates the block.
//
// at is where this block sits in the document: {"pipeline","turbostats"}
// under run, {"serve","turbostats"} under serve. The caller supplies it
// because the block is reachable from two places and a violation naming a
// path the operator cannot find in their file is a violation they cannot
// act on.
//
// Order matters: the first violation is the one an operator reads, so the
// destination comes before what is sent to it.
func (t *TurboStats) Check(at []string) []Violation {
	if t == nil {
		// A config with no turbostats block at all. Enabled() used to absorb
		// this, and the label rules read the block before asking whether
		// reporting is on.
		return nil
	}

	var out []Violation
	add := func(code errs.Code, key, format string, args ...any) {
		out = append(out, Violation{
			Code: code, Path: append(append([]string{}, at...), key),
			Message: fmt.Sprintf(format, args...),
		})
	}

	// Labels are checked whether or not reporting is on. A label set that
	// would be refused the moment someone sets report_to has the defect
	// now, and finding it then means finding it in production.
	if len(t.Labels) > MaxLabels {
		add(errs.CodeConfigInvalid, "labels",
			"turbostats.labels has %d entries; at most %d", len(t.Labels), MaxLabels)
	}
	for k, v := range t.Labels {
		switch {
		case reservedLabels[k]:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is a field the bundle already carries", k)
		case !labelKey.MatchString(k):
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is not a label name: lower case, starting with a letter, [a-z0-9_]", k)
		case len(k) > MaxLabelKeyLen:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s is %d characters; at most %d", k, len(k), MaxLabelKeyLen)
		case len(v) > MaxLabelValueLen:
			add(errs.CodeConfigInvalid, "labels",
				"turbostats.labels.%s has a %d character value; at most %d", k, len(v), MaxLabelValueLen)
		}
	}

	if !t.Enabled() {
		// Nothing else to validate. An instance with no control plane is the
		// ordinary case, and a half-filled block that reports nowhere is not
		// an error a pipeline should refuse to start over.
		return out
	}

	if reason := reportToProblem(t.ReportTo); reason != "" {
		add(errs.CodeConfigInvalid, "report_to", "turbostats.report_to %s", reason)
	}
	if t.ID == "" {
		add(errs.CodeConfigInvalid, "id",
			"turbostats.id is required when report_to is set: the control plane files a bundle under it")
	}
	if _, err := wire.ParseCredential(t.Key); err != nil {
		// Never echo the key. A validation message ends up in a log, an
		// issue, or a screenshot.
		add(errs.CodeConfigInvalid, "key",
			"turbostats.key is not a credential; `sqlflow-control credential create` prints one")
	}
	if t.IntervalSeconds < 0 {
		add(errs.CodeConfigInvalid, "interval_seconds",
			"turbostats.interval_seconds %d is not a duration", t.IntervalSeconds)
	}
	return out
}

// reportToProblem says why a destination is refused, or returns empty.
//
// Plaintext is refused off the loopback because a device is on a public
// network: the bundle and its headers would cross it in the clear, and a
// signature proves who sent a document rather than hiding it.
func reportToProblem(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "is not a URL"
	}
	switch u.Scheme {
	case "https":
		return ""
	case "http":
		if isLoopback(u.Hostname()) {
			// Someone testing against a control plane on their own machine.
			// There is no network to listen on.
			return ""
		}
		return "is plaintext to a public address; use https"
	}
	return "must be http or https"
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// CheckError folds Check into one error for a process that must refuse to
// start.
//
// run had no such call. A config validate rejects -- plaintext to a public
// address, or a missing id -- started anyway and posted signed bundles in
// the clear, which is exactly what reportToProblem is written to refuse.
func (t *TurboStats) CheckError(at []string) error {
	violations := t.Check(at)
	if len(violations) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("the turbostats config is invalid")
	for _, v := range violations {
		fmt.Fprintf(&b, "\n  %s: %s", strings.Join(v.Path, "."), v.Message)
	}
	return errs.New(violations[0].Code, "%s", b.String())
}
