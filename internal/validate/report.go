// Package validate checks a pipeline config without running it.
//
// Every check is offline: no broker, no sink, no network, and nothing from
// the config's commands block executes. That is what lets CI, a pre-commit
// hook, and an agent loop all call it without consequences.
package validate

import "sort"

// Status is the outcome of one check.
type Status string

const (
	StatusPass    Status = "pass"
	StatusFail    Status = "fail"
	StatusSkipped Status = "skipped"
)

// Severity separates a fault that must block from advice that must not.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Position points at the place in the user's own text a diagnostic refers to.
//
// Rendered marks a position measured against the rendered config rather than
// the file on disk. A template with control structures shifts line numbers,
// and reporting a shifted line as a source line sends an editor -- human or
// model -- to the wrong place.
type Position struct {
	Source   string `json:"source"`
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Rendered bool   `json:"rendered,omitempty"`
}

// Check records that one class of validation ran, and what it concluded.
//
// A check that could not run reports StatusSkipped with a Reason, never
// StatusPass. A consumer must be able to tell "checked and fine" from "not
// checked", because a model treats an unqualified pass as proof.
type Check struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Diagnostic is one fault, carrying what a reader needs to fix it without
// opening the source: the code, where it is, and what to do.
type Diagnostic struct {
	Code       string    `json:"code"`
	Class      string    `json:"class"`
	Severity   Severity  `json:"severity"`
	Message    string    `json:"message"`
	Position   *Position `json:"position,omitempty"`
	Context    string    `json:"context,omitempty"`
	DidYouMean []string  `json:"did_you_mean,omitempty"`
	Action     string    `json:"action,omitempty"`
}

// Variables reports the template's variable use from both sides.
//
// Unused is what makes a typo name itself: a missing
// SQLFLOW_AZURE_CONNECTION_STRING alongside an unread
// SQLFLOW_AZURE_STORAGE_CONNECTION_STRING is a misspelling, and reporting only
// the missing half leaves the reader to guess (#120).
type Variables struct {
	Referenced []string `json:"referenced"`
	Provided   []string `json:"provided"`
	Missing    []string `json:"missing"`
	Unused     []string `json:"unused,omitempty"`
}

// Request is one validation job.
//
// Config carries the config's text. Path names the file for diagnostics only.
// The split is deliberate: the pull-based control plane hands a job to an
// instance that shares no filesystem with the submitter (#178).
type Request struct {
	Path   string            `json:"path,omitempty"`
	Config string            `json:"config"`
	Vars   map[string]string `json:"vars,omitempty"`
}

// Report is the result of a validation job.
type Report struct {
	Config      string       `json:"config,omitempty"`
	OK          bool         `json:"ok"`
	Checks      []Check      `json:"checks"`
	Diagnostics []Diagnostic `json:"diagnostics"`
	Variables   *Variables   `json:"variables,omitempty"`
}

// Add appends a diagnostic.
func (r *Report) Add(d Diagnostic) {
	r.Diagnostics = append(r.Diagnostics, d)
}

// SetCheck records a check's outcome, replacing any earlier entry for the same
// id so a check cannot appear twice.
func (r *Report) SetCheck(id string, status Status, reason string) {
	for i := range r.Checks {
		if r.Checks[i].ID == id {
			r.Checks[i] = Check{ID: id, Status: status, Reason: reason}
			return
		}
	}
	r.Checks = append(r.Checks, Check{ID: id, Status: status, Reason: reason})
}

// Vars returns the variable report, or nil when the template never parsed.
func (r *Report) Vars() *Variables { return r.Variables }

// Finish orders the diagnostics and computes OK.
//
// The ordering is not cosmetic. jsonschema builds an error's causes from a
// map, so the same config reports the same faults in a different order on
// every run. A report that reorders cannot be diffed between runs, cached, or
// compared in a test, so diagnostics are sorted by where they are: top of the
// file first, and anything without a position ahead of everything else.
//
// Only an error clears OK. A warning is advice, and gating CI on advice makes
// the advice unwelcome.
func (r *Report) Finish() {
	sort.SliceStable(r.Diagnostics, func(i, j int) bool {
		a, b := r.Diagnostics[i], r.Diagnostics[j]

		al, ac := 0, 0
		if a.Position != nil {
			al, ac = a.Position.Line, a.Position.Column
		}
		bl, bc := 0, 0
		if b.Position != nil {
			bl, bc = b.Position.Line, b.Position.Column
		}

		if al != bl {
			return al < bl
		}
		if ac != bc {
			return ac < bc
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.Message < b.Message
	})

	r.OK = true
	for _, d := range r.Diagnostics {
		if d.Severity == SeverityError {
			r.OK = false
			return
		}
	}
}
