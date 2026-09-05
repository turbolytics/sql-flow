package sinks

import (
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// Retry defaults. Deliberately short: the point is to absorb a hiccup, not to
// wait out an outage. A destination that is still refusing after a few seconds
// is better reported, because the exit code says "retryable" and a supervisor
// restarts the pipeline anyway.
//
// Retrying is on by default. Before this, one refused connection killed the
// process, which cost a cold start, a group rejoin and a rebalance to recover
// from a blip. That default was the defect, not a safe baseline.
const (
	DefaultRetryMaxAttempts    = 4
	DefaultRetryInitialBackoff = 100 * time.Millisecond
	DefaultRetryMaxBackoff     = 2 * time.Second
	DefaultRetryDeadline       = 10 * time.Second
)

// RetryPolicyFrom resolves a config block into a policy, filling in defaults
// for anything the user left out.
func RetryPolicyFrom(c *config.SinkRetry) RetryPolicy {
	p := RetryPolicy{
		MaxAttempts:    DefaultRetryMaxAttempts,
		InitialBackoff: DefaultRetryInitialBackoff,
		MaxBackoff:     DefaultRetryMaxBackoff,
		Deadline:       DefaultRetryDeadline,
	}
	if c == nil {
		return p
	}
	if c.MaxAttempts > 0 {
		p.MaxAttempts = c.MaxAttempts
	}
	if c.InitialBackoffMS > 0 {
		p.InitialBackoff = time.Duration(c.InitialBackoffMS) * time.Millisecond
	}
	if c.MaxBackoffMS > 0 {
		p.MaxBackoff = time.Duration(c.MaxBackoffMS) * time.Millisecond
	}
	if c.DeadlineSeconds > 0 {
		p.Deadline = time.Duration(c.DeadlineSeconds) * time.Second
	}
	return p
}

// Enabled reports whether the policy will ever make a second attempt.
func (p RetryPolicy) Enabled() bool { return p.MaxAttempts > 1 }
