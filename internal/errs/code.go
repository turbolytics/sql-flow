// Package errs defines SQLFlow's error codes and the error type that carries
// them.
//
// Codes are public API. A provider shows them to its own customers and writes
// automation against them, so a code never changes meaning and never
// disappears. See registry.go for the rule and the test that enforces it.
//
// A code has three dot-separated parts, class.domain.reason:
//
//	user.sql.bind_failed
//	system.sink.unreachable
//
// The class says whose fault it is. A consumer that has never seen a code
// still matches its class prefix and knows whether to show the customer a
// message or page an operator. That guarantee is what lets new codes ship
// without breaking anyone, so match on the prefix rather than on an
// exhaustive list.
package errs

import "strings"

// Class is the first part of a code. Two exist, and no third will be added:
// every failure is either something the user can fix or something we can.
type Class string

const (
	// ClassUser marks a failure the user can fix: bad SQL, bad config, a
	// schema that does not match the data.
	ClassUser Class = "user"

	// ClassSystem marks everything else: a source or sink that cannot be
	// reached, a resource limit, a bug of ours.
	ClassSystem Class = "system"
)

// Code identifies one failure. Use the constants in registry.go rather than
// building a Code from a string.
type Code string

// parts splits a code into its three components. It reports ok == false for
// anything malformed, so a half-formed code like "user.sql" never resolves to
// a class and never routes a failure to the wrong audience.
func (c Code) parts() (class, domain, reason string, ok bool) {
	p := strings.Split(string(c), ".")
	if len(p) != 3 || p[0] == "" || p[1] == "" || p[2] == "" {
		return "", "", "", false
	}
	return p[0], p[1], p[2], true
}

// Class returns the code's class, or an empty Class when the code is
// malformed.
func (c Code) Class() Class {
	class, _, _, ok := c.parts()
	if !ok {
		return ""
	}
	switch Class(class) {
	case ClassUser, ClassSystem:
		return Class(class)
	default:
		return ""
	}
}

// Domain returns the subsystem the failure came from: config, sql, source,
// sink, state, batch, limit or internal.
func (c Code) Domain() string {
	_, domain, _, ok := c.parts()
	if !ok {
		return ""
	}
	return domain
}

// Reason returns the specific failure within the domain.
func (c Code) Reason() string {
	_, _, reason, ok := c.parts()
	if !ok {
		return ""
	}
	return reason
}

// IsUser reports whether the user can fix this failure. Prefer it to
// comparing against a list of codes: it keeps working when new codes appear.
func (c Code) IsUser() bool { return c.Class() == ClassUser }

// IsSystem reports whether the failure is ours rather than the user's.
func (c Code) IsSystem() bool { return c.Class() == ClassSystem }
