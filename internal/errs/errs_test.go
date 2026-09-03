package errs

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

const goldenPath = "testdata/codes.golden"

func TestCodeSplitsIntoItsThreeParts(t *testing.T) {
	c := Code("user.sql.bind_failed")
	assert.Equal(t, ClassUser, c.Class())
	assert.Equal(t, "sql", c.Domain())
	assert.Equal(t, "bind_failed", c.Reason())
	assert.True(t, c.IsUser())
	assert.False(t, c.IsSystem())
}

// A malformed code must not report a class. Guessing one would route a
// failure to the wrong audience.
func TestMalformedCodeHasNoClass(t *testing.T) {
	for _, bad := range []Code{"", "nonsense", "user", "user.sql", "other.sql.x", "USER.sql.x"} {
		assert.Equal(t, Class(""), bad.Class())
		assert.False(t, bad.IsUser())
		assert.False(t, bad.IsSystem())
	}
}

func TestEveryRegisteredCodeIsWellFormed(t *testing.T) {
	for _, d := range All() {
		if d.Code.Class() == "" {
			t.Errorf("%q has no valid class", d.Code)
		}
		if len(strings.Split(string(d.Code), ".")) != 3 {
			t.Errorf("%q is not class.domain.reason", d.Code)
		}
		if d.Summary == "" {
			t.Errorf("%q has no summary", d.Code)
		}
		// A code an operator cannot act on is not usable by the people who
		// see it, which is the whole point of publishing codes.
		if d.Action == "" {
			t.Errorf("%q has no recommended action", d.Code)
		}
	}
}

// Every domain carries a catch-all, so a new failure has a home before anyone
// gives it a specific code. Without this, the next issue that adds an error
// invents a code in a hurry and the space fragments.
func TestEveryDomainHasACatchAll(t *testing.T) {
	catchAllReason := map[Class]string{ClassUser: "invalid", ClassSystem: "internal"}

	seen := map[string]bool{}
	for _, d := range All() {
		seen[string(d.Code)] = true
	}

	for _, d := range All() {
		class, domain := d.Code.Class(), d.Code.Domain()
		// The internal domain is itself the catch-all of last resort.
		if class == ClassSystem && domain == "internal" {
			continue
		}
		want := fmt.Sprintf("%s.%s.%s", class, domain, catchAllReason[class])
		if !seen[want] {
			t.Errorf("domain %s.%s has no catch-all; expected %q", class, domain, want)
		}
	}
}

// Codes are public API. Adding one is safe, because consumers match on the
// class prefix. Removing one, or changing what it means, breaks a provider's
// automation and the runbook their support team reads.
func TestRegistryIsAppendOnly(t *testing.T) {
	current := map[Code]bool{}
	lines := make([]string, 0, len(registry))
	for _, d := range All() {
		current[d.Code] = true
		lines = append(lines, string(d.Code))
	}
	sort.Strings(lines)

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("writing golden: %v", err)
		}
		t.Log("golden updated")
		return
	}

	raw, err := os.ReadFile(goldenPath)
	assert.NoError(t, err)

	for _, published := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if published == "" {
			continue
		}
		if !current[Code(published)] {
			t.Errorf("%q was published and is now missing; codes are append-only. "+
				"Restore it, or if you are certain it never shipped, run "+
				"UPDATE_GOLDEN=1 go test ./internal/errs", published)
		}
	}
}

func TestErrorPrefixesTheCode(t *testing.T) {
	e := New(CodeConfigNotFound, "config file not found: %s", "/nope.yml")
	assert.Equal(t, "[user.config.not_found] config file not found: /nope.yml", e.Error())

	wrapped := Wrap(CodeSinkUnreachable, errors.New("dial tcp: refused"), "clickhouse sink")
	assert.Equal(t, "[system.sink.unreachable] clickhouse sink: dial tcp: refused", wrapped.Error())

	positioned := New(CodeSQLBindFailed, "unknown column %q", "citty").At("sql", 12, 7)
	assert.Equal(t, `[user.sql.bind_failed] unknown column "citty" at sql:12:7`, positioned.Error())
}

// A multi-line cause is the normal case for YAML and SQL. The code has to
// survive on the first line, where an operator actually sees it.
func TestCodeStaysOnTheFirstLineOfAMultiLineCause(t *testing.T) {
	cause := errors.New("yaml: unmarshal errors:\n  line 3: field bad_key not found")
	got := Wrap(CodeConfigParseFailed, cause, "parsing YAML failed").Error()

	first, _, _ := strings.Cut(got, "\n")
	assert.True(t, strings.HasPrefix(first, "[user.config.parse_failed]"))
}

// The code has to survive the fmt.Errorf wrapping every interior call site
// already does, or converting the codebase would mean touching all 220 raise
// sites instead of the ~30 boundaries.
func TestCodeSurvivesWrapping(t *testing.T) {
	base := New(CodeStateCorrupt, "state file has no offsets table")
	wrapped := fmt.Errorf("opening state: %w", fmt.Errorf("loading offsets: %w", base))

	assert.Equal(t, CodeStateCorrupt, CodeOf(wrapped))
	assert.Equal(t, ClassSystem, ClassOf(wrapped))
	assert.True(t, HasCode(wrapped, CodeStateCorrupt))
	assert.True(t, errors.Is(wrapped, error(base)))
}

// An uncoded error is ours until proven otherwise. Defaulting to a user error
// would blame a customer for our bug.
func TestUncodedErrorReportsAsInternal(t *testing.T) {
	assert.Equal(t, CodeInternalUnexpected, CodeOf(errors.New("bare")))
	assert.Equal(t, ClassSystem, ClassOf(errors.New("bare")))
	assert.Equal(t, Code(""), CodeOf(nil))
}

// The boundary nearest the user decides how to describe the failure, so the
// outermost code wins.
func TestOutermostCodeWins(t *testing.T) {
	inner := New(CodeSinkWriteFailed, "rejected")
	outer := Wrap(CodeSinkUnreachable, inner, "after retries")
	assert.Equal(t, CodeSinkUnreachable, CodeOf(outer))
}

func TestPositionOfFindsAWrappedPosition(t *testing.T) {
	base := New(CodeSQLBindFailed, "unknown column").At("sql", 3, 9)
	wrapped := fmt.Errorf("invoking handler: %w", base)

	pos, ok := PositionOf(wrapped)
	assert.True(t, ok)
	assert.Equal(t, "sql", pos.Source)
	assert.Equal(t, 3, pos.Line)
	assert.Equal(t, 9, pos.Column)

	_, ok = PositionOf(New(CodeSQLBindFailed, "no position"))
	assert.False(t, ok)
}

func TestLookupReportsUnknownCodes(t *testing.T) {
	d, ok := Lookup(CodeConfigNotFound)
	assert.True(t, ok)
	assert.Equal(t, CodeConfigNotFound, d.Code)

	// A newer component can emit a code this build has never heard of. The
	// caller falls back to the class prefix rather than failing.
	_, ok = Lookup(Code("user.sql.from_the_future"))
	assert.False(t, ok)
	assert.True(t, Code("user.sql.from_the_future").IsUser())
}
