package growth

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

var (
	testFunc = regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\) \{$`)
	funcEnd  = regexp.MustCompile(`(?m)^}$`)
)

// testBodies maps every test function in the module to its file and whether
// its body calls Check.
func testBodies(t *testing.T) (files map[string]string, checks map[string]bool) {
	t.Helper()
	files, checks = map[string]string{}, map[string]bool{}
	for _, dir := range []string{"cmd", "internal", "turbostats"} {
		err := filepath.WalkDir(filepath.Join("..", "..", dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return err
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(src)
			for _, m := range testFunc.FindAllStringSubmatchIndex(text, -1) {
				body := text[m[1]:]
				if end := funcEnd.FindStringIndex(body); end != nil {
					body = body[:end[0]]
				}
				name := text[m[2]:m[3]]
				files[name] = path
				checks[name] = strings.Contains(body, "growth.Check(t)")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return files, checks
}

// The growth pass selects its tests by name, and -short skips them by
// growth.Check. A test that calls Check under another name skips in the unit
// pass and is never selected by the growth pass: it runs nowhere, and its
// feature still counts every other test, so nothing else would say so. A
// TestGrowth test that does not call Check runs in the unit pass at full
// cost.
func TestToolingCoverageGrowth_TheNameAndTheSkipAgree(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")
	files, checks := testBodies(t)

	var names []string
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	growth := 0
	for _, name := range names {
		named := strings.HasPrefix(name, "TestGrowth")
		if named {
			growth++
		}
		switch {
		case checks[name] && !named:
			t.Errorf("%s (%s) calls growth.Check but is not named TestGrowth..., "+
				"so no pass runs it", name, files[name])
		case named && !checks[name]:
			t.Errorf("%s (%s) is named for the growth pass but does not call growth.Check, "+
				"so the unit pass runs it too", name, files[name])
		}
	}
	if growth == 0 {
		t.Fatal("found no TestGrowth test; the scan is broken")
	}
}

func TestToolingCoverageGrowth_BoundedUnlessAskedForFull(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")
	for env, want := range map[string]int{"": 1, "bounded": 1, "full": 2} {
		t.Setenv(Env, env)
		assert.Equal(t, want, Budget(t, 1, 2))
	}
}

// A typo that fell back to bounded would let a release pass on the short run.
func TestToolingCoverageGrowth_AnUnknownModeFails(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")
	t.Setenv(Env, "ful")
	rec := &recorder{}
	Current(rec)
	assert.Equal(t, 1, len(rec.fatals))
	assert.That(t, strings.Contains(rec.fatals[0], `SQLFLOW_GROWTH="ful"`))
}

// recorder stands in for *testing.T so a test can read a Fatalf without
// ending itself. testing.TB has an unexported method, so embedding it is the
// only way to satisfy the interface outside the testing package.
type recorder struct {
	testing.TB
	fatals []string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}
