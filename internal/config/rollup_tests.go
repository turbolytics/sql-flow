package config

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/errs"
)

// RollupTests is a file of fixture cases for `sqlflow rollup test`.
type RollupTests struct {
	Tests []RollupTestCase `yaml:"tests"`
}

// RollupTestCase is one case: writes to a rollup's source, each one
// statement in its own transaction, and the rows listed tables must hold
// afterward. Unlisted tables are checked by the invariants only.
type RollupTestCase struct {
	Name   string                      `yaml:"name"`
	Rollup string                      `yaml:"rollup"`
	Writes [][]map[string]any          `yaml:"writes"`
	Expect map[string][]map[string]any `yaml:"expect"`
}

// LoadRollupTests renders a test file and decodes it strictly, as the
// rollups file is.
func LoadRollupTests(path string) (*RollupTests, error) {
	rendered, err := RenderTemplate(path, nil)
	if err != nil {
		return nil, err
	}
	var t RollupTests
	if err := decodeStrict(rendered, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// RollupTableColumns is every table r declares and its columns, in order:
// the time column, the set's dimensions, then the measures by name. Tables
// are named as rollup.Table names them: the set, an underscore, the grain.
func RollupTableColumns(r Rollup) map[string][]string {
	out := map[string][]string{}
	for _, set := range r.DimensionSets {
		cols := append([]string{r.Source.TimeColumn}, set.Dimensions...)
		cols = append(cols, set.MeasureNames()...)
		for _, g := range r.Ladder() {
			out[set.Name+"_"+g.Name] = cols
		}
	}
	return out
}

// Check reports every rule a case breaks, at its YAML path: a name, a
// declared rollup whose source no rollup makes, at least one write, expect
// tables of that rollup or of a rollup chained from it, and expected rows
// that name every column of their table and no other. That every written
// key is a source column is checked against the database, which alone
// knows every column.
func (t *RollupTests) Check(conf *RollupsConf) []Violation {
	var out []Violation
	add := func(path []string, format string, args ...any) {
		out = append(out, Violation{Code: errs.CodeConfigRollup, Path: path, Message: fmt.Sprintf(format, args...)})
	}
	rollups := map[string]Rollup{}
	// owner maps each rollup table to the rollup that makes it.
	owner := map[string]string{}
	for _, r := range conf.Rollups {
		rollups[r.Name] = r
		for table := range RollupTableColumns(r) {
			owner[table] = r.Name
		}
	}
	for i, tc := range t.Tests {
		path := []string{"tests", strconv.Itoa(i)}
		if tc.Name == "" {
			add(at(path, "name"), "a test case needs a name")
		}
		if len(tc.Writes) == 0 {
			add(at(path, "writes"), "test case %q writes nothing", tc.Name)
		}
		r, ok := rollups[tc.Rollup]
		if !ok {
			add(at(path, "rollup"), "test case %q names rollup %q, which the rollups file does not declare", tc.Name, tc.Rollup)
			continue
		}
		// A write to a rollup table by hand is drift, so a chained rollup is
		// tested through the rollup its rows come from.
		if from, chained := owner[r.Source.Table]; chained {
			root := r
			for seen := map[string]bool{}; owner[root.Source.Table] != "" && !seen[root.Name]; {
				seen[root.Name] = true
				root = rollups[owner[root.Source.Table]]
			}
			add(at(path, "rollup"),
				"test case %q names rollup %s, whose source %s is a table of rollup %s; name rollup %s, whose writes reach %s through the triggers, and list %s's tables under expect",
				tc.Name, r.Name, r.Source.Table, from, root.Name, r.Name, r.Name)
			continue
		}
		tables := chainedTables(r, conf)
		for _, table := range slices.Sorted(maps.Keys(tc.Expect)) {
			cols, ok := tables[table]
			if !ok {
				add(at(path, "expect", table), "test case %q expects table %s, which neither rollup %s nor a rollup reading its tables declares",
					tc.Name, table, r.Name)
				continue
			}
			for j, row := range tc.Expect[table] {
				for _, c := range cols {
					if _, ok := row[c]; !ok {
						add(at(path, "expect", table, strconv.Itoa(j)),
							"test case %q: an expected %s row names no %s; name every column of the table", tc.Name, table, c)
						break
					}
				}
				for _, k := range slices.Sorted(maps.Keys(row)) {
					if !slices.Contains(cols, k) {
						add(at(path, "expect", table, strconv.Itoa(j), k),
							"test case %q: an expected %s row names %s, which the table does not have", tc.Name, table, k)
					}
				}
			}
		}
	}
	return out
}

// chainedTables is RollupTableColumns of r and of every rollup that reads
// one of those tables, directly or through another such rollup.
func chainedTables(r Rollup, conf *RollupsConf) map[string][]string {
	out := RollupTableColumns(r)
	added := map[string]bool{r.Name: true}
	for grew := true; grew; {
		grew = false
		for _, next := range conf.Rollups {
			if _, reads := out[next.Source.Table]; reads && !added[next.Name] {
				added[next.Name] = true
				maps.Copy(out, RollupTableColumns(next))
				grew = true
			}
		}
	}
	return out
}

// CheckError is Check as one coded error listing every violation, or nil.
func (t *RollupTests) CheckError(conf *RollupsConf) error {
	violations := t.Check(conf)
	if len(violations) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("the rollups test file is invalid")
	for _, v := range violations {
		fmt.Fprintf(&b, "\n  %s: %s", strings.Join(v.Path, "."), v.Message)
	}
	return errs.New(violations[0].Code, "%s", b.String())
}
