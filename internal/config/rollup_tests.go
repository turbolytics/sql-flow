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
// declared rollup, at least one write, expect tables the rollup declares,
// and expected rows that name every column of their table. That every
// written key is a source column is checked against the database, which
// alone knows every column.
func (t *RollupTests) Check(conf *RollupsConf) []Violation {
	var out []Violation
	add := func(path []string, format string, args ...any) {
		out = append(out, Violation{Code: errs.CodeConfigRollup, Path: path, Message: fmt.Sprintf(format, args...)})
	}
	rollups := map[string]Rollup{}
	for _, r := range conf.Rollups {
		rollups[r.Name] = r
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
		tables := RollupTableColumns(r)
		for _, table := range slices.Sorted(maps.Keys(tc.Expect)) {
			cols, ok := tables[table]
			if !ok {
				add(at(path, "expect", table), "test case %q expects table %s, which rollup %s does not declare", tc.Name, table, r.Name)
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
