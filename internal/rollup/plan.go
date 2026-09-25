package rollup

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Plan is what one install does to one rollup's tables beyond creating the
// missing ones and replacing every function and trigger.
type Plan struct {
	// Backfill lists the tables to fill from the table each is built from.
	// `sqlflow rollup run` fills them.
	Backfill []string
	// Retain lists the tables of a grain or dimension set the file no longer
	// declares. They keep their rows and their triggers, so a rollback to
	// the previous file loses nothing.
	Retain []string
	// Restore lists retained tables the file declares again. Their triggers
	// never stopped, so they need no backfill.
	Restore []string
}

// changeRemedy ends every refusal: the old tables keep what they hold, and
// the new shape gets its own.
const changeRemedy = "declare a new rollup or dimension set for the new shape"

// PlanChange compares a rollup as last applied, prev, with the rollup as
// declared, r, at path in the file.
//
// prev is nil when the database has no state row for the rollup: a first
// install, or tables a migration created, which the install adopts. Nothing
// says those tables are complete, so every table built from the source is
// filled, and its upserts fire the triggers that fill the rest.
//
// missing holds each declared table that does not exist yet, and retained
// the state row's retained tables. A change that would corrupt stored rows
// is a violation, and the plan is empty.
func PlanChange(r config.Rollup, path []string, prev *Applied, retained []string, missing map[string]bool) (Plan, []config.Violation) {
	var plan Plan
	es := edges(r)
	if prev == nil {
		for _, e := range es {
			if e.Grain.From == r.Source.Grain {
				plan.Backfill = append(plan.Backfill, e.Table)
			}
		}
		return plan, nil
	}
	if v := changes(r, path, *prev); len(v) > 0 {
		return Plan{}, v
	}

	kept := map[string]bool{}
	for _, t := range retained {
		kept[t] = true
	}
	declared := map[string]bool{}
	for _, e := range es {
		declared[e.Table] = true
		if kept[e.Table] {
			plan.Restore = append(plan.Restore, e.Table)
		}
		// A table whose from is also new fills when its from fills: the
		// from's upserts fire this table's trigger.
		if missing[e.Table] && !missing[e.From] {
			plan.Backfill = append(plan.Backfill, e.Table)
		}
	}
	for _, set := range sortedKeys(prev.DimensionSets) {
		for _, g := range sortedKeys(prev.Grains) {
			if t := set + "_" + g; !declared[t] && !kept[t] {
				plan.Retain = append(plan.Retain, t)
			}
		}
	}
	return plan, nil
}

// changes returns every difference between prev and r that stored rows
// cannot absorb. An added or removed grain or dimension set is not one: it
// adds tables or leaves them in place.
func changes(r config.Rollup, path []string, prev Applied) []config.Violation {
	next := AppliedFrom(r)
	var out []config.Violation
	add := func(p []string, format string, args ...any) {
		out = append(out, config.Violation{Code: errs.CodeConfigRollupChange, Path: p, Message: fmt.Sprintf(format, args...)})
	}

	if !sameSource(prev.Source, next.Source) {
		add(yamlPath(path, "source"), "rollup %s: the source changed, and every stored row was built from the old one; %s",
			r.Name, changeRemedy)
	}
	for _, g := range sortedKeys(next.Grains) {
		if from, ok := prev.Grains[g]; ok && from != next.Grains[g] {
			add(yamlPath(path, "grains", g, "from"), "rollup %s grain %s: from changed from %s to %s, and the grain's triggers read the old one; %s",
				r.Name, g, from, next.Grains[g], changeRemedy)
		}
	}
	for i, set := range r.DimensionSets {
		old, ok := prev.DimensionSets[set.Name]
		if !ok {
			continue
		}
		cur := next.DimensionSets[set.Name]
		spath := yamlPath(path, "dimension_sets", strconv.Itoa(i))
		if !slices.Equal(old.Dimensions, cur.Dimensions) {
			add(yamlPath(spath, "dimensions"), "rollup %s dimension set %s: dimensions changed from [%s] to [%s], which changes every stored row's key; %s",
				r.Name, set.Name, strings.Join(old.Dimensions, ", "), strings.Join(cur.Dimensions, ", "), changeRemedy)
		}
		for _, m := range sortedKeys(old.Measures, cur.Measures) {
			was, had := old.Measures[m]
			now, has := cur.Measures[m]
			switch {
			case had && !has:
				add(yamlPath(spath, "measures"), "rollup %s dimension set %s: measure %s was removed, and its column would go stale in every stored row; %s",
					r.Name, set.Name, m, changeRemedy)
			case !had && has:
				add(yamlPath(spath, "measures", m), "rollup %s dimension set %s: measure %s was added, and the stored tables have no column for it; %s",
					r.Name, set.Name, m, changeRemedy)
			case was != now:
				add(yamlPath(spath, "measures", m), "rollup %s dimension set %s: measure %s changed from %s to %s, and every stored row was merged the old way; %s",
					r.Name, set.Name, m, describe(was), describe(now), changeRemedy)
			}
		}
	}
	return out
}

func sameSource(a, b AppliedSource) bool {
	return a.Table == b.Table && a.TimeColumn == b.TimeColumn && a.Grain == b.Grain &&
		slices.Equal(a.Dimensions, b.Dimensions)
}

// describe writes a measure the way a message names it: sum(posts) as integer.
func describe(m AppliedMeasure) string {
	s := m.Type
	if m.Column != "" {
		s += "(" + m.Column + ")"
	}
	if m.Numeric != "" {
		s += " as " + m.Numeric
	}
	return s
}

// sortedKeys is the union of the maps' keys, sorted, so a plan and a message
// come out the same on every run.
func sortedKeys[V any](ms ...map[string]V) []string {
	seen := map[string]bool{}
	for _, m := range ms {
		for k := range m {
			seen[k] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}
