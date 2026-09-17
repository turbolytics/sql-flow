package config

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/errs"
)

var (
	rollupMeasureTypes         = map[string]bool{"sum": true, "min": true, "max": true, "count_buckets": true}
	rollupReservedMeasureTypes = map[string]bool{"avg": true, "gauge": true, "histogram": true}
	// A filter param may not take a name the generated dataset already uses,
	// or grain, which serve reserves.
	rollupReservedParams = map[string]bool{"since": true, "until": true, "top": true, "grain": true}
)

type rollupAdd func(path []string, format string, args ...any)

const nameRule = "must be lowercase letters, digits and underscores, starting with a letter"

// Check returns every rule the rollups file breaks, in document order. The
// generators refuse a file with any.
func (c *RollupsConf) Check() []Violation {
	var out []Violation
	add := func(path []string, format string, args ...any) {
		out = append(out, Violation{Code: errs.CodeConfigRollup, Path: path, Message: fmt.Sprintf(format, args...)})
	}

	if len(c.Rollups) == 0 {
		add([]string{"rollups"}, "rollups declares no rollup, so there is nothing to generate")
	}
	names := map[string]bool{}
	// Dataset names share one serve file, whichever rollup declares them.
	datasets := map[string]bool{}
	for i, r := range c.Rollups {
		path := []string{"rollups", strconv.Itoa(i)}
		switch {
		case !serveNamePattern.MatchString(r.Name):
			add(at(path, "name"), "rollup name %q %s", r.Name, nameRule)
		case names[r.Name]:
			add(at(path, "name"), "rollup %s is declared twice", r.Name)
		}
		names[r.Name] = true
		checkRollup(r, path, datasets, add)
	}
	return out
}

// CheckError folds Check into one error carrying every violation.
func (c *RollupsConf) CheckError() error {
	violations := c.Check()
	if len(violations) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("the rollups file is invalid")
	for _, v := range violations {
		fmt.Fprintf(&b, "\n  %s: %s", strings.Join(v.Path, "."), v.Message)
	}
	return errs.New(violations[0].Code, "%s", b.String())
}

func checkRollup(r Rollup, path []string, datasets map[string]bool, add rollupAdd) {
	src := at(path, "source")
	for _, f := range []struct{ key, value string }{{"table", r.Source.Table}, {"time_column", r.Source.TimeColumn}} {
		if !serveNamePattern.MatchString(f.value) {
			add(at(src, f.key), "rollup %s: source.%s %q %s", r.Name, f.key, f.value, nameRule)
		}
	}
	sourceDims := map[string]bool{}
	for k, d := range r.Source.Dimensions {
		if !serveNamePattern.MatchString(d) {
			add(at(src, "dimensions", strconv.Itoa(k)), "rollup %s: source dimension %q %s", r.Name, d, nameRule)
		}
		sourceDims[d] = true
	}

	// Rules 1, 2 and 3. Widths grow along every from, so no chain can cycle.
	widths := map[string]time.Duration{}
	if w, err := ParseServeDuration(r.Source.Grain); err != nil {
		add(at(src, "grain"), "rollup %s: source.grain %v", r.Name, err)
	} else {
		widths[r.Source.Grain] = w
	}
	grainNames := slices.Sorted(maps.Keys(r.Grains))
	if len(grainNames) == 0 {
		add(at(path, "grains"), "rollup %s declares no grain", r.Name)
	}
	for _, g := range grainNames {
		w, err := ParseServeDuration(g)
		switch {
		case err != nil:
			add(at(path, "grains", g), "rollup %s: grain name %q is not a duration such as 5m, 1h or 1d", r.Name, g)
		case g == r.Source.Grain:
			add(at(path, "grains", g), "rollup %s: grain %s is the source grain; the source table already holds it", r.Name, g)
		default:
			widths[g] = w
		}
	}
	for _, g := range grainNames {
		w, ok := widths[g]
		if !ok || g == r.Source.Grain {
			continue
		}
		from := r.Grains[g].From
		fw, known := widths[from]
		switch {
		case !known:
			add(at(path, "grains", g, "from"), "rollup %s grain %s: from %q is neither the source grain %s nor a declared grain",
				r.Name, g, from, r.Source.Grain)
		case w <= fw || w%fw != 0:
			add(at(path, "grains", g, "from"), "rollup %s grain %s: its width is not a whole multiple of from grain %s",
				r.Name, g, from)
		}
	}

	// Rules 4, 5 and 6.
	sets := map[string]bool{}
	for j, set := range r.DimensionSets {
		spath := at(path, "dimension_sets", strconv.Itoa(j))
		switch {
		case !serveNamePattern.MatchString(set.Name):
			add(at(spath, "name"), "rollup %s: dimension set name %q %s", r.Name, set.Name, nameRule)
		case sets[set.Name]:
			add(at(spath, "name"), "rollup %s: dimension set %s is declared twice", r.Name, set.Name)
		}
		sets[set.Name] = true
		for _, g := range grainNames {
			if set.Name+"_"+g == r.Source.Table {
				add(at(spath, "name"), "rollup %s: dimension set %s at grain %s would be named %s, which is the source table",
					r.Name, set.Name, g, r.Source.Table)
			}
		}

		dims := map[string]bool{}
		for k, d := range set.Dimensions {
			dpath := at(spath, "dimensions", strconv.Itoa(k))
			switch {
			case !sourceDims[d]:
				add(dpath, "rollup %s dimension set %s: dimension %s is not in source.dimensions", r.Name, set.Name, d)
			case dims[d]:
				add(dpath, "rollup %s dimension set %s: dimension %s is listed twice", r.Name, set.Name, d)
			}
			dims[d] = true
		}

		if len(set.Measures) == 0 {
			add(at(spath, "measures"), "rollup %s dimension set %s declares no measure", r.Name, set.Name)
		}
		for _, name := range set.MeasureNames() {
			checkMeasure(r, set, name, at(spath, "measures", name), dims, add)
		}
	}

	if r.Serve == nil {
		return
	}

	// Rule 14.
	svpath := at(path, "serve")
	if !serveNamePattern.MatchString(r.Serve.Catalog) {
		add(at(svpath, "catalog"), "rollup %s: serve.catalog %q must name the catalog serve.yml attaches the database as",
			r.Name, r.Serve.Catalog)
	}
	if r.Serve.MaxBuckets <= 0 {
		add(at(svpath, "max_buckets"), "rollup %s: serve.max_buckets is %d; it must be positive", r.Name, r.Serve.MaxBuckets)
	}
	for k, ds := range r.Serve.Datasets {
		checkRollupDataset(r, ds, at(svpath, "datasets", strconv.Itoa(k)), widths, datasets, add)
	}
}

func checkMeasure(r Rollup, set RollupDimensionSet, name string, path []string, keys map[string]bool, add rollupAdd) {
	m := set.Measures[name]
	switch {
	case !serveNamePattern.MatchString(name):
		add(path, "rollup %s dimension set %s: measure name %q %s", r.Name, set.Name, name, nameRule)
	case name == r.Source.TimeColumn || keys[name]:
		add(path, "rollup %s dimension set %s: measure %s has the name of a column the table is keyed on",
			r.Name, set.Name, name)
	}

	switch {
	case rollupReservedMeasureTypes[m.Type]:
		add(at(path, "type"), "rollup %s dimension set %s: measure %s has type %s, which this version does not generate; use sum, min, max or count_buckets",
			r.Name, set.Name, name, m.Type)
	case !rollupMeasureTypes[m.Type]:
		add(at(path, "type"), "rollup %s dimension set %s: measure %s has type %q; use sum, min, max or count_buckets",
			r.Name, set.Name, name, m.Type)
	case m.Type == "count_buckets" && m.Column != "":
		add(at(path, "column"), "rollup %s dimension set %s: measure %s counts source buckets and reads no column",
			r.Name, set.Name, name)
	case m.Type != "count_buckets" && !serveNamePattern.MatchString(m.Column):
		add(at(path, "column"), "rollup %s dimension set %s: measure %s needs column naming the source column it reads",
			r.Name, set.Name, name)
	}
}

func checkRollupDataset(r Rollup, ds RollupDataset, path []string, widths map[string]time.Duration, seen map[string]bool, add rollupAdd) {
	switch {
	case !serveNamePattern.MatchString(ds.Name):
		add(at(path, "name"), "rollup %s: dataset name %q %s", r.Name, ds.Name, nameRule)
	case seen[ds.Name]:
		add(at(path, "name"), "rollup %s: dataset %s is declared twice", r.Name, ds.Name)
	}
	seen[ds.Name] = true

	if ds.CacheTTLSeconds < 0 {
		add(at(path, "cache_ttl_seconds"), "rollup %s dataset %s: cache_ttl_seconds is %d; it must not be negative, and 0 means no cache",
			r.Name, ds.Name, ds.CacheTTLSeconds)
	}

	// Rule 7.
	set, ok := r.DimensionSet(ds.DimensionSet)
	if !ok {
		add(at(path, "dimension_set"), "rollup %s dataset %s: dimension_set %q is not declared", r.Name, ds.Name, ds.DimensionSet)
		return
	}
	if len(set.Dimensions) > 1 {
		add(at(path, "dimension_set"), "rollup %s dataset %s: dimension set %s has %d dimensions; a served dataset has at most one",
			r.Name, ds.Name, set.Name, len(set.Dimensions))
		return
	}

	// Rules 8, 9, 13 and 16.
	if len(set.Dimensions) == 1 {
		dim := set.Dimensions[0]
		switch {
		case ds.Fold == nil:
			add(path, "rollup %s dataset %s: dimension set %s has dimension %s, so the dataset needs a fold on it",
				r.Name, ds.Name, set.Name, dim)
		case ds.Fold.Dimension != dim:
			add(at(path, "fold", "dimension"), "rollup %s dataset %s: fold.dimension is %q; the dimension set's dimension is %s",
				r.Name, ds.Name, ds.Fold.Dimension, dim)
		}
		for _, name := range set.MeasureNames() {
			if set.Measures[name].Type == "count_buckets" {
				add(at(path, "dimension_set"), "rollup %s dataset %s: measure %s is count_buckets, which cannot be summed across %s values into other",
					r.Name, ds.Name, name, dim)
			}
		}
		if ds.Fold != nil {
			checkFold(r, ds, set, at(path, "fold"), add)
		}
	} else if ds.Fold != nil {
		add(at(path, "fold"), "rollup %s dataset %s: dimension set %s has no dimension to fold", r.Name, ds.Name, set.Name)
	}

	// Rule 15.
	for _, param := range slices.Sorted(maps.Keys(ds.Filters)) {
		fpath := at(path, "filters", param)
		switch {
		case !serveNamePattern.MatchString(param) || rollupReservedParams[param]:
			add(fpath, "rollup %s dataset %s: filter param %q %s, and not since, until, top or grain",
				r.Name, ds.Name, param, nameRule)
		case len(set.Dimensions) == 0 || ds.Filters[param] != set.Dimensions[0]:
			add(fpath, "rollup %s dataset %s: filter %s names %q, which is not the dimension set's dimension",
				r.Name, ds.Name, param, ds.Filters[param])
		}
	}

	// Rules 10, 11, 12 and 17.
	def, defErr := ParseServeDuration(ds.DefaultRange)
	if defErr != nil {
		add(at(path, "default_range"), "rollup %s dataset %s: default_range %v", r.Name, ds.Name, defErr)
	}
	if len(ds.MaxRange) == 0 {
		add(at(path, "max_range"), "rollup %s dataset %s: max_range serves no grain", r.Name, ds.Name)
		return
	}
	sourceServable := slices.Equal(slices.Sorted(slices.Values(set.Dimensions)), slices.Sorted(slices.Values(r.Source.Dimensions)))
	widest := time.Duration(0)
	byRange := map[time.Duration]string{}
	for _, g := range slices.Sorted(maps.Keys(ds.MaxRange)) {
		gpath := at(path, "max_range", g)
		width, known := widths[g]
		switch {
		case !known:
			add(gpath, "rollup %s dataset %s: max_range names grain %s, which is not declared", r.Name, ds.Name, g)
			continue
		case g == r.Source.Grain && !sourceServable:
			add(gpath, "rollup %s dataset %s: the source grain %s is served only for a dimension set with the source's dimensions",
				r.Name, ds.Name, g)
			continue
		}
		mr, err := ParseServeDuration(ds.MaxRange[g])
		if err != nil {
			add(gpath, "rollup %s dataset %s grain %s: max_range %v", r.Name, ds.Name, g, err)
			continue
		}
		if other, dup := byRange[mr]; dup {
			add(gpath, "rollup %s dataset %s: grains %s and %s both have max_range %s, so neither is the finer choice",
				r.Name, ds.Name, other, g, FormatServeDuration(mr))
			continue
		}
		byRange[mr] = g
		// A range filters on bucket starts, so it returns at most
		// ceil(range / width) buckets wherever it begins.
		if buckets := int64((mr + width - 1) / width); r.Serve.MaxBuckets > 0 && buckets > int64(r.Serve.MaxBuckets) {
			add(gpath, "rollup %s dataset %s grain %s: max_range %s is %d buckets, more than serve.max_buckets %d",
				r.Name, ds.Name, g, ds.MaxRange[g], buckets, r.Serve.MaxBuckets)
		}
		if mr > widest {
			widest = mr
		}
	}
	if defErr == nil && widest > 0 && def > widest {
		add(at(path, "default_range"), "rollup %s dataset %s: default_range %s is wider than the widest max_range %s",
			r.Name, ds.Name, FormatServeDuration(def), FormatServeDuration(widest))
	}
}

func checkFold(r Rollup, ds RollupDataset, set RollupDimensionSet, path []string, add rollupAdd) {
	top := ds.Fold.Top
	switch {
	case top.Max < 1:
		add(at(path, "top", "max"), "rollup %s dataset %s: fold.top.max is %d; it must be at least 1", r.Name, ds.Name, top.Max)
	case top.Default < 1 || top.Default > top.Max:
		add(at(path, "top", "default"), "rollup %s dataset %s: fold.top.default %d must lie within 1 and top.max %d",
			r.Name, ds.Name, top.Default, top.Max)
	}

	var sums []string
	for _, name := range set.MeasureNames() {
		if set.Measures[name].Type == "sum" {
			sums = append(sums, name)
		}
	}
	switch {
	case ds.Fold.RankBy == "" && len(sums) != 1:
		add(at(path, "rank_by"), "rollup %s dataset %s: fold.rank_by is required when the dimension set has %d sum measures",
			r.Name, ds.Name, len(sums))
	case ds.Fold.RankBy != "" && !slices.Contains(sums, ds.Fold.RankBy):
		add(at(path, "rank_by"), "rollup %s dataset %s: fold.rank_by %q is not a sum measure of dimension set %s",
			r.Name, ds.Name, ds.Fold.RankBy, set.Name)
	}
}
