package rollup

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Check holds a rollups file, a migration committed from it, and a serve file
// to each other. It returns the file's rule violations alone when there are
// any, since a file that breaks a rule generates nothing to compare.
// Otherwise it returns every drift and every row bound the serve file breaks.
func Check(conf *config.RollupsConf, migration []byte, serve *config.ServeConf) []config.Violation {
	if violations := conf.Check(); len(violations) > 0 {
		return violations
	}

	var out []config.Violation
	drift := func(path []string, format string, args ...any) {
		out = append(out, config.Violation{Code: errs.CodeConfigRollupDrift, Path: path, Message: fmt.Sprintf(format, args...)})
	}

	script, err := PostgresDDL(conf)
	if err != nil {
		return []config.Violation{{Code: errs.CodeOf(err), Path: []string{"rollups"}, Message: err.Error()}}
	}
	if script != string(migration) {
		drift([]string{"migration"}, "the migration differs from `sqlflow rollup ddl` from line %d; regenerate it rather than editing it",
			firstDifferentLine(script, string(migration)))
	}

	generated, err := ServeDatasets(conf)
	if err != nil {
		return []config.Violation{{Code: errs.CodeOf(err), Path: []string{"rollups"}, Message: err.Error()}}
	}
	index := map[string]int{}
	for i, ds := range serve.Serve.Datasets {
		index[ds.Name] = i
	}
	bounds := datasetBounds(conf)

	for _, want := range generated {
		i, ok := index[want.Name]
		if !ok {
			drift([]string{"serve", "datasets"}, "dataset %s is not in the serve file; add the output of `sqlflow rollup serve`", want.Name)
			continue
		}
		got := serve.Serve.Datasets[i]
		path := []string{"serve", "datasets", strconv.Itoa(i)}
		compareDataset(want, got, path, drift)

		b := bounds[want.Name]
		rows := int64(b.buckets) * b.series
		if limit := serve.Serve.MaxRows(got); rows > int64(limit) {
			out = append(out, config.Violation{Code: errs.CodeConfigRollup, Path: path, Message: fmt.Sprintf(
				"dataset %s can answer %d rows, %d buckets × %d series, more than its max_rows %d; lower max_buckets or fold.top.max, or raise max_rows",
				want.Name, rows, b.buckets, b.series, limit)})
		}
	}
	return out
}

type bound struct {
	buckets int
	series  int64
}

// datasetBounds is each generated dataset's worst case: max_buckets buckets,
// each carrying top.max values and other, or one series without a dimension.
func datasetBounds(conf *config.RollupsConf) map[string]bound {
	out := map[string]bound{}
	for _, r := range conf.Rollups {
		if r.Serve == nil {
			continue
		}
		for _, ds := range r.Serve.Datasets {
			b := bound{buckets: r.Serve.MaxBuckets, series: 1}
			if ds.Fold != nil {
				b.series = ds.Fold.Top.Max + 1
			}
			out[ds.Name] = b
		}
	}
	return out
}

func compareDataset(want, got config.ServeDataset, path []string, drift func([]string, string, ...any)) {
	child := func(keys ...string) []string { return slices.Concat(path, keys) }

	if describeParams(want.Params) != describeParams(got.Params) {
		drift(child("params"), "dataset %s: params are %s; the declaration generates %s",
			want.Name, describeParams(got.Params), describeParams(want.Params))
	}
	if !reflect.DeepEqual(want.Range, got.Range) {
		drift(child("range"), "dataset %s: range differs from the generated range %+v", want.Name, *want.Range)
	}

	if !reflect.DeepEqual(want.Cache, got.Cache) {
		drift(child("cache"), "dataset %s: the cache block differs from what the declaration's cache_ttl_seconds generates", want.Name)
	}

	wantGrains, gotGrains := want.GrainNames(), got.GrainNames()
	if !slices.Equal(wantGrains, gotGrains) {
		drift(child("grains"), "dataset %s: grains are %s; the declaration generates %s",
			want.Name, strings.Join(gotGrains, ", "), strings.Join(wantGrains, ", "))
		return
	}
	for _, g := range wantGrains {
		if want.Grains[g].Bucket != got.Grains[g].Bucket {
			drift(child("grains", g, "bucket"), "dataset %s grain %s: bucket is %q; the declaration generates %s",
				want.Name, g, got.Grains[g].Bucket, want.Grains[g].Bucket)
		}
		if !reflect.DeepEqual(want.Grains[g].Cache, got.Grains[g].Cache) {
			drift(child("grains", g, "cache"), "dataset %s grain %s: the cache block differs from what the declaration's cache_ttl_by_grain generates",
				want.Name, g)
		}
		if want.Grains[g].MaxRange != got.Grains[g].MaxRange {
			drift(child("grains", g, "max_range"), "dataset %s grain %s: max_range is %s; the declaration generates %s",
				want.Name, g, got.Grains[g].MaxRange, want.Grains[g].MaxRange)
		}
		if collapse(want.Grains[g].SQL) != collapse(got.Grains[g].SQL) {
			drift(child("grains", g, "sql"), "dataset %s grain %s: the SQL differs from what `sqlflow rollup serve` generates",
				want.Name, g)
		}
	}
}

// describeParams writes params in name order, so two lists compare equal
// whatever order a hand-edited file puts them in.
func describeParams(params []config.ServeParam) string {
	parts := make([]string, 0, len(params))
	for _, p := range params {
		s := p.Name + " " + p.Type
		if p.Min != nil {
			s += " min " + strconv.FormatInt(*p.Min, 10)
		}
		if p.Max != nil {
			s += " max " + strconv.FormatInt(*p.Max, 10)
		}
		parts = append(parts, s)
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

// collapse makes whitespace insignificant, so reindenting SQL is not drift.
func collapse(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

func firstDifferentLine(a, b string) int {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return i + 1
		}
	}
	return min(len(al), len(bl)) + 1
}
