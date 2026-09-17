package rollup

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

// ServeDatasets builds the serve datasets a rollups file declares, in file
// order. Each reads its grain's rollup table within the range serve resolves,
// folds its dimension to the top values, and declares the bounds that keep
// every response under max_rows.
func ServeDatasets(conf *config.RollupsConf) ([]config.ServeDataset, error) {
	if err := conf.CheckError(); err != nil {
		return nil, err
	}
	var out []config.ServeDataset
	for _, r := range conf.Rollups {
		if r.Serve == nil {
			continue
		}
		for _, ds := range r.Serve.Datasets {
			out = append(out, serveDataset(r, ds))
		}
	}
	return out, nil
}

// ServeYAML renders datasets as the items of a serve file's datasets list.
func ServeYAML(datasets []config.ServeDataset) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(datasets); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func serveDataset(r config.Rollup, ds config.RollupDataset) config.ServeDataset {
	set, _ := r.DimensionSet(ds.DimensionSet)

	params := []config.ServeParam{{Name: "since", Type: "timestamp"}, {Name: "until", Type: "timestamp"}}
	for _, p := range slices.Sorted(maps.Keys(ds.Filters)) {
		params = append(params, config.ServeParam{Name: p, Type: "string"})
	}
	if ds.Fold != nil {
		min, max := int64(1), ds.Fold.Top.Max
		params = append(params, config.ServeParam{Name: "top", Type: "integer", Min: &min, Max: &max})
	}

	grains := map[string]config.ServeGrain{}
	for g, maxRange := range ds.MaxRange {
		// A rollup grain is named for its width, so the name is the bucket.
		grains[g] = config.ServeGrain{Bucket: g, MaxRange: maxRange, SQL: grainSQL(r, set, ds, g)}
	}

	var cache *config.ServeDatasetCache
	if ds.CacheTTLSeconds > 0 {
		cache = &config.ServeDatasetCache{TTLSeconds: ds.CacheTTLSeconds}
	}

	return config.ServeDataset{
		Name:        ds.Name,
		Description: ds.Description,
		Params:      params,
		Cache:       cache,
		Range:       &config.ServeRange{Since: "since", Until: "until", Default: ds.DefaultRange},
		Grains:      grains,
	}
}

// grainSQL reads one grain. #282 binds since and until already resolved, and
// serve refuses a top outside its bounds, so the SQL defaults and clamps
// nothing.
func grainSQL(r config.Rollup, set config.RollupDimensionSet, ds config.RollupDataset, grain string) string {
	t := r.Source.TimeColumn
	source := grain == r.Source.Grain
	from := r.Serve.Catalog + "." + Table(set, grain)
	if source {
		from = r.Serve.Catalog + "." + r.Source.Table
	}

	// The innermost SELECT names each measure. At the source grain a measure
	// is its source column, which may be named differently.
	measures := set.MeasureNames()
	inner := make([]string, len(measures))
	for i, name := range measures {
		m := set.Measures[name]
		switch {
		case !source:
			inner[i] = name
		case m.Type == "count_buckets":
			inner[i] = "1::BIGINT AS " + name
		case m.Column == name:
			inner[i] = name
		default:
			inner[i] = m.Column + " AS " + name
		}
	}

	if len(set.Dimensions) == 0 {
		return fmt.Sprintf("SELECT %s, %s\nFROM %s\nWHERE %s >= $since AND %s < $until\nORDER BY %s\n",
			t, strings.Join(inner, ", "), from, t, t, t)
	}

	dim := set.Dimensions[0]
	rank := ds.Fold.RankBy
	if rank == "" {
		for _, name := range measures {
			if set.Measures[name].Type == "sum" {
				rank = name
			}
		}
	}
	outer := make([]string, len(measures))
	for i, name := range measures {
		switch set.Measures[name].Type {
		case "sum":
			outer[i] = "sum(" + name + ")::BIGINT AS " + name
		case "min":
			outer[i] = "min(" + name + ") AS " + name
		case "max":
			outer[i] = "max(" + name + ") AS " + name
		}
	}
	filter := ""
	for _, p := range slices.Sorted(maps.Keys(ds.Filters)) {
		filter += fmt.Sprintf("\n      AND %s = coalesce($%s, %s)", dim, p, dim)
	}

	return fmt.Sprintf(`SELECT %[1]s,
       CASE WHEN %[2]s_rank <= coalesce($top, %[3]d) THEN %[2]s ELSE 'other' END AS %[2]s,
       %[4]s
FROM (
  SELECT %[1]s, %[2]s, %[5]s,
         dense_rank() OVER (ORDER BY %[2]s_total DESC, %[2]s) AS %[2]s_rank
  FROM (
    SELECT %[1]s, %[2]s, %[6]s, sum(%[7]s) OVER (PARTITION BY %[2]s) AS %[2]s_total
    FROM %[8]s
    WHERE %[1]s >= $since AND %[1]s < $until%[9]s
  )
)
GROUP BY ALL
ORDER BY %[1]s, %[2]s
`, t, dim, ds.Fold.Top.Default, strings.Join(outer, ",\n       "), strings.Join(measures, ", "),
		strings.Join(inner, ", "), rank, from, filter)
}
