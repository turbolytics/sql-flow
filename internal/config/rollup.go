package config

import (
	"maps"
	"slices"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

// RollupsConf is a whole rollups file. `sqlflow rollup` generates, from each
// rollup, a migration that creates its tables and the triggers that keep them
// current, and the serve datasets that read them.
type RollupsConf struct {
	// The rollups this file declares.
	Rollups []Rollup `yaml:"rollups"`
}

// Rollup is one source table and the coarser tables kept from it.
type Rollup struct {
	// Names the rollup in messages. Lowercase letters, digits and
	// underscores, starting with a letter.
	Name string `yaml:"name"`
	// The table the pipeline writes at the finest grain.
	Source RollupSource `yaml:"source"`
	// Each coarser grain, keyed by its width such as 5m, 1h or 1d, and the
	// grain it is built from.
	Grains map[string]RollupGrain `yaml:"grains"`
	// The dimensions and measures kept at every grain. Each set gets one
	// table per grain, named <set>_<grain>.
	DimensionSets []RollupDimensionSet `yaml:"dimension_sets"`
	// The serve datasets to generate. Omit to generate tables only.
	Serve *RollupServe `yaml:"serve,omitempty"`
}

// RollupSource is the finest grain, which the pipeline writes.
type RollupSource struct {
	// The source table. The generator never creates it.
	Table string `yaml:"table"`
	// The timestamptz column holding each row's bucket.
	TimeColumn string `yaml:"time_column"`
	// The source's bucket width, such as 1m.
	Grain string `yaml:"grain"`
	// The source's key besides time. A dimension set with exactly these
	// dimensions can serve the source grain directly.
	Dimensions []string `yaml:"dimensions,omitempty"`
}

// RollupGrain is one coarser grain.
type RollupGrain struct {
	// The grain this one is re-merged from: the source grain or a narrower
	// declared grain whose width divides this one's.
	From string `yaml:"from"`
}

// RollupDimensionSet is one table per grain: a time bucket, some dimensions,
// and the measures kept for each.
type RollupDimensionSet struct {
	// Names the set and prefixes its tables.
	Name string `yaml:"name"`
	// Source dimensions the set keeps. Empty keeps one row per bucket.
	Dimensions []string `yaml:"dimensions"`
	// Measures keyed by the column name they get in every table of the set.
	Measures map[string]RollupMeasure `yaml:"measures"`
}

// RollupMeasure is one kept value and how it merges.
type RollupMeasure struct {
	// sum, min, max or count_buckets. avg, gauge and histogram are reserved
	// and refused until a later version generates them.
	Type string `yaml:"type" jsonschema:"enum=sum,enum=min,enum=max,enum=count_buckets,enum=avg,enum=gauge,enum=histogram"`
	// The source column the measure reads. Required for sum, min and max;
	// refused for count_buckets, which counts source buckets.
	Column string `yaml:"column,omitempty"`
}

// RollupServe declares the serve datasets generated from a rollup.
type RollupServe struct {
	// The catalog serve.yml's commands attach the database as, such as pg.
	Catalog string `yaml:"catalog"`
	// The most buckets one response may carry at any grain. Each grain's
	// max_range divided by its width must not exceed it.
	MaxBuckets int `yaml:"max_buckets"`
	// The datasets to generate.
	Datasets []RollupDataset `yaml:"datasets"`
}

// RollupDataset is one generated serve dataset.
type RollupDataset struct {
	// The serve dataset's name.
	Name string `yaml:"name"`
	// The dimension set the dataset reads. At most one dimension.
	DimensionSet string `yaml:"dimension_set"`
	// Shown in /v1/datasets.
	Description string `yaml:"description,omitempty"`
	// The width a request gets without since, such as 24h.
	DefaultRange string `yaml:"default_range"`
	// The grains served, each with the widest range it serves.
	MaxRange map[string]string `yaml:"max_range"`
	// Keeps the top values of the dimension as their own series and sums the
	// rest into "other". Required when the set has a dimension.
	Fold *RollupFold `yaml:"fold,omitempty"`
	// Params that filter the dimension to one value, never folded, keyed by
	// param name.
	Filters map[string]string `yaml:"filters,omitempty"`
	// Opts the generated dataset into serve's response cache, for this many
	// seconds. 0 leaves it out. The generated SQL reads the range half-open,
	// which is what the cache's key needs.
	CacheTTLSeconds int `yaml:"cache_ttl_seconds,omitempty" jsonschema:"minimum=0"`
	// Grains that hold their answers for their own number of seconds, such
	// as {1d: 600}: a year of days changes by one open bucket, and re-running
	// the widest query every cache_ttl_seconds buys nothing a chart can show.
	// Needs cache_ttl_seconds, which the other grains take.
	CacheTTLByGrain map[string]int `yaml:"cache_ttl_by_grain,omitempty"`
}

// RollupFold bounds a dataset's series.
type RollupFold struct {
	// The dimension folded. It must be the dimension set's dimension.
	Dimension string `yaml:"dimension"`
	// The sum measure that ranks the dimension's values. Optional when the
	// set has exactly one sum measure.
	RankBy string `yaml:"rank_by,omitempty"`
	// How many values keep their own series.
	Top RollupTop `yaml:"top"`
}

// RollupTop is the generated top param's default and bound.
type RollupTop struct {
	// The series a request gets without top.
	Default int64 `yaml:"default"`
	// The most series a request may ask for. A larger top is refused.
	Max int64 `yaml:"max"`
}

// RollupLevel is one declared grain and its width.
type RollupLevel struct {
	// The grain as declared, such as 15m.
	Name string
	// The bucket width the name parses to.
	Width time.Duration
	// The grain this one is built from.
	From string
}

// IsRollups reports whether rendered text is a rollups file: a top-level
// rollups key, and neither a serve nor a pipeline key.
func IsRollups(rendered []byte) bool {
	var top map[string]any
	if err := yaml.Unmarshal(rendered, &top); err != nil {
		return false
	}
	_, rollups := top["rollups"]
	_, serve := top["serve"]
	_, pipeline := top["pipeline"]
	return rollups && !serve && !pipeline
}

// ParseRollups decodes rendered rollups text strictly, as ParseServe does.
func ParseRollups(rendered []byte) (*RollupsConf, error) {
	var conf RollupsConf
	if err := decodeStrict(rendered, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// LoadRollups renders a rollups file and decodes it strictly.
func LoadRollups(path string) (*RollupsConf, error) {
	rendered, err := RenderTemplate(path, nil)
	if err != nil {
		return nil, err
	}
	return ParseRollups(rendered)
}

// Ladder returns the declared grains from the narrowest to the widest, ties
// by name. A grain whose name does not parse, or that repeats the source
// grain, is left out; Check reports both.
func (r Rollup) Ladder() []RollupLevel {
	var out []RollupLevel
	for name, g := range r.Grains {
		w, err := ParseServeDuration(name)
		if err != nil || name == r.Source.Grain {
			continue
		}
		out = append(out, RollupLevel{Name: name, Width: w, From: g.From})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Width != out[j].Width {
			return out[i].Width < out[j].Width
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// DimensionSet returns the set with the given name.
func (r Rollup) DimensionSet(name string) (RollupDimensionSet, bool) {
	for _, s := range r.DimensionSets {
		if s.Name == name {
			return s, true
		}
	}
	return RollupDimensionSet{}, false
}

// MeasureNames returns the measure names, sorted, so generated columns come
// out in the same order on every run.
func (s RollupDimensionSet) MeasureNames() []string {
	return slices.Sorted(maps.Keys(s.Measures))
}
