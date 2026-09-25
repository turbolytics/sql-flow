package rollup

import (
	"slices"

	"github.com/turbolytics/sql-flow/internal/config"
)

// Applied is the part of a rollup its tables are built from: the source, the
// grains, and the dimension sets. The state table stores it as JSON, and
// every later version reads it back, so its field names are a contract.
//
// The serve block is left out. It changes what serve reads, never what a
// table holds.
type Applied struct {
	Source AppliedSource `json:"source"`
	// Each grain and the grain it is built from.
	Grains        map[string]string     `json:"grains"`
	DimensionSets map[string]AppliedSet `json:"dimension_sets"`
}

// AppliedSource is the source as applied.
type AppliedSource struct {
	Table      string   `json:"table"`
	TimeColumn string   `json:"time_column"`
	Grain      string   `json:"grain"`
	Dimensions []string `json:"dimensions"`
}

// AppliedSet is one dimension set as applied.
type AppliedSet struct {
	Dimensions []string                  `json:"dimensions"`
	Measures   map[string]AppliedMeasure `json:"measures"`
}

// AppliedMeasure is one measure as applied.
type AppliedMeasure struct {
	Type    string `json:"type"`
	Column  string `json:"column,omitempty"`
	Numeric string `json:"numeric,omitempty"`
}

// AppliedFrom normalizes r, so an edit that changes no stored row compares
// equal: key order, dimension order, and a sum's numeric written out as its
// default, integer. A reorder changes neither a key nor a value, since the
// upserts name their columns and ON CONFLICT matches an index by its set of
// columns.
func AppliedFrom(r config.Rollup) Applied {
	out := Applied{
		Source: AppliedSource{
			Table: r.Source.Table, TimeColumn: r.Source.TimeColumn, Grain: r.Source.Grain,
			Dimensions: sortedCopy(r.Source.Dimensions),
		},
		Grains:        map[string]string{},
		DimensionSets: map[string]AppliedSet{},
	}
	for name, g := range r.Grains {
		out.Grains[name] = g.From
	}
	for _, set := range r.DimensionSets {
		measures := map[string]AppliedMeasure{}
		for name, m := range set.Measures {
			numeric := m.Numeric
			if m.Type == "sum" && numeric == "" {
				numeric = "integer"
			}
			measures[name] = AppliedMeasure{Type: m.Type, Column: m.Column, Numeric: numeric}
		}
		out.DimensionSets[set.Name] = AppliedSet{Dimensions: sortedCopy(set.Dimensions), Measures: measures}
	}
	return out
}

// RetainedTable is a table a later declaration removed, and the shape that
// built it: its dimension set, its grain and the grain it was built from.
// Declaring it again is checked against the shape, because its stored rows
// were merged that way.
type RetainedTable struct {
	Table string     `json:"table"`
	Set   string     `json:"set"`
	Grain string     `json:"grain"`
	From  string     `json:"from"`
	Shape AppliedSet `json:"shape"`
}

// sortedCopy is never nil, so an empty list stores as [] and not null.
func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	slices.Sort(out)
	return out
}
