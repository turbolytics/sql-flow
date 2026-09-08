package coverage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// constructedKinds are the kinds something builds. An invariant may also
// apply to a pipeline, which no constructor produces, so integrations.yml
// never carries that kind and asking for it is a mistake.
var constructedKinds = map[string]bool{"sink": true, "source": true, "handler": true}

// Invariants returns every id invariants.yml declares, as a set.
//
// The conformance harness judges invariants by id, and an id nothing declares
// reports as an unknown marker rather than as a cell -- the claim would be
// tested and invisible. A test in the harness holds every id it judges to
// this set.
func Invariants() (map[string]bool, error) {
	raw, err := readRegistry("invariants.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Invariants []struct {
			ID string `yaml:"id"`
		} `yaml:"invariants"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse invariants.yml: %w", err)
	}

	out := make(map[string]bool, len(doc.Invariants))
	for _, invariant := range doc.Invariants {
		out[invariant.ID] = true
	}
	return out, nil
}

// IsTestOnly reports whether integrations.yml marks an integration as
// existing only for tests.
//
// The conformance harness's doubles must name an integration, because a
// marker carries one. Naming a real sink would credit that sink for what a
// double did. A test-only id gets no cells, so a double's marker lands
// nowhere.
func IsTestOnly(integration string) (bool, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return false, err
	}

	var doc struct {
		Integrations []struct {
			ID       string `yaml:"id"`
			TestOnly bool   `yaml:"test_only"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return false, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID == integration {
			return entry.TestOnly, nil
		}
	}
	return false, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}

// FeatureFor returns the features.yml id an integration attributes to, and
// whether it has one.
//
// An integration id is not a feature id. sink.noop attributes to
// sink.console, and a test_only integration attributes to nothing at all.
// Emitting the integration id as a feature marker put
// "marks sink.conformance_double" into the feature matrix's unknown list.
func FeatureFor(integration string) (string, bool, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return "", false, err
	}

	var doc struct {
		Integrations []struct {
			ID      string `yaml:"id"`
			Feature string `yaml:"feature"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", false, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID == integration {
			return entry.Feature, entry.Feature != "", nil
		}
	}
	return "", false, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}

// Exemptions returns the invariants integrations.yml excuses one integration
// from, as a set.
func Exemptions(integration string) (map[string]bool, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Integrations []struct {
			ID     string `yaml:"id"`
			Exempt []struct {
				Invariant string `yaml:"invariant"`
			} `yaml:"exempt"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID != integration {
			continue
		}
		out := make(map[string]bool, len(entry.Exempt))
		for _, ex := range entry.Exempt {
			out[ex.Invariant] = true
		}
		return out, nil
	}
	return nil, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}

// readRegistry reads one file from docs/coverage.
//
// Located relative to this source file, not the working directory: `go test
// ./...` runs each package from its own directory.
func readRegistry(name string) ([]byte, error) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("coverage: cannot locate %s", name)
	}
	path := filepath.Join(filepath.Dir(self), "..", "..", "docs", "coverage", name)

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("coverage: read %s: %w", path, err)
	}
	return raw, nil
}

// Integrations returns the bare type names integrations.yml declares for one
// kind, sorted.
//
// "sink.clickhouse" is reported as "clickhouse", which is the form the
// constructor switches use, so a test in each of those packages can hold
// Kinds() equal to this. An integration the engine can build and the registry
// does not name has no invariant cells at all, which is the sink.iceberg
// failure: nothing written down, so nothing can be missing.
func Integrations(kind string) ([]string, error) {
	if !constructedKinds[kind] {
		return nil, fmt.Errorf("coverage: nothing constructs a %q", kind)
	}

	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Integrations []struct {
			ID          string `yaml:"id"`
			Kind        string `yaml:"kind"`
			TestOnly    bool   `yaml:"test_only"`
			Constructed *bool  `yaml:"constructed"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	out := []string{}
	for _, integration := range doc.Integrations {
		// Neither a test-only integration nor an unconstructed one has a case
		// in a constructor switch, so neither may appear in the list Kinds()
		// is held equal to.
		if integration.TestOnly {
			continue
		}
		if integration.Constructed != nil && !*integration.Constructed {
			continue
		}
		if integration.Kind == kind {
			out = append(out, strings.TrimPrefix(integration.ID, kind+"."))
		}
	}
	sort.Strings(out)
	return out, nil
}

// LatticeEntry is one Arrow type every sink must account for.
type LatticeEntry struct {
	// Key is conformance.CanonicalKey's output for the type, not
	// DataType.String(): those differ, and only the canonical form matches
	// both what DuckDB emits and what a test constructs.
	Key string `yaml:"key"`

	// DuckDB names the SQL types a user casts to in handler SQL. The rendered
	// integration page is keyed by these, because a user writes a CAST and
	// never sees an Arrow type.
	DuckDB []string `yaml:"duckdb"`

	// Depth is 1 for a scalar, 2 for a container.
	Depth int `yaml:"depth"`
}

// Lattice returns every Arrow type lattice.yml declares, in file order.
//
// The set is closed, which is what makes a gap visible: a type an integration
// does not declare is reported rather than absent. An open set lets a type
// nobody thought of pass unnoticed, which is the sink.iceberg failure --
// nothing written down, so nothing could be missing.
func Lattice() ([]LatticeEntry, error) {
	raw, err := readRegistry("lattice.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Lattice []LatticeEntry `yaml:"lattice"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse lattice.yml: %w", err)
	}
	return doc.Lattice, nil
}

// TypeDecl is one integration's declared outcome for one Arrow type.
type TypeDecl struct {
	// Key is the lattice.yml key. It is the mapping key in the YAML, so
	// TypesFor fills it in rather than the parser.
	Key string `yaml:"-"`

	// Outcome is exact, coerced or unsupported.
	Outcome string `yaml:"outcome"`

	// Rule states a coercion in prose, for the rendered page. Required when
	// Outcome is coerced: a coercion with no rule is an excuse, and a reader
	// cannot predict what their column will hold.
	Rule string `yaml:"rule"`

	// Code is the errs code an unsupported type must fail with.
	Code string `yaml:"code"`

	// Columns are the destination column types that accept this key. Every one
	// is exercised, and the cell is covered only when all of them pass.
	Columns []string `yaml:"columns"`
}

// NullRule is what an integration does with a null, when the destination
// column does not say otherwise.
//
// It is separate from TypeDecl because it is one statement about every type
// rather than one per type: ClickHouse stores a null in a non-Nullable column
// as the column type's zero value whatever that type is.
type NullRule struct {
	Outcome string `yaml:"outcome"`
	Rule    string `yaml:"rule"`
}

// NullsFor returns an integration's default null rule.
func NullsFor(integration string) (NullRule, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return NullRule{}, err
	}

	var doc struct {
		Integrations []struct {
			ID    string              `yaml:"id"`
			Nulls map[string]NullRule `yaml:"nulls"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return NullRule{}, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID != integration {
			continue
		}
		return entry.Nulls["default"], nil
	}
	return NullRule{}, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}

// TypesFor returns one integration's type table, sorted by key.
//
// Sorted rather than in file order: a YAML mapping carries no order, so file
// order is whatever the parser chose, and a runner iterating it would report
// its rows in a different sequence run to run.
func TypesFor(integration string) ([]TypeDecl, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Integrations []struct {
			ID    string              `yaml:"id"`
			Types map[string]TypeDecl `yaml:"types"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID != integration {
			continue
		}
		out := make([]TypeDecl, 0, len(entry.Types))
		for key, decl := range entry.Types {
			decl.Key = key
			out = append(out, decl)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out, nil
	}
	return nil, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}
