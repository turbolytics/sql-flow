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
