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
// apply to a pipeline, which no constructor produces, so no integration of
// that kind is constructed and asking for it is a mistake.
var constructedKinds = map[string]bool{"sink": true, "source": true, "handler": true}

// Integration is one entry of docs/coverage/integrations, whole.
//
// One type rather than a partial struct per accessor. Seven functions each
// declared their own, so a field reached one caller and not another, and the
// registry could only be read one question at a time.
type Integration struct {
	ID   string `yaml:"id"`
	Kind string `yaml:"kind"`

	// Feature is the features.yml id this attributes to. An integration id is
	// not a feature id: sink.noop attributes to sink.console, and a test_only
	// integration attributes to nothing.
	Feature string `yaml:"feature"`

	// TestOnly marks an id that ships to nobody. The conformance harness's
	// doubles must name an integration, because a marker carries one, and
	// naming a real sink would credit that sink for what a double did.
	TestOnly bool `yaml:"test_only"`

	// Constructed is false for an entry no constructor switch builds. Pipeline
	// configurations are the only ones, and Integrations() must not report
	// them: the Kinds() agreement tests are held equal to that list.
	Constructed *bool `yaml:"constructed"`

	Exempt []struct {
		Invariant string `yaml:"invariant"`
	} `yaml:"exempt"`

	Nulls map[string]NullRule `yaml:"nulls"`
	Types map[string]TypeDecl `yaml:"types"`
}

// loadIntegrations reads every file in docs/coverage/integrations, in filename
// order.
//
// Filename order is id order, because the filename is the id. The generator
// holds the two equal, so a file that disagrees fails `make coverage-check`
// rather than being silently reachable under the wrong name.
func loadIntegrations() ([]Integration, error) {
	dir, err := registryPath("integrations")
	if err != nil {
		return nil, err
	}

	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("coverage: read %s: %w", dir, err)
	}

	var out []Integration
	for _, entry := range names {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yml") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("coverage: read %s: %w", path, err)
		}
		var integration Integration
		if err := yaml.Unmarshal(raw, &integration); err != nil {
			return nil, fmt.Errorf("coverage: parse %s: %w", path, err)
		}
		out = append(out, integration)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// integration returns one entry by id.
func integration(id string) (Integration, error) {
	all, err := loadIntegrations()
	if err != nil {
		return Integration{}, err
	}
	for _, entry := range all {
		if entry.ID == id {
			return entry, nil
		}
	}
	return Integration{}, fmt.Errorf("coverage: integrations/ declares no %q", id)
}

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

// IsTestOnly reports whether the registry marks an integration as existing
// only for tests.
//
// The conformance harness's doubles must name an integration, because a
// marker carries one. Naming a real sink would credit that sink for what a
// double did. A test-only id gets no cells, so a double's marker lands
// nowhere.
func IsTestOnly(id string) (bool, error) {
	entry, err := integration(id)
	if err != nil {
		return false, err
	}
	return entry.TestOnly, nil
}

// FeatureFor returns the features.yml id an integration attributes to, and
// whether it has one.
//
// An integration id is not a feature id. sink.noop attributes to
// sink.console, and a test_only integration attributes to nothing at all.
// Emitting the integration id as a feature marker put
// "marks sink.conformance_double" into the feature matrix's unknown list.
func FeatureFor(id string) (string, bool, error) {
	entry, err := integration(id)
	if err != nil {
		return "", false, err
	}
	return entry.Feature, entry.Feature != "", nil
}

// Exemptions returns the invariants the registry excuses one integration
// from, as a set.
func Exemptions(id string) (map[string]bool, error) {
	entry, err := integration(id)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(entry.Exempt))
	for _, ex := range entry.Exempt {
		out[ex.Invariant] = true
	}
	return out, nil
}

// registryPath locates one entry of docs/coverage, file or directory.
//
// Relative to this source file, not the working directory: `go test ./...`
// runs each package from its own directory.
func registryPath(name string) (string, error) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("coverage: cannot locate %s", name)
	}
	return filepath.Join(filepath.Dir(self), "..", "..", "docs", "coverage", name), nil
}

// readRegistry reads one file from docs/coverage.
func readRegistry(name string) ([]byte, error) {
	path, err := registryPath(name)
	if err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("coverage: read %s: %w", path, err)
	}
	return raw, nil
}

// Integrations returns the bare type names the registry declares for one
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

	all, err := loadIntegrations()
	if err != nil {
		return nil, err
	}

	out := []string{}
	for _, entry := range all {
		// Neither a test-only integration nor an unconstructed one has a case
		// in a constructor switch, so neither may appear in the list Kinds()
		// is held equal to.
		if entry.TestOnly {
			continue
		}
		if entry.Constructed != nil && !*entry.Constructed {
			continue
		}
		if entry.Kind == kind {
			out = append(out, strings.TrimPrefix(entry.ID, kind+"."))
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

	// Columns are the destination column types that accept this key, each
	// carrying what to write and what must come back. Every one is exercised,
	// and the cell is covered only when all of them pass.
	//
	// The pair is the unit rather than the key. One Arrow key reaches several
	// destinations that demand different content -- DuckDB's VARCHAR is the
	// universal text carrier, and ClickHouse reparses text into UUID, Decimal
	// and Enum -- so the value belongs here.
	Columns []ColumnDecl `yaml:"columns"`
}

// ColumnDecl is one destination column type, and what the integration claims
// about writing this Arrow key into it.
type ColumnDecl struct {
	// Type is the destination column type, as its DDL spells it.
	Type string `yaml:"type"`

	// Value overrides the key's canonical value. Empty means the canonical
	// one. Only the utf8 row may set it: text is the only thing a destination
	// reparses, and a value declared for any other key would be ignored.
	Value string `yaml:"value"`

	// Expect is what the destination must hold afterwards, as ReadBack renders
	// it. Declared rather than recorded from a run: a table copied from the
	// sink compares the sink to itself.
	Expect string `yaml:"expect"`

	// Instant marks a pair the timestamp claim must exercise with the host
	// clock moved off UTC. It is how a text value bound for a temporal column
	// -- #153's shape, which no Arrow key describes -- reaches that verdict.
	Instant bool `yaml:"instant"`
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
func NullsFor(id string) (NullRule, error) {
	entry, err := integration(id)
	if err != nil {
		return NullRule{}, err
	}
	return entry.Nulls["default"], nil
}

// NullElementsFor returns an integration's rule for a null held inside a
// non-null list.
//
// Separate from NullsFor because the answers differ: a ClickHouse column can
// be Nullable and its Array(T) elements cannot, so the column-level rule says
// nothing about what a list holds.
func NullElementsFor(id string) (NullRule, error) {
	entry, err := integration(id)
	if err != nil {
		return NullRule{}, err
	}
	return entry.Nulls["list_element"], nil
}

// TypesFor returns one integration's type table, sorted by key.
//
// Sorted rather than in file order: a YAML mapping carries no order, so file
// order is whatever the parser chose, and a runner iterating it would report
// its rows in a different sequence run to run.
func TypesFor(id string) ([]TypeDecl, error) {
	entry, err := integration(id)
	if err != nil {
		return nil, err
	}
	out := make([]TypeDecl, 0, len(entry.Types))
	for key, decl := range entry.Types {
		decl.Key = key
		out = append(out, decl)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
