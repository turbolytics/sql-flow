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

	// Located relative to this file, not the working directory: `go test ./...`
	// runs each package from its own directory.
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("coverage: cannot locate the registry")
	}
	path := filepath.Join(filepath.Dir(self), "..", "..",
		"docs", "coverage", "integrations.yml")

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("coverage: read %s: %w", path, err)
	}

	var doc struct {
		Integrations []struct {
			ID   string `yaml:"id"`
			Kind string `yaml:"kind"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse %s: %w", path, err)
	}

	out := []string{}
	for _, integration := range doc.Integrations {
		if integration.Kind == kind {
			out = append(out, strings.TrimPrefix(integration.ID, kind+"."))
		}
	}
	sort.Strings(out)
	return out, nil
}
