// Package schema generates the config JSON Schema from the Go types the
// engine actually parses.
//
// The schema used to be hand-written, which made it a second, independent
// statement of the config format. Nothing held the two together: a field added
// to config.Pipeline was invisible to the schema, and a sink added to the
// registry was rejected by `config validate` while `run` accepted it. That
// happened, to `type: webhook`.
//
// Reflecting the schema removes the second statement. The Go types are the
// format, and the schema is an artifact of them.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/invopop/jsonschema"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/internal/sources"
)

// ID is the schema's published identifier. Providers reference it, so it does
// not change.
const ID = "https://turbolytics.io/schemas/config.json"

// configPkg is the import path of the package Generate reflects. It is the key
// prefix the reflector builds doc-comment lookups from.
const configPkg = "github.com/turbolytics/sql-flow/internal/config"

// Generate reflects config.Conf into a JSON Schema document.
//
// configDir is the path to internal/config, which the reflector reads doc
// comments from. It is a parameter because the caller's working directory
// differs: the make target runs from the repository root, the golden test from
// its own package.
//
// The output is deterministic: the same types produce the same bytes, which is
// what lets a golden test hold the committed file equal to this.
func Generate(configDir string) ([]byte, error) {
	r := &jsonschema.Reflector{
		// The config is YAML. Its Go types carry yaml tags and no json tags,
		// so the reflector reads names, omitempty and inlining from those.
		FieldNameTag: "yaml",

		// One document rather than a $defs map with references. The schema is
		// read by people and by `config example`, and a flat tree is what both
		// want.
		ExpandedStruct: true,
		DoNotReference: true,
		Anonymous:      true,
	}

	// Descriptions live in doc comments beside the fields, so the format is
	// documented where it is defined rather than in a parallel file.
	// The base is the package the directory holds, not the module: comment
	// keys are built by joining base with each file's path relative to
	// configDir, so naming the module here would key every comment as
	// "sql-flow.Sink" and match nothing.
	if err := r.AddGoComments(configPkg, configDir); err != nil {
		return nil, fmt.Errorf("reading config doc comments from %s: %w", configDir, err)
	}

	s := r.Reflect(&config.Conf{})
	s.ID = ID
	s.Version = "https://json-schema.org/draft/2020-12/schema"

	if err := applyEnums(s); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	// Schema text is not HTML. Escaping < and > would mangle any description
	// that compares values.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// applyEnums fills the type discriminators from the registries the engine
// builds from.
//
// This is the reason to generate rather than hand-write. A hand-copied enum
// drifts the moment someone adds an integration, and the drift is silent until
// a user writes the new type and `validate` rejects what `run` accepts.
func applyEnums(s *jsonschema.Schema) error {
	targets := []struct {
		path  []string
		kinds []string
	}{
		{[]string{"pipeline", "source", "type"}, sources.Kinds()},
		{[]string{"pipeline", "sink", "type"}, sinks.Kinds()},
		{[]string{"pipeline", "handler", "type"}, handlers.ConfigTypes()},
	}

	for _, t := range targets {
		node, err := resolve(s, t.path)
		if err != nil {
			return err
		}
		node.Enum = make([]any, 0, len(t.kinds))
		for _, k := range t.kinds {
			node.Enum = append(node.Enum, k)
		}
	}

	// Sinks appear in three more places, each a full sink definition: the DLQ
	// a failed record diverts to, and the sink a managed table's window
	// collects into. Missing one leaves an enum that drifts exactly where the
	// hand-written schema drifted.
	for _, path := range [][]string{
		{"pipeline", "on_error", "dlq", "type"},
		{"tables", "sql", "manager", "sink", "type"},
	} {
		node, err := resolve(s, path)
		if err != nil {
			return err
		}
		node.Enum = make([]any, 0, len(sinks.Kinds()))
		for _, k := range sinks.Kinds() {
			node.Enum = append(node.Enum, k)
		}
	}

	return nil
}

// jsonRoundTrip normalizes a YAML document into the JSON data model the
// validator works on.
func jsonRoundTrip(v any) (any, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// resolve walks a property path, stepping through arrays as it meets them.
func resolve(s *jsonschema.Schema, path []string) (*jsonschema.Schema, error) {
	node := s
	for i, key := range path {
		if node.Items != nil && node.Properties == nil {
			node = node.Items
		}
		if node.Properties == nil {
			return nil, fmt.Errorf("schema path %v: no properties at %q", path, key)
		}
		next, ok := node.Properties.Get(key)
		if !ok {
			return nil, fmt.Errorf("schema path %v: %q not found at depth %d", path, key, i)
		}
		node = next
	}
	return node, nil
}
