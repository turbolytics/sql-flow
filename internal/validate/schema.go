package validate

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// configSchemaJSON is the config JSON Schema. It moved here from internal/cli
// because go:embed cannot reach outside its own package, and this package is
// now the one that validates against it.
//
//go:embed schemas/config.json
var configSchemaJSON []byte

const schemaURL = "https://turbolytics.io/schemas/config.json"

// SchemaJSON returns the embedded config schema. `config example` renders it
// as a commented skeleton.
func SchemaJSON() []byte { return configSchemaJSON }

// checkSchema validates the rendered config against the config schema and
// reports every violation, each anchored to the line it came from.
func checkSchema(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not valid YAML: "+err.Error(), nil))
		return
	}

	var doc any
	if err := yaml.Unmarshal(rendered, &doc); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigParseFailed, SeverityError,
			"the config is not valid YAML: "+err.Error(), nil))
		return
	}

	// The validator works on the JSON data model, so the document is
	// round-tripped to normalize YAML's own types.
	normalized, err := jsonRoundTrip(doc)
	if err != nil {
		rep.SetCheck("config.schema", StatusFail, "")
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError,
			"the config could not be normalized: "+err.Error(), nil))
		return
	}

	schema, err := compileSchema()
	if err != nil {
		// Ours, not the user's. Skipped rather than failed: the config was
		// never actually checked and must not be reported as sound.
		rep.SetCheck("config.schema", StatusSkipped,
			"the embedded schema failed to compile: "+err.Error())
		return
	}

	if err := schema.Validate(normalized); err != nil {
		rep.SetCheck("config.schema", StatusFail, "")

		var verr *jsonschema.ValidationError
		if !errors.As(err, &verr) {
			rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, err.Error(), nil))
			return
		}

		for _, leaf := range leaves(verr) {
			var pos *Position
			if line, col, ok := lineOf(&root, leaf.InstanceLocation); ok {
				pos = &Position{Source: "config", Line: line, Column: col}
			}
			// leaf.Error() rather than ErrorKind.LocalizedString: the latter
			// wants a *message.Printer, and a nil one is not documented as
			// safe. Error() already nests the cause under its location.
			d := diagnostic(errs.CodeConfigInvalid, SeverityError, leaf.Error(), pos)
			d.Context = "/" + strings.Join(leaf.InstanceLocation, "/")
			rep.Add(d)
		}
		return
	}

	rep.SetCheck("config.schema", StatusPass, "")
}

// leaves flattens a validation error to its most specific causes. The root
// error says only that the document is invalid; the leaves name the keys.
func leaves(e *jsonschema.ValidationError) []*jsonschema.ValidationError {
	if len(e.Causes) == 0 {
		return []*jsonschema.ValidationError{e}
	}
	var out []*jsonschema.ValidationError
	for _, c := range e.Causes {
		out = append(out, leaves(c)...)
	}
	return out
}

// lineOf resolves an instance path to a position in the YAML document. A
// diagnostic that cannot name a line sends its reader to the top of the file,
// which for a model means editing the wrong thing.
func lineOf(root *yaml.Node, path []string) (int, int, bool) {
	node := root
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		node = node.Content[0]
	}

	for depth, seg := range path {
		last := depth == len(path)-1

		switch node.Kind {
		case yaml.MappingNode:
			found := false
			// Content alternates key, value.
			for i := 0; i+1 < len(node.Content); i += 2 {
				if node.Content[i].Value != seg {
					continue
				}
				// The key carries the position a reader wants, not the value.
				// "/commands: got object, want array" belongs on the
				// `commands:` line, not on the first line of the object under
				// it, so an editor jumps to the name that is wrong.
				if last {
					return node.Content[i].Line, node.Content[i].Column, true
				}
				node = node.Content[i+1]
				found = true
				break
			}
			if !found {
				return node.Line, node.Column, true
			}
		case yaml.SequenceNode:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node.Content) {
				return node.Line, node.Column, true
			}
			node = node.Content[idx]
		default:
			return node.Line, node.Column, true
		}
	}
	return node.Line, node.Column, true
}

func compileSchema() (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(configSchemaJSON))
	if err != nil {
		return nil, fmt.Errorf("parsing config schema failed: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaURL, doc); err != nil {
		return nil, fmt.Errorf("loading config schema failed: %w", err)
	}
	return compiler.Compile(schemaURL)
}

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
