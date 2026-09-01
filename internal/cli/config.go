package cli

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/config"
	"gopkg.in/yaml.v3"
)

// configSchemaJSON is the config JSON Schema the Python engine validates
// against. go:embed cannot reach outside its own package directory, so the
// canonical file at sqlflow/static/schemas/config.json is mirrored here;
// TestEmbeddedSchemaMatchesPython fails if the two drift apart.
//
//go:embed schemas/config.json
var configSchemaJSON []byte

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect and validate sqlflow configuration",
	}

	cmd.AddCommand(newConfigValidateCommand())
	cmd.AddCommand(newConfigExampleCommand())

	return cmd
}

func newConfigValidateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <config>",
		Short: "Validate the configuration file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Usage on a schema violation is noise: the config parsed as a
			// command line just fine, it is the file that is wrong.
			cmd.SilenceUsage = true

			if err := validateConfig(args[0]); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%s: valid\n", args[0])
			return nil
		},
	}
}

func newConfigExampleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "example",
		Short: "Print a commented example configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			out, err := configExample()
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(cmd.OutOrStdout(), out)
			return err
		},
	}
}

// validateConfig renders the config template and validates the result against
// the config schema, the same two steps the Python `config validate` performs.
func validateConfig(path string) error {
	rendered, err := config.RenderTemplate(path, map[string]string{})
	if err != nil {
		return err
	}

	var doc any
	if err := yaml.Unmarshal(rendered, &doc); err != nil {
		return fmt.Errorf("parsing YAML failed: %w", err)
	}

	// The validator works on the JSON data model, so the YAML document is
	// round-tripped to normalize its types (YAML ints, in particular).
	normalized, err := jsonRoundTrip(doc)
	if err != nil {
		return fmt.Errorf("normalizing config failed: %w", err)
	}

	schema, err := compileConfigSchema()
	if err != nil {
		return err
	}

	if err := schema.Validate(normalized); err != nil {
		// The validator's own message already nests each cause under the
		// instance location that failed.
		return fmt.Errorf("%s is invalid: %w", path, err)
	}

	return nil
}

func compileConfigSchema() (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(configSchemaJSON))
	if err != nil {
		return nil, fmt.Errorf("parsing config schema failed: %w", err)
	}

	const schemaURL = "https://turbolytics.io/schemas/config.json"

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaURL, doc); err != nil {
		return nil, fmt.Errorf("loading config schema failed: %w", err)
	}

	schema, err := compiler.Compile(schemaURL)
	if err != nil {
		return nil, fmt.Errorf("compiling config schema failed: %w", err)
	}

	return schema, nil
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

// configExample renders the schema as a commented YAML skeleton, a port of the
// Python jsonschema_to_yaml: descriptions become comments, primitives become
// <type> placeholders, and enums are listed as alternatives.
func configExample() (string, error) {
	var root schemaNode
	if err := json.Unmarshal(configSchemaJSON, &root); err != nil {
		return "", fmt.Errorf("parsing config schema failed: %w", err)
	}

	lines := processProperties(root.Properties, 0)
	return strings.Join(lines, "\n") + "\n", nil
}

func processProperties(properties []schemaProperty, level int) []string {
	var out []string
	indent := strings.Repeat("  ", level)

	for _, prop := range properties {
		value := prop.Node

		if value.Description != "" {
			out = append(out, fmt.Sprintf("%s# %s", indent, value.Description))
		}

		placeholder := value.placeholder()

		switch value.Type {
		case "object":
			out = append(out, fmt.Sprintf("%s%s:", indent, prop.Name))
			out = append(out, processProperties(value.Properties, level+1)...)
		case "array":
			out = append(out, fmt.Sprintf("%s%s:", indent, prop.Name))
			if value.Items != nil && len(value.Items.Properties) > 0 {
				out = append(out, fmt.Sprintf("%s  -", indent))
				out = append(out, processProperties(value.Items.Properties, level+2)...)
			} else {
				out = append(out, fmt.Sprintf("%s  - %s", indent, placeholder))
			}
		default:
			out = append(out, fmt.Sprintf("%s%s: %s", indent, prop.Name, placeholder))
		}
	}

	return out
}

var typePlaceholders = map[string]string{
	"string":  "<string>",
	"integer": "<integer>",
	"boolean": "<boolean>",
	"number":  "<number>",
	"array":   "<array>",
	"object":  "<object>",
}

func (n schemaNode) placeholder() string {
	if len(n.Enum) > 0 {
		return strings.Join(n.Enum, " | ")
	}
	if n.Type == "" {
		return "<unknown>"
	}
	if p, ok := typePlaceholders[n.Type]; ok {
		return p
	}
	return "<unknown>"
}

// schemaNode is the slice of JSON Schema the example renderer reads. Property
// order is preserved, since it determines the order of the emitted YAML.
type schemaNode struct {
	Description string
	Type        string
	Enum        []string
	Properties  []schemaProperty
	Items       *schemaNode
}

type schemaProperty struct {
	Name string
	Node schemaNode
}

func (n *schemaNode) UnmarshalJSON(data []byte) error {
	var raw struct {
		Description string            `json:"description"`
		Type        string            `json:"type"`
		Enum        []json.RawMessage `json:"enum"`
		Properties  orderedProperties `json:"properties"`
		Items       *schemaNode       `json:"items"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	n.Description = raw.Description
	n.Type = raw.Type
	n.Properties = raw.Properties
	n.Items = raw.Items

	// Python renders enum members with str(), which prints a string bare and
	// any other scalar as its literal text.
	for _, member := range raw.Enum {
		var s string
		if err := json.Unmarshal(member, &s); err == nil {
			n.Enum = append(n.Enum, s)
			continue
		}
		n.Enum = append(n.Enum, string(member))
	}

	return nil
}

// orderedProperties decodes a JSON object into properties in document order,
// which a map would lose.
type orderedProperties []schemaProperty

func (p *orderedProperties) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))

	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected a JSON object, got %v", token)
	}

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected a property name, got %v", token)
		}

		var node schemaNode
		if err := decoder.Decode(&node); err != nil {
			return err
		}

		*p = append(*p, schemaProperty{Name: name, Node: node})
	}

	// Consume the closing brace.
	_, err = decoder.Token()
	return err
}
