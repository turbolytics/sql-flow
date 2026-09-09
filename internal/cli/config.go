package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/validate"
)

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

// validateConfig renders the config template and validates the result,
// delegating to internal/validate so `config validate` and `validate` cannot
// disagree about what a valid config is.
func validateConfig(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return errs.New(errs.CodeConfigNotFound, "config file not found: %s", path)
	}

	rep, err := validate.Validate(context.Background(), validate.Request{
		Path:   path,
		Config: string(src),
	})
	if err != nil {
		return err
	}
	if rep.OK {
		return nil
	}

	// Every fault in one message. Reporting the first would cost a reader a
	// run per mistake.
	var b strings.Builder
	fmt.Fprintf(&b, "%s is invalid", path)
	for _, d := range rep.Diagnostics {
		if d.Severity != validate.SeverityError {
			continue
		}
		fmt.Fprintf(&b, "\n  %s", d.Message)
	}
	return errs.New(errs.CodeConfigInvalid, "%s", b.String())
}

// configExample renders the schema as a commented YAML skeleton, a port of the
// Python jsonschema_to_yaml: descriptions become comments, primitives become
// <type> placeholders, and enums are listed as alternatives.
func configExample() (string, error) {
	var root schemaNode
	if err := json.Unmarshal(validate.SchemaJSON(), &root); err != nil {
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

		// Every line gets its own marker. A description now comes from a Go
		// doc comment and can run to several lines; prefixing only the first
		// leaves the rest as bare text, which is not valid YAML.
		for _, line := range strings.Split(value.Description, "\n") {
			if line == "" {
				continue
			}
			out = append(out, fmt.Sprintf("%s# %s", indent, line))
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
