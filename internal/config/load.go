package config

import (
	"fmt"
	"github.com/flosch/pongo2/v6"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

func RenderTemplate(path string, overrides map[string]string) ([]byte, error) {
	/*
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	*/

	tmpl := pongo2.Must(pongo2.FromFile(path))

	/*
		tmpl, err := template.New("config").Parse(string(raw))
		if err != nil {
			return nil, err
		}
	*/

	vars := pongo2.Context{}
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], "SQLFLOW_") {
			vars[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		vars[k] = v
	}

	out, err := tmpl.Execute(vars)
	if err != nil {
		return nil, fmt.Errorf("rendering template failed: %w", err)
	}

	return []byte(out), nil
}

func Load(path string, overrides map[string]string) (*Conf, error) {
	rendered, err := RenderTemplate(path, overrides)
	if err != nil {
		return nil, fmt.Errorf("rendering config failed: %w", err)
	}

	var conf Conf
	if err := yaml.Unmarshal(rendered, &conf); err != nil {
		return nil, fmt.Errorf("parsing YAML failed: %w", err)
	}
	return &conf, nil
}
