package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"gopkg.in/yaml.v3"
)

// settingsVars mirrors sqlflow.settings.VARS: values the Python engine seeds
// the template context with, overridable by their SQLFLOW_-prefixed env var.
func settingsVars() map[string]any {
	staticRoot := os.Getenv("SQLFLOW_STATIC_ROOT")
	if staticRoot == "" {
		staticRoot = filepath.Join("/tmp", "sqlflow", "static")
	}

	cacheDir := os.Getenv("SQLFLOW_SQL_RESULTS_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = filepath.Join("/tmp", "sqlflow", "resultscache")
	}

	return map[string]any{
		"STATIC_ROOT":           staticRoot,
		"SQL_RESULTS_CACHE_DIR": cacheDir,
	}
}

// RenderTemplate renders a config through Jinja2, the templating the config
// spec is written in: every SQLFLOW_-prefixed environment variable is in
// scope, plus the settings vars, plus explicit overrides.
func RenderTemplate(path string, overrides map[string]string) ([]byte, error) {
	tmpl, err := gonja.FromFile(path)
	if err != nil {
		return nil, fmt.Errorf("parsing template failed: %w", err)
	}

	vars := settingsVars()
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], "SQLFLOW_") {
			vars[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		vars[k] = v
	}

	out, err := tmpl.ExecuteToBytes(exec.NewContext(vars))
	if err != nil {
		return nil, fmt.Errorf("rendering template failed: %w", err)
	}

	return out, nil
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
