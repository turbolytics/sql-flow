package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"gopkg.in/yaml.v3"
)

// SQLResultsCacheDir mirrors sqlflow.settings.SQL_RESULTS_CACHE_DIR: where
// disk-backed handlers stage their batch and result files. It is the fallback
// for a handler that does not declare sql_results_cache_dir.
func SQLResultsCacheDir() string {
	if dir := os.Getenv("SQLFLOW_SQL_RESULTS_CACHE_DIR"); dir != "" {
		return dir
	}
	return filepath.Join("/tmp", "sqlflow", "resultscache")
}

// settingsVars mirrors sqlflow.settings.VARS: values the Python engine seeds
// the template context with, overridable by their SQLFLOW_-prefixed env var.
func settingsVars() map[string]any {
	staticRoot := os.Getenv("SQLFLOW_STATIC_ROOT")
	if staticRoot == "" {
		staticRoot = filepath.Join("/tmp", "sqlflow", "static")
	}

	return map[string]any{
		"STATIC_ROOT":           staticRoot,
		"SQL_RESULTS_CACHE_DIR": SQLResultsCacheDir(),
	}
}

// RenderTemplate renders a config through Jinja2, the templating the config
// spec is written in: every SQLFLOW_-prefixed environment variable is in
// scope, plus the settings vars, plus explicit overrides.
func RenderTemplate(path string, overrides map[string]string) ([]byte, error) {
	// Checked up front: the template loader reports a missing file as a stat
	// error against its parent directory, which reads as an unrelated failure.
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("config file not found: %s", path)
	}

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
	// Decoded strictly: the config schema sets additionalProperties: false, so
	// an unrecognized key is a typo the user wants to hear about rather than a
	// setting silently dropped.
	dec := yaml.NewDecoder(bytes.NewReader(rendered))
	dec.KnownFields(true)
	if err := dec.Decode(&conf); err != nil {
		return nil, fmt.Errorf("parsing YAML failed: %w", err)
	}
	return &conf, nil
}
