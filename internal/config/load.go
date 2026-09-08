package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/turbolytics/sql-flow/internal/errs"
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
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, errs.New(errs.CodeConfigNotFound, "config file not found: %s", path)
	}

	return RenderTemplateString(src, overrides)
}

// RenderTemplateString renders config text. RenderTemplate is the same thing
// for a file, and calls this.
//
// The text form exists because a validation job carries the config's content
// rather than a path: the pull-based control plane hands a job to an instance
// that shares no filesystem with the submitter (#178).
func RenderTemplateString(src []byte, overrides map[string]string) ([]byte, error) {
	tmpl, err := gonja.FromBytes(src)
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigParseFailed, err, "parsing template failed")
	}

	vars := map[string]any{}
	for k, v := range TemplateVars(overrides) {
		vars[k] = v
	}

	out, err := tmpl.ExecuteToBytes(exec.NewContext(vars))
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigParseFailed, err, "rendering template failed")
	}

	return out, nil
}

// SettingsVarNames are the variables the engine injects into every render.
//
// Validation needs them separated from the rest: nobody supplied them, so
// reporting them as supplied-but-never-read would put two lines of noise on
// every config and bury the one name that matters.
func SettingsVarNames() []string {
	names := make([]string, 0, 2)
	for k := range settingsVars() {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// TemplateVars returns the context a render runs against, so validation can
// report what the config actually had available to it.
func TemplateVars(overrides map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range settingsVars() {
		out[k] = fmt.Sprint(v)
	}
	for _, v := range os.Environ() {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 && strings.HasPrefix(parts[0], "SQLFLOW_") {
			out[parts[0]] = parts[1]
		}
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func Load(path string, overrides map[string]string) (*Conf, error) {
	rendered, err := RenderTemplate(path, overrides)
	if err != nil {
		// Returned as-is. The inner error already names the file and the
		// stage that failed, so another "rendering config failed" prefix adds
		// a word and no information.
		return nil, err
	}

	var conf Conf
	// Decoded strictly: the config schema sets additionalProperties: false, so
	// an unrecognized key is a typo the user wants to hear about rather than a
	// setting silently dropped.
	dec := yaml.NewDecoder(bytes.NewReader(rendered))
	dec.KnownFields(true)
	if err := dec.Decode(&conf); err != nil {
		return nil, errs.Wrap(errs.CodeConfigParseFailed, err, "parsing YAML failed")
	}
	return &conf, nil
}
