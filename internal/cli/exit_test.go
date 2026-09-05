package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const statefulExample = "../../dev/config/examples/kafka.stateful.window.yml"

// corruptStateCommand points the stateful example at a file that is not a
// DuckDB database, and captures everything the command prints.
func corruptStateCommand(t *testing.T) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.db")
	assert.NoError(t, os.WriteFile(statePath, []byte("not a duckdb database"), 0o644))
	t.Setenv("SQLFLOW_STATE_PATH", statePath)

	var out bytes.Buffer
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"run", statefulExample, "--max-msgs", "1"})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	return cmd, &out
}

func runWithCorruptState(t *testing.T) (error, string) {
	t.Helper()
	cmd, out := corruptStateCommand(t)
	return cmd.Execute(), out.String()
}

// The exit code is the whole point of the taxonomy: it is what a supervisor
// reads. Exiting 1 marks the failure retryable, so the supervisor restarts
// forever into the same bytes.
func TestLifecycleExitCodes_CorruptStateFileExitsTerminal(t *testing.T) {
	cmd, _ := corruptStateCommand(t)

	code := execute(cmd)

	assert.Equal(t, errs.ExitStateCorrupt, code)
	assert.That(t, !errs.Retryable(code))
}

// A successful command exits zero.
func TestLifecycleExitCodes_SuccessExitsZero(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"version"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})

	assert.Equal(t, errs.ExitOK, execute(cmd))
}

// Guards propagation rather than driving it: the code has to survive cobra's
// error path to reach execute at all.
func TestLifecycleExitCodes_SurvivesCobra(t *testing.T) {
	err, _ := runWithCorruptState(t)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCorrupt, errs.CodeOf(err))
}

// Failing loud means the operator sees the error. Cobra prints the full flag
// list for any error a command returns, which buries the one line that says
// what went wrong.
func TestLifecycleExitCodes_NoFlagList(t *testing.T) {
	_, output := runWithCorruptState(t)

	if strings.Contains(output, "Flags:") {
		t.Errorf("a runtime failure printed the usage text:\n%s", output)
	}
}

// runArgs executes the CLI with args and reports the exit code and output.
func runArgs(t *testing.T, args ...string) (int, string) {
	t.Helper()
	var out bytes.Buffer
	cmd := NewRootCommand()
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	return execute(cmd), out.String()
}

// A config path that does not exist is the user's to fix. Exiting retryable
// would have a supervisor restart until someone notices.
func TestLifecycleExitCodes_MissingConfigIsTerminal(t *testing.T) {
	code, output := runArgs(t, "run", "/nope/does-not-exist.yml")

	assert.Equal(t, errs.ExitUserError, code)
	assert.That(t, !errs.Retryable(code))
	assert.That(t, strings.Contains(output, string(errs.CodeConfigNotFound)))
}

// Unparseable YAML does not become parseable on a retry.
func TestLifecycleExitCodes_MalformedConfigIsTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yml")
	assert.NoError(t, os.WriteFile(path, []byte("pipeline:\n  name: [unclosed\n"), 0o644))

	code, output := runArgs(t, "run", path)

	assert.Equal(t, errs.ExitUserError, code)
	assert.That(t, !errs.Retryable(code))
	assert.That(t, strings.Contains(output, "user.config"))
}

// Suppressing usage for runtime errors must not suppress it for the errors
// usage actually answers.
func TestLifecycleExitCodes_UsageErrorStillPrintsUsage(t *testing.T) {
	_, output := runArgs(t, "run", "--not-a-real-flag")

	if !strings.Contains(output, "Flags:") {
		t.Errorf("a usage error lost its usage text:\n%s", output)
	}
}
