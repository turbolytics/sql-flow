package run

import (
	"testing"

	"github.com/zeebo/assert"
)

// The Go engine is meant to be a drop-in replacement for the Python one,
// shipped under the same image and the same entrypoint. `run` is the only
// command whose surface differed: Python takes the config positionally and
// caps with --max-msgs-to-process, Go took -c and --max-msgs. Both spellings
// have to work, or swapping the image breaks every existing invocation.
func TestCliInvocation_ResolveConfigPathPythonPositionalForm(t *testing.T) {
	got, err := resolveConfigPath("", []string{"pipeline.yml"})
	assert.NoError(t, err)
	assert.Equal(t, "pipeline.yml", got)
}

func TestCliInvocation_ResolveConfigPathGoFlagForm(t *testing.T) {
	got, err := resolveConfigPath("pipeline.yml", nil)
	assert.NoError(t, err)
	assert.Equal(t, "pipeline.yml", got)
}

// Passing the same path both ways is redundant but unambiguous, so it is
// allowed; two different paths is a mistake worth reporting.
func TestCliInvocation_ResolveConfigPathBothFormsAgreeing(t *testing.T) {
	got, err := resolveConfigPath("pipeline.yml", []string{"pipeline.yml"})
	assert.NoError(t, err)
	assert.Equal(t, "pipeline.yml", got)
}

func TestCliInvocation_ResolveConfigPathConflictingFormsError(t *testing.T) {
	_, err := resolveConfigPath("a.yml", []string{"b.yml"})
	assert.Error(t, err)
}

func TestCliInvocation_ResolveConfigPathMissingConfigErrors(t *testing.T) {
	_, err := resolveConfigPath("", nil)
	assert.Error(t, err)
}

func TestCliInvocation_ResolveMaxMsgsPythonFlag(t *testing.T) {
	got, err := resolveMaxMsgs(0, 500)
	assert.NoError(t, err)
	assert.Equal(t, 500, got)
}

func TestCliInvocation_ResolveMaxMsgsGoFlag(t *testing.T) {
	got, err := resolveMaxMsgs(500, 0)
	assert.NoError(t, err)
	assert.Equal(t, 500, got)
}

func TestCliInvocation_ResolveMaxMsgsBothFormsAgreeing(t *testing.T) {
	got, err := resolveMaxMsgs(500, 500)
	assert.NoError(t, err)
	assert.Equal(t, 500, got)
}

func TestCliInvocation_ResolveMaxMsgsConflictingFormsError(t *testing.T) {
	_, err := resolveMaxMsgs(500, 900)
	assert.Error(t, err)
}

// Neither given means no cap, which is the documented default.
func TestCliInvocation_ResolveMaxMsgsUnsetIsUnlimited(t *testing.T) {
	got, err := resolveMaxMsgs(0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, got)
}

// The command must accept zero or one positional argument: zero for the -c
// form, one for the Python form. A second is a typo, not a config.
func TestCliInvocation_NewCommandRejectsTwoPositionalArgs(t *testing.T) {
	cmd := NewCommand()
	assert.Error(t, cmd.Args(cmd, []string{"a.yml", "b.yml"}))
	assert.NoError(t, cmd.Args(cmd, []string{"a.yml"}))
	assert.NoError(t, cmd.Args(cmd, nil))
}

// --config must not be marked required, or the positional form fails before
// RunE is ever reached.
func TestCliInvocation_NewCommandConfigFlagIsNotRequired(t *testing.T) {
	cmd := NewCommand()
	flag := cmd.Flags().Lookup("config")
	assert.NotNil(t, flag)
	assert.Equal(t, 0, len(flag.Annotations[requiredAnnotation]))
}

func TestCliInvocation_NewCommandHasPythonMaxMsgsFlag(t *testing.T) {
	cmd := NewCommand()
	assert.NotNil(t, cmd.Flags().Lookup("max-msgs-to-process"))
	assert.NotNil(t, cmd.Flags().Lookup("max-msgs"))
}
