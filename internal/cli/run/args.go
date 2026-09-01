package run

import "fmt"

// requiredAnnotation is cobra's marker for a required flag. It is named here
// so a test can assert --config is NOT marked required; marking it would
// reject the Python engine's positional form before RunE runs.
const requiredAnnotation = "cobra_annotation_bash_completion_one_required_flag"

// resolveConfigPath accepts the config the way either engine spells it.
//
// The Python engine takes the config as a positional argument
// (`run pipeline.yml`); this one grew up taking `-c pipeline.yml`. Both are
// accepted so that swapping the Go binary in under the same image and
// entrypoint does not break existing invocations.
func resolveConfigPath(flagValue string, args []string) (string, error) {
	var positional string
	if len(args) > 0 {
		positional = args[0]
	}

	switch {
	case flagValue == "" && positional == "":
		return "", fmt.Errorf("a config is required: pass it positionally (run <config>) or with -c/--config")
	case flagValue != "" && positional != "" && flagValue != positional:
		return "", fmt.Errorf(
			"conflicting configs: %q given positionally and %q given with -c/--config",
			positional, flagValue,
		)
	case flagValue != "":
		return flagValue, nil
	default:
		return positional, nil
	}
}

// resolveMaxMsgs accepts either engine's spelling of the message cap:
// --max-msgs here, --max-msgs-to-process in the Python engine. Zero means
// unlimited in both, so zero reads as "not given".
func resolveMaxMsgs(maxMsgs, maxMsgsToProcess int) (int, error) {
	switch {
	case maxMsgs != 0 && maxMsgsToProcess != 0 && maxMsgs != maxMsgsToProcess:
		return 0, fmt.Errorf(
			"conflicting message caps: --max-msgs=%d and --max-msgs-to-process=%d",
			maxMsgs, maxMsgsToProcess,
		)
	case maxMsgsToProcess != 0:
		return maxMsgsToProcess, nil
	default:
		return maxMsgs, nil
	}
}
