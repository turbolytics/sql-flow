package serve

import "fmt"

// resolveConfigPath accepts the config positionally or with -c, the way run
// does, so the two commands are spelled alike.
func resolveConfigPath(flagValue string, args []string) (string, error) {
	var positional string
	if len(args) > 0 {
		positional = args[0]
	}

	switch {
	case flagValue == "" && positional == "":
		return "", fmt.Errorf("a config is required: pass it positionally (serve <config>) or with -c/--config")
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
