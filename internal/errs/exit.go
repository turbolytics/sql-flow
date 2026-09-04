package errs

// Exit codes tell a supervisor what to do next. They are a coarse projection
// of the error codes, and they live here so the two classifications cannot
// drift apart: #161 consumes this table rather than restating it.
//
// The distinction that matters is Retryable. Restarting a pipeline whose
// config is wrong burns a restart and fails the same way, forever. That is
// the crash loop a supervisor has to be told to avoid.
const (
	// ExitOK is a clean shutdown, including a drain on SIGTERM.
	ExitOK = 0

	// ExitInternal is a failure we could not classify. Retryable, because a
	// bug we have not characterized may well be transient.
	ExitInternal = 1

	// 2 is deliberately unused. Cobra exits 2 on a usage error, and Go's
	// runtime exits 2 when a signal cannot kill PID 1. Reusing it here would
	// make those indistinguishable from a pipeline failure.

	// ExitUserError marks a failure only the user can fix: config, SQL,
	// credentials. Terminal. A supervisor that retries it loops forever.
	ExitUserError = 10

	// ExitSourceUnreachable and ExitSinkUnreachable mark a dependency that
	// was not there. Retryable: the dependency may come back.
	ExitSourceUnreachable = 11
	ExitSinkUnreachable   = 12

	// ExitResourceLimit marks a declared limit the pipeline exceeded.
	// Retryable, though it repeats until someone raises the limit or the
	// workload shrinks.
	ExitResourceLimit = 13

	// ExitStateCorrupt marks a state file that cannot be read. Terminal: a
	// restart reads the same bytes. An operator has to look at it.
	ExitStateCorrupt = 14
)

// exitCodes overrides the class default for codes whose remedy is more
// specific than "the user fixes it" or "we look at it".
var exitCodes = map[Code]int{
	CodeStateCorrupt:      ExitStateCorrupt,
	CodeSourceUnreachable: ExitSourceUnreachable,
	CodeSinkUnreachable:   ExitSinkUnreachable,
}

// ExitCode maps an error to the code the process should exit with.
//
// An unknown code still resolves, because it falls back to the class prefix.
// A future system.limit.disk_exhausted exits ExitInternal rather than
// crashing the mapping, and gains its own entry when someone adds it.
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}

	code := CodeOf(err)
	if exit, ok := exitCodes[code]; ok {
		return exit
	}

	switch code.Class() {
	case ClassUser:
		return ExitUserError
	case ClassSystem:
		if code.Domain() == "limit" {
			return ExitResourceLimit
		}
		return ExitInternal
	default:
		return ExitInternal
	}
}

// Retryable reports whether restarting the process could succeed. A
// supervisor should stop restarting a pipeline that exits with a code this
// reports false for.
func Retryable(exit int) bool {
	switch exit {
	case ExitUserError, ExitStateCorrupt:
		return false
	default:
		return true
	}
}
