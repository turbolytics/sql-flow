package serve

import "regexp"

var (
	// userinfoPassword matches the password in scheme://user:password@host.
	userinfoPassword = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s"']*):[^@\s"']*@`)

	// assignedSecret matches a credential assigned with = or :, in a libpq
	// keyword string, a URL query, or a line of config. The surrounding
	// [a-z0-9_]* catch the compound names backends actually use:
	// motherduck_token, s3_secret_access_key, aws_session_token.
	assignedSecret = regexp.MustCompile(
		`(?i)\b([a-z0-9_]*(?:password|passwd|pwd|token|secret|api_?key|access_?key|credential)[a-z0-9_]*)(\s*[=:]\s*)('[^']*'|"[^"]*"|[^\s"'&;,)]+)`)

	// optionSecret matches DuckDB's own option form, a bare keyword followed
	// by a quoted value: ATTACH ... (TOKEN 'x'), CREATE SECRET (... SECRET
	// 'x'). KEY_ID is deliberately absent: it identifies rather than
	// authenticates, and a reader needs it to tell two secrets apart.
	optionSecret = regexp.MustCompile(`(?i)\b(token|secret|password)(\s+)'[^']*'`)
)

// Redact removes credentials from a database error before it is logged.
//
// DuckDB's Postgres extension puts the whole connection string in a
// connection error: `Unable to connect to Postgres at
// "postgresql://user:password@host/db"`. A serve config attaches with exactly
// that string, so the error that says Postgres is unreachable also says how
// to log in to it. MotherDuck does the same with its token, in the URL or as
// an ATTACH option, and DuckDB's secrets with their keys.
//
// It errs towards redacting. A query whose own SQL reads `WHERE token = 'abc'`
// loses that literal from the log, which costs a reader one detail; leaking a
// credential into a log an operator pastes into an issue costs more.
func Redact(s string) string {
	s = userinfoPassword.ReplaceAllString(s, "$1:***@")
	s = assignedSecret.ReplaceAllString(s, "${1}${2}***")
	return optionSecret.ReplaceAllString(s, "$1$2'***'")
}
