package serve

import "regexp"

var (
	// userinfoPassword matches the password in scheme://user:password@host.
	userinfoPassword = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s"']*):[^@\s"']*@`)
	// keywordPassword matches password=... in a libpq keyword string.
	keywordPassword = regexp.MustCompile(`(?i)(password\s*=\s*)('[^']*'|[^\s"']+)`)
)

// Redact removes passwords from a database error before it is logged.
//
// DuckDB's Postgres extension puts the whole connection string in a
// connection error: `Unable to connect to Postgres at
// "postgresql://user:password@host/db"`. A serve config attaches with exactly
// that string, so the error that says Postgres is unreachable also says how
// to log in to it.
func Redact(s string) string {
	s = userinfoPassword.ReplaceAllString(s, "$1:***@")
	return keywordPassword.ReplaceAllString(s, "${1}***")
}
