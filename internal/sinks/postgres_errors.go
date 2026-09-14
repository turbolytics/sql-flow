package sinks

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// postgresError codes a failure by its SQLSTATE class, which is the whole
// retry policy for this sink: the ladder retries system.sink.unreachable and
// nothing user class, so the table below decides both the code and whether
// another attempt is made. No new code: a server-side data exception is the
// value #233's encode_failed describes, one hop later.
//
//	08  connection exception             unreachable   retried, exit 12
//	57  operator intervention, shutdown  unreachable   retried, exit 12
//	53  insufficient resources           unreachable   retried, exit 12
//	40  serialization, deadlock          unreachable   retried, exit 12
//	22  data exception                   encode_failed exit 10
//	21  cardinality violation            sink invalid  exit 10
//	23  integrity constraint             sink invalid  exit 10
//	42  syntax, undefined object, grant  sink invalid  exit 10
//	28  authentication                   sink invalid  exit 10
//	3D  3F  database, schema missing     sink invalid  exit 10
//	else                                 write_failed  exit 1
func postgresError(err error, format string, args ...any) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		args = append(args, pgErr.Code)
		format += " (SQLSTATE %s)"
		class := ""
		if len(pgErr.Code) >= 2 {
			class = pgErr.Code[:2]
		}
		switch class {
		case "08", "57", "53", "40":
			return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
		case "22":
			return errs.Wrap(errs.CodeSinkEncodeFailed, err, format, args...)
		case "21", "23", "42", "28", "3D", "3F":
			return errs.Wrap(errs.CodeSinkInvalid, err, format, args...)
		default:
			return errs.Wrap(errs.CodeSinkWriteFailed, err, format, args...)
		}
	}
	if isUnreachable(err) || errors.Is(err, context.Canceled) {
		return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
	}
	return errs.Wrap(errs.CodeSinkWriteFailed, err, format, args...)
}

// copyFailedPrefix is how the server words a COPY the client abandoned. pgx
// abandons one when it cannot encode a value for its column, and the server
// then answers 57014, query_canceled, the same code a cancelled statement
// gets.
const copyFailedPrefix = "COPY from stdin failed"

// postgresCopyError codes a CopyFrom failure.
//
// A value pgx cannot encode for its column fails identically on every
// attempt, and it reaches the sink two ways: as a plain error before any row
// is sent, or as the server's 57014 after pgx abandoned a COPY it had
// started. Read by class alone, the second is operator intervention and
// would be retried as unreachable, so it is recognised by the server's
// wording first. Any other server answer or network failure classifies as
// any other statement's.
func postgresCopyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "57014" && strings.HasPrefix(pgErr.Message, copyFailedPrefix) {
			return errs.Wrap(errs.CodeSinkEncodeFailed, err, "postgres sink: encode a value for its column (SQLSTATE %s)", pgErr.Code)
		}
		return postgresError(err, "postgres sink: copy into staging")
	}
	if isUnreachable(err) || errors.Is(err, context.Canceled) {
		return postgresError(err, "postgres sink: copy into staging")
	}
	return errs.Wrap(errs.CodeSinkEncodeFailed, err, "postgres sink: encode a value for its column")
}
