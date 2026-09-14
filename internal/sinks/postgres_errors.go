package sinks

import (
	"context"
	"errors"

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

// postgresCopyError codes a CopyFrom failure. A server answer or a network
// failure classifies as any other statement's. Anything else is pgx refusing
// to encode a Go value for the column's type, which fails identically on
// every attempt.
func postgresCopyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) || isUnreachable(err) || errors.Is(err, context.Canceled) {
		return postgresError(err, "postgres sink: copy into staging")
	}
	return errs.Wrap(errs.CodeSinkEncodeFailed, err, "postgres sink: encode a value for its column")
}
