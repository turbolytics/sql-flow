package errs

import "sort"

// The registry is append-only. A published code keeps its meaning forever,
// because providers show these to their customers and write automation
// against them.
//
// Adding a code is safe: consumers match on the class prefix, so an unknown
// code still routes correctly. Removing one or changing what it means is not.
// TestRegistryIsAppendOnly compares this file against testdata/codes.golden
// and fails on a removal.
//
// Each domain carries a catch-all, so a new failure always has a home before
// anyone gives it a specific code. User domains catch all with `invalid`,
// system domains with `internal`. Reach for a specific code first; use the
// catch-all rather than inventing a code in a hurry.
const (
	// Config: the pipeline file itself.
	CodeConfigNotFound    Code = "user.config.not_found"
	CodeConfigParseFailed Code = "user.config.parse_failed"
	CodeConfigInvalid     Code = "user.config.invalid"

	// A template variable the config reads but nothing defines. Jinja2
	// renders it as an empty string and reports no error, so the failure
	// surfaces far from its cause: #120 spent days on an authentication
	// error that was a misspelled variable name.
	CodeConfigTemplateUndefined Code = "user.config.template_undefined"

	// A serve file sets a policy this version parses and refuses, such as
	// rate_limit. Accepting it silently would let an author ship a config
	// believing it enforced.
	CodeConfigServeReserved Code = "user.config.serve_reserved"

	// A serve dataset breaks a rule the schema cannot state: a name, a
	// param, a grain, or a placeholder its SQL uses.
	CodeConfigServeDataset Code = "user.config.serve_dataset"

	// A rollups file breaks a rule the schema cannot state: a grain, a
	// dimension set, a measure, or a served dataset's bounds.
	CodeConfigRollup Code = "user.config.rollup"

	// A file generated from a rollups file differs from what the file
	// generates now: the migration or a serve dataset was edited by hand, or
	// the declaration changed without regenerating them.
	CodeConfigRollupDrift Code = "user.config.rollup_drift"

	// A rollups file changed in a way that would corrupt the rows its tables
	// already hold: a measure, a dimension set's dimensions, a grain's from,
	// or the source. Also a table that exists with other columns than the
	// file declares, because another declaration built it.
	CodeConfigRollupChange Code = "user.config.rollup_change"

	// Data: the messages themselves, as opposed to the pipeline definition.
	// A malformed record is the producer's problem, never ours.
	CodeDataMalformed Code = "user.data.malformed"
	CodeDataInvalid   Code = "user.data.invalid"

	// SQL: the handler's query and the schema it binds against.
	CodeSQLBindFailed      Code = "user.sql.bind_failed"
	CodeSQLTypeUnsupported Code = "user.sql.type_unsupported"
	CodeSQLParseFailed     Code = "user.sql.parse_failed"
	CodeSQLInvalid         Code = "user.sql.invalid"

	// Source and sink configuration the user got wrong.
	CodeSourceSecurityInvalid Code = "user.source.security_invalid"
	CodeSourceInvalid         Code = "user.source.invalid"
	CodeSinkInvalid           Code = "user.sink.invalid"
	CodeSinkTypeUnsupported   Code = "user.sink.type_unsupported"

	// A value the sink's client could not encode for the destination column.
	// The value fails identically on every attempt, so the retry ladder does
	// not retry it (#233).
	CodeSinkEncodeFailed Code = "user.sink.encode_failed"

	// Source and sink failures that are not the user's doing.
	CodeSourceUnreachable Code = "system.source.unreachable"
	CodeSourceInternal    Code = "system.source.internal"
	CodeSinkUnreachable   Code = "system.sink.unreachable"
	CodeSinkWriteFailed   Code = "system.sink.write_failed"
	CodeSinkInternal      Code = "system.sink.internal"

	// Durable state and the offsets committed with it.
	CodeStateCorrupt      Code = "system.state.corrupt"
	CodeStateCommitFailed Code = "system.state.commit_failed"
	// CodeProgressWriteFailed is the liveness row failing to write on a
	// pipeline with no state path, where the write autocommits by itself: the
	// batch is not rolled back and the pipeline keeps running. It matters
	// because the idle close acts on what this row confirms, so while the
	// write fails no window closes on idleness. With a state path the same
	// failure fails the batch's commit, which rolls back and is reported as
	// CodeStateCommitFailed instead.
	CodeProgressWriteFailed Code = "system.state.progress_write_failed"
	CodeStateInternal       Code = "system.state.internal"

	// Batch orchestration.
	CodeBatchInternal Code = "system.batch.internal"

	// Lifecycle: the process stopping. A drain that ran out of time is not a
	// sink failure and not a user error. Nothing unwritten was committed, so
	// nothing is lost; the code says the tail was replayed rather than
	// written.
	CodeDrainIncomplete   Code = "system.lifecycle.drain_incomplete"
	CodeLifecycleInternal Code = "system.lifecycle.internal"

	// The rollup daemon and install. A domain of its own, so a supervisor
	// can tell the rollup process's failures from a pipeline's.
	CodeRollupInternal Code = "system.rollup.internal"
	// The rollup store refused or dropped the connection. Retryable: the
	// database may come back.
	CodeRollupUnreachable Code = "system.rollup.unreachable"

	// The last resort. CodeOf returns it for an error carrying no code, so an
	// unclassified failure still reports as ours rather than the user's.
	CodeInternalUnexpected Code = "system.internal.unexpected"
)

// Definition documents one code. Action is what the operator should do, and
// it is the reason this registry exists rather than a bare list of constants:
// a code with no recommended action is not usable by the people who see it.
type Definition struct {
	Code    Code
	Summary string
	Action  string
}

var registry = map[Code]Definition{
	CodeConfigNotFound: {
		CodeConfigNotFound,
		"The config file does not exist at the given path.",
		"Check the path passed to `sqlflow run`.",
	},
	CodeConfigParseFailed: {
		CodeConfigParseFailed,
		"The config file is not valid YAML, or its template failed to render.",
		"Run `sqlflow config validate` to see the position of the problem.",
	},
	CodeConfigInvalid: {
		CodeConfigInvalid,
		"The config is well-formed but asks for something impossible.",
		"Read the message for the offending key, then correct it.",
	},
	CodeConfigTemplateUndefined: {
		CodeConfigTemplateUndefined,
		"The config reads a template variable that nothing defines. It renders as an empty string.",
		"Define the variable, or correct its name. `sqlflow validate` lists every variable the config reads and every one you supplied.",
	},
	CodeConfigServeReserved: {
		CodeConfigServeReserved,
		"A serve config sets a policy this version parses and does not enforce.",
		"Remove the key or set it to zero. The message names the key.",
	},
	CodeConfigServeDataset: {
		CodeConfigServeDataset,
		"A serve dataset breaks a rule: its name, its params, its grains, or the placeholders its SQL uses.",
		"Read the message for the dataset, grain and rule, then correct the dataset.",
	},
	CodeConfigRollup: {
		CodeConfigRollup,
		"A rollups file breaks a rule: a grain, a dimension set, a measure, or a served dataset's bounds.",
		"Read the message for the rollup and the rule, then correct the declaration.",
	},
	CodeConfigRollupDrift: {
		CodeConfigRollupDrift,
		"A migration or serve dataset generated from a rollups file differs from what the file generates now.",
		"Regenerate it with `sqlflow rollup ddl` or `sqlflow rollup serve` instead of editing it by hand.",
	},
	CodeRollupInternal: {
		CodeRollupInternal,
		"A rollup command failed on a database error it could not classify. Nothing it began was committed.",
		"Read the wrapped database error. Retry once the database is healthy; if it repeats, report it with the message.",
	},
	CodeRollupUnreachable: {
		CodeRollupUnreachable,
		"A rollup command could not connect to the database in store.postgres.dsn.",
		"Check that the database is up and reachable from this host, and that the dsn's host, port, user and password are right. The message names the host and the database, never the password.",
	},
	CodeConfigRollupChange: {
		CodeConfigRollupChange,
		"A rollups file changed in a way that would corrupt the rows its tables already hold, or a table exists with other columns than the file declares.",
		"Declare a new dimension set or rollup for the new shape, and drop the old tables by hand once nothing reads them. The message names the change.",
	},
	CodeDataMalformed: {
		CodeDataMalformed,
		"A message could not be parsed.",
		"Fix the producer, or set pipeline.on_error to dlq to divert bad records and keep running.",
	},
	CodeDataInvalid: {
		CodeDataInvalid,
		"A message is unusable for a reason the specific codes do not cover.",
		"Inspect the record. Set pipeline.on_error to dlq to collect them.",
	},
	CodeSQLBindFailed: {
		CodeSQLBindFailed,
		"The handler SQL did not bind against the inferred schema.",
		"Check column names and types against a sample message.",
	},
	CodeSQLTypeUnsupported: {
		CodeSQLTypeUnsupported,
		"A message field has a type the handler cannot convert.",
		"Cast the field in SQL, or pin a structured schema for the topic.",
	},
	CodeSQLParseFailed: {
		CodeSQLParseFailed,
		"A SQL statement in the config does not parse.",
		"Correct the statement. The position names the line it starts on.",
	},
	CodeSQLInvalid: {
		CodeSQLInvalid,
		"The handler SQL is wrong in a way the specific codes do not cover.",
		"Read the message, then correct the query.",
	},
	CodeSourceSecurityInvalid: {
		CodeSourceSecurityInvalid,
		"The source's TLS or SASL configuration is wrong, or its certificate files cannot be read.",
		"Check security_protocol, the sasl block, and that every ssl path exists and is readable by the pipeline's user.",
	},
	CodeSourceInvalid: {
		CodeSourceInvalid,
		"The source configuration is incomplete or contradictory.",
		"Correct the source block for the configured source type.",
	},
	CodeSinkInvalid: {
		CodeSinkInvalid,
		"The sink configuration is incomplete or contradictory.",
		"Correct the sink block. A missing required field is the usual cause.",
	},
	CodeSinkTypeUnsupported: {
		CodeSinkTypeUnsupported,
		"A result column has an Arrow type the sink cannot convert.",
		"Cast the column in the handler SQL to a type the sink's type table declares.",
	},
	CodeSinkEncodeFailed: {
		CodeSinkEncodeFailed,
		"The sink's client could not encode a result value for the destination column. It fails the same way on every attempt and is not retried.",
		"Cast or format the column in the handler SQL to match the destination column's type. The message names the column and the value.",
	},
	CodeSourceUnreachable: {
		CodeSourceUnreachable,
		"The source refused a connection or stopped answering.",
		"Check the broker or endpoint, then the network path and credentials.",
	},
	CodeSourceInternal: {
		CodeSourceInternal,
		"The source failed for a reason the specific codes do not cover.",
		"Report it with the surrounding log lines.",
	},
	CodeSinkUnreachable: {
		CodeSinkUnreachable,
		"The sink refused a connection or stopped answering.",
		"Check the destination, then the network path and credentials.",
	},
	CodeSinkWriteFailed: {
		CodeSinkWriteFailed,
		"The sink accepted a connection and rejected the write.",
		"Check the destination schema against the pipeline's output columns.",
	},
	CodeSinkInternal: {
		CodeSinkInternal,
		"The sink failed for a reason the specific codes do not cover.",
		"Report it with the surrounding log lines.",
	},
	CodeStateCorrupt: {
		CodeStateCorrupt,
		"The state file exists but does not carry the expected schema.",
		"Do not delete it. Preserve it and report, then start from a new path to resume service.",
	},
	CodeStateCommitFailed: {
		CodeStateCommitFailed,
		"The state transaction could not commit, so its batch rolled back.",
		"Check disk space and permissions on the state path.",
	},
	CodeProgressWriteFailed: {
		CodeProgressWriteFailed,
		"The liveness row could not be written on a pipeline with no state path, so last_arrival is stale.",
		"The batch is unaffected: without a state path the write commits by itself. While this persists, a window with idle_close_seconds does not close on idleness, because the engine cannot confirm the stream is quiet; its buckets wait. With a state path this failure is reported as system.state.commit_failed.",
	},
	CodeStateInternal: {
		CodeStateInternal,
		"Durable state failed for a reason the specific codes do not cover.",
		"Report it with the surrounding log lines.",
	},
	CodeBatchInternal: {
		CodeBatchInternal,
		"A batch failed for a reason the specific codes do not cover.",
		"Report it with the surrounding log lines.",
	},
	CodeDrainIncomplete: {
		CodeDrainIncomplete,
		"The drain deadline passed before the buffered batch or the closed windows were written.",
		"Nothing is lost: what was not written was not committed, and the next start replays it. Raise pipeline.drain_deadline_seconds if the sink needs longer, or check the sink.",
	},
	CodeLifecycleInternal: {
		CodeLifecycleInternal,
		"Starting or stopping the process failed for a reason the specific codes do not cover.",
		"Report it with the surrounding log lines.",
	},
	CodeInternalUnexpected: {
		CodeInternalUnexpected,
		"An error reached the boundary carrying no code.",
		"Report it. Every path out should carry a code, so this is a gap in ours.",
	},
}

// Lookup returns the definition for a code. A caller that gets ok == false is
// holding a code this build does not know, which is normal when a newer
// component produced it: fall back to the class prefix.
func Lookup(c Code) (Definition, bool) {
	d, ok := registry[c]
	return d, ok
}

// All returns every registered definition, ordered by code. Documentation and
// the append-only test both read it.
func All() []Definition {
	out := make([]Definition, 0, len(registry))
	for _, d := range registry {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}
