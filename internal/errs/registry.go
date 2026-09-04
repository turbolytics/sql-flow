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

	// Data: the messages themselves, as opposed to the pipeline definition.
	// A malformed record is the producer's problem, never ours.
	CodeDataMalformed Code = "user.data.malformed"
	CodeDataInvalid   Code = "user.data.invalid"

	// SQL: the handler's query and the schema it binds against.
	CodeSQLBindFailed      Code = "user.sql.bind_failed"
	CodeSQLTypeUnsupported Code = "user.sql.type_unsupported"
	CodeSQLInvalid         Code = "user.sql.invalid"

	// Source and sink configuration the user got wrong.
	CodeSourceSecurityInvalid Code = "user.source.security_invalid"
	CodeSourceInvalid         Code = "user.source.invalid"
	CodeSinkInvalid           Code = "user.sink.invalid"

	// Source and sink failures that are not the user's doing.
	CodeSourceUnreachable Code = "system.source.unreachable"
	CodeSourceInternal    Code = "system.source.internal"
	CodeSinkUnreachable   Code = "system.sink.unreachable"
	CodeSinkWriteFailed   Code = "system.sink.write_failed"
	CodeSinkInternal      Code = "system.sink.internal"

	// Durable state and the offsets committed with it.
	CodeStateCorrupt      Code = "system.state.corrupt"
	CodeStateCommitFailed Code = "system.state.commit_failed"
	CodeStateInternal     Code = "system.state.internal"

	// Batch orchestration.
	CodeBatchInternal Code = "system.batch.internal"

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
