package run

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkSchemaRegistry refuses, before anything is dialed, a config validate
// would refuse, for a config that never went through validate. The first
// violation is the error; validate lists them all.
//
// The order is load-bearing. refuseUnshippedFormats assumes the rules
// passed: run it first and a typo such as format: avr, which is not
// registry-backed, slips through it, while a valid avro config with a
// broken auth block is told "arrives with #300" instead of what is wrong.
func checkSchemaRegistry(conf *config.Conf) error {
	if vs := conf.CheckSchemaRegistry(); len(vs) > 0 {
		return errs.New(vs[0].Code, "%s: %s", vs[0].Key(), vs[0].Message)
	}
	return refuseUnshippedFormats(conf)
}

// refuseUnshippedFormats is the gate between this config surface and the
// engine behind it. Delete the source half when #300 wires the typed
// handler, and the sink half when #304 wires the encoders. Until then a
// registry format has to fail here: the source would read framed bytes as
// JSON and fail every record as user.data.malformed, and the sink would
// write JSON under a format the config promised was Avro.
func refuseUnshippedFormats(conf *config.Conf) error {
	if src := conf.Pipeline.Source.Kafka; src != nil && src.Value.RegistryBacked() {
		return errs.New(errs.CodeConfigInvalid,
			"pipeline.source.kafka.value.format: this build reads json only; %s arrives with #300",
			src.Value.ResolvedFormat())
	}
	for path, s := range conf.EachSink() {
		if s.Kafka == nil || !s.Kafka.Value.RegistryBacked() {
			continue
		}
		return errs.New(errs.CodeConfigInvalid,
			"%s.kafka.value.format: this build writes json only; %s arrives with #304",
			strings.Join(path, "."), s.Kafka.Value.ResolvedFormat())
	}
	return nil
}
