package run

import (
	"strconv"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkSchemaRegistry refuses, before anything is dialed, a config validate
// would refuse, for a config that never went through validate. The first
// violation is the error; validate lists them all.
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
	p := &conf.Pipeline
	if src := p.Source.Kafka; src != nil && src.Value.RegistryBacked() {
		return errs.New(errs.CodeConfigInvalid,
			"pipeline.source.kafka.value.format: this build reads json only; %s arrives with #300",
			src.Value.ResolvedFormat())
	}
	sink := func(s config.Sink, key string) error {
		if s.Kafka == nil || !s.Kafka.Value.RegistryBacked() {
			return nil
		}
		return errs.New(errs.CodeConfigInvalid,
			"%s.kafka.value.format: this build writes json only; %s arrives with #304",
			key, s.Kafka.Value.ResolvedFormat())
	}
	if err := sink(p.Sink, "pipeline.sink"); err != nil {
		return err
	}
	if p.OnError != nil && p.OnError.DLQ != nil {
		if err := sink(*p.OnError.DLQ, "pipeline.on_error.dlq"); err != nil {
			return err
		}
	}
	if conf.Tables != nil {
		for i, table := range conf.Tables.SQL {
			if table.Window == nil {
				continue
			}
			if err := sink(table.Window.Sink, "tables.sql."+strconv.Itoa(i)+".window.sink"); err != nil {
				return err
			}
		}
	}
	return nil
}
