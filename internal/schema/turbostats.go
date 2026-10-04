package schema

import (
	"fmt"

	"github.com/invopop/jsonschema"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
)

// wirePkg is the import path the reflector keys the contract's doc
// comments by.
const wirePkg = "github.com/turbolytics/sql-flow/turbostats/wire"

// GenerateTurboStatsBundle reflects wire.Bundle into the bundle schema.
//
// wireDir is the path to turbostats/wire, which the reflector reads doc
// comments from: the contract's prose becomes the schema's descriptions.
func GenerateTurboStatsBundle(wireDir string) ([]byte, error) {
	return generateWire(wireDir, &wire.Bundle{}, tsschema.BundleID)
}

// GenerateTurboStatsResponse reflects wire.Response into the response
// schema.
func GenerateTurboStatsResponse(wireDir string) ([]byte, error) {
	return generateWire(wireDir, &wire.Response{}, tsschema.ResponseID)
}

func generateWire(wireDir string, v any, id string) ([]byte, error) {
	r := &jsonschema.Reflector{
		// Every reader ignores unknown sections and fields. The config
		// schemas reject unknown keys; this one must not, or a validator
		// deployed today rejects the engine released tomorrow.
		AllowAdditionalProperties: true,
		// One document, read by people and by reporters in other languages.
		ExpandedStruct: true,
		DoNotReference: true,
	}
	if err := r.AddGoComments(wirePkg, wireDir); err != nil {
		return nil, fmt.Errorf("reading contract doc comments from %s: %w", wireDir, err)
	}
	s := r.Reflect(v)
	s.ID = jsonschema.ID(id)
	s.Version = "https://json-schema.org/draft/2020-12/schema"
	pinBuckets(s)
	return encode(s)
}

// pinBuckets fixes every duration's buckets at the contract's length.
//
// The length is read from wire.DurationBounds rather than written here, the
// way the config generator reads its enums from the registries. A copied
// number would drift the day a boundary is added.
func pinBuckets(s *jsonschema.Schema) {
	n := uint64(len(wire.DurationBounds) + 1)
	walkSchema(s, func(node *jsonschema.Schema) {
		if node.Properties == nil {
			return
		}
		b, ok := node.Properties.Get("buckets")
		if !ok {
			return
		}
		if _, isDuration := node.Properties.Get("sum_seconds"); !isDuration {
			return
		}
		b.MinItems = &n
		b.MaxItems = &n
	})
}

// walkSchema calls fn on s and every schema beneath it.
func walkSchema(s *jsonschema.Schema, fn func(*jsonschema.Schema)) {
	if s == nil {
		return
	}
	fn(s)
	if s.Properties != nil {
		for p := s.Properties.Oldest(); p != nil; p = p.Next() {
			walkSchema(p.Value, fn)
		}
	}
	walkSchema(s.Items, fn)
	walkSchema(s.AdditionalProperties, fn)
}
