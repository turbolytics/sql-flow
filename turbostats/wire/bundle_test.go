package wire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// A section's presence says what the process does. A run bundle carries no
// serve key at all, not an empty one.
func TestBundle_AbsentSectionsLeaveNoKey(t *testing.T) {
	b := Bundle{V: Version, Pipeline: &Pipeline{MessageCount: 1}}
	raw := mustMarshal(t, b)
	for _, key := range []string{`"serve"`, `"commands"`, `"exit"`,
		`"last_activity_at"`, `"interval_seconds"`} {
		if strings.Contains(raw, key) {
			t.Fatalf("bundle carries %s: %s", key, raw)
		}
	}
	if !strings.Contains(raw, `"pipeline"`) {
		t.Fatalf("bundle lost its pipeline section: %s", raw)
	}
}

// The activity timestamps live inside their sections. Only the denormalized
// copy sits at the top level.
func TestBundle_SectionTimestampsLiveInTheirSections(t *testing.T) {
	at := time.Date(2026, 9, 19, 19, 59, 58, 0, time.UTC)
	b := Bundle{
		V:              Version,
		LastActivityAt: &at,
		Pipeline:       &Pipeline{LastMessageAt: &at},
		Serve:          &Serve{LastRequestAt: &at},
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(mustMarshal(t, b)), &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["last_message_at"]; ok {
		t.Fatal("last_message_at is still at the top level")
	}
	if !strings.Contains(string(doc["pipeline"]), "last_message_at") {
		t.Fatalf("pipeline lost last_message_at: %s", doc["pipeline"])
	}
	if !strings.Contains(string(doc["serve"]), "last_request_at") {
		t.Fatalf("serve lost last_request_at: %s", doc["serve"])
	}
	if _, ok := doc["last_activity_at"]; !ok {
		t.Fatal("last_activity_at is missing")
	}
}

// An absent cache and an empty cache are different facts.
func TestServe_CacheIsAbsentWithoutOne(t *testing.T) {
	raw := mustMarshal(t, Serve{RequestCount: 3})
	if strings.Contains(raw, "cache") {
		t.Fatalf("serve carries a cache it does not have: %s", raw)
	}
}

// instance.pipeline became instance.name: serve has no pipeline.
func TestInstance_NameReplacesPipeline(t *testing.T) {
	raw := mustMarshal(t, Instance{Name: "bluesky-firehose"})
	if !strings.Contains(raw, `"name":"bluesky-firehose"`) {
		t.Fatalf("instance has no name: %s", raw)
	}
	if strings.Contains(raw, `"pipeline"`) {
		t.Fatalf("instance still carries pipeline: %s", raw)
	}
}

// A reader ignores what it does not know. This is the rule that keeps a new
// section, and the day commands ship, from breaking a deployed reader.
func TestBundleAndResponse_IgnoreUnknownFields(t *testing.T) {
	var b Bundle
	err := json.Unmarshal([]byte(`{"v":1,"future_section":{"x":1},"process":{"new_field":2}}`), &b)
	if err != nil {
		t.Fatal(err)
	}
	var r Response
	err = json.Unmarshal([]byte(`{"v":1,"commands":[{"id":"c1","verb":"x"}],"later":true}`), &r)
	if err != nil {
		t.Fatal(err)
	}
	if r.V != 1 || len(r.Commands) != 1 {
		t.Fatalf("response parsed wrong: %+v", r)
	}
}

// A process a second old has an uptime of 0, and a receiver must not read
// that as an older engine that sends none. So both durations are pointers:
// absent means absent, and zero is a reading.
func TestBundle_ZeroDurationsArePresentAndNilIsAbsent(t *testing.T) {
	zero := int64(0)
	with := mustMarshal(t, Bundle{V: Version,
		Process: Process{UptimeSeconds: &zero}, IdleSeconds: &zero})
	for _, want := range []string{`"uptime_seconds":0`, `"idle_seconds":0`} {
		if !strings.Contains(with, want) {
			t.Errorf("a zero duration is missing %s: %s", want, with)
		}
	}

	without := mustMarshal(t, Bundle{V: Version})
	for _, absent := range []string{"uptime_seconds", "idle_seconds"} {
		if strings.Contains(without, absent) {
			t.Errorf("an absent duration is present as %s: %s", absent, without)
		}
	}
}

// The wire's buckets are the contract's, not the engine's: nine counts
// against eight boundaries, so a receiver reads a distribution without being
// told the boundaries.
func TestDuration_HasNineCountsAgainstEightBounds(t *testing.T) {
	if len(DurationBounds) != 8 {
		t.Fatalf("DurationBounds has %d entries", len(DurationBounds))
	}
	for i := 1; i < len(DurationBounds); i++ {
		if DurationBounds[i] <= DurationBounds[i-1] {
			t.Fatalf("DurationBounds is not ascending at %d", i)
		}
	}
	d := Duration{Count: 3, SumSeconds: 0.5, MinSeconds: 0.1, MaxSeconds: 0.3,
		Buckets: make([]uint64, len(DurationBounds)+1)}
	raw := mustMarshal(t, d)
	for _, want := range []string{`"count":3`, `"sum_seconds":0.5`,
		`"min_seconds":0.1`, `"max_seconds":0.3`, `"buckets":[0,0,0,0,0,0,0,0,0]`} {
		if !strings.Contains(raw, want) {
			t.Errorf("a duration is missing %s: %s", want, raw)
		}
	}
}

// A section that measured nothing sends no duration at all. Zeros would say
// the work happened and took no time.
func TestPipeline_DurationsAndErrorsAreAbsentUntilMeasured(t *testing.T) {
	raw := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{}})
	for _, absent := range []string{"duration", "source_error_count",
		"handler_error_count", "sink_error_count", "state_error_count",
		"dlq_rows", "last_error_code", "last_error_at", "recv_wait_seconds"} {
		if strings.Contains(raw, absent) {
			t.Errorf("an unmeasured pipeline carries %s: %s", absent, raw)
		}
	}
}

// Zero is a reading: a pipeline that has failed nothing reports zero
// errors, which is different from one whose engine does not count them.
func TestPipeline_ZeroErrorsArePresent(t *testing.T) {
	zero := int64(0)
	raw := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{SinkErrorCount: &zero}})
	if !strings.Contains(raw, `"sink_error_count":0`) {
		t.Errorf("a zero error count is absent: %s", raw)
	}
}
