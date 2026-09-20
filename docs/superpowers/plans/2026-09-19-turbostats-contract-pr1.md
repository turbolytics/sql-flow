# TurboStats Contract Amendment, PR 1: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the amended TurboStats wire contract: a public `turbostats/wire` package with the sectioned bundle and Ed25519 signing, a `serve` section fed by flat totals, and `GET /turbostats/v1` on `sqlflow serve`.

**Architecture:** The wire types and signing functions move to a public package that the private control plane imports. `internal/turbostats` stays the collector and aliases the wire types. `Collect` takes a `Source` whose optional `Pipeline` and `Serve` members decide which sections the bundle carries. `sqlflow serve` always builds a meter provider with a manual reader, and the Prometheus exporter becomes a second reader.

**Tech Stack:** Go standard library (`crypto/ed25519`, `crypto/sha256`), OpenTelemetry metric SDK (`sdkmetric.ManualReader`), `github.com/zeebo/assert`, pytest with testcontainers for the release suite.

**Spec:** `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`. It amends `docs/superpowers/specs/2026-09-10-turbostats-v1-design.md`. Read both.

**Out of this plan:** the reporter, the `pipeline.turbostats` and `serve.turbostats` config blocks, and the final bundle on drain. Those are PR 2.

## Global Constraints

- `turbostats/wire` imports only the Go standard library.
- Media type stays `application/vnd.turbolytics.turbostats.v1+json`. `"v"` stays `1`.
- A bundle marshals under 1024 bytes. One test holds a full `run` bundle under it, and one holds a full `serve` bundle.
- Every number in a bundle section is read from a dimensionless series, or from a stats function at collect time. `Collect` sums nothing and filters nothing.
- Instrument names must not end in `_total`. The Prometheus exporter appends it.
- Canonical signing string: `v1\n{method}\n{path}\n{timestamp}\n{sha256_hex(body)}`. Timestamp is Unix seconds. Path excludes the query.
- Key id: the first 16 hex characters of the SHA-256 of the 32-byte public key.
- Credential string: `sfc_` plus the unpadded base64url encoding of the 32-byte seed.
- Headers: `X-Turbostats-Key-Id`, `X-Turbostats-Timestamp`, `X-Turbostats-Signature` (standard base64).
- `/turbostats/v1` stays off by default in both commands. A process does not answer on a route unless told to.
- All prose, including code comments and commit messages, follows `CLAUDE.md`: active voice, short sentences, comments explain why.
- Commit messages name the defect, the fix, and the evidence, and state what breaks if the change is wrong. No attribution lines and no session links.
- Every new Go test calls `coverage.Covers(t, "<feature id>")` with an id from `docs/coverage/features.yml`.
- Work on a branch. Never commit to `main`.

## File Structure

| File | Responsibility |
|---|---|
| `turbostats/wire/bundle.go` (create) | The bundle and response types, the media type, the version |
| `turbostats/wire/sign.go` (create) | Canonical string, key id, credential string, sign, verify, headers |
| `turbostats/wire/bundle_test.go`, `sign_test.go` (create) | JSON shape tests and vector tests |
| `turbostats/wire/testdata/vectors.json` (create) | The pinned seed, request, key id, and signature |
| `internal/turbostats/bundle.go` (modify) | Aliases to the wire types, plus `Static` and `Source` |
| `internal/turbostats/collect.go` (modify) | Builds sections from a `Source` |
| `internal/serve/metrics.go` (modify) | Always-present provider, flat totals |
| `internal/serve/server.go`, `http.go` (modify) | Provider wiring, the 5xx signal, the `/turbostats/v1` route |
| `internal/cli/serve/serve.go` (modify) | The `--turbostats` flag and the `Static` |
| `internal/cli/run/metrics.go`, `root.go` (modify) | The new `Collect` call |
| `tests/release/test_image.py` (modify) | The moved field, and a `serve` bundle test |
| `docs/coverage/features.yml`, `CHANGELOG.md`, the 2026-09-10 spec (modify) | Registry entry, release note, pointer to the amendment |

---

### Task 1: The wire bundle types

**Files:**
- Create: `turbostats/wire/bundle.go`
- Test: `turbostats/wire/bundle_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `wire.Bundle`, `wire.Instance`, `wire.Process`, `wire.Pipeline`, `wire.Serve`, `wire.ServeCache`, `wire.Exit`, `wire.Response`, `wire.MediaType`, `wire.Version`.

- [ ] **Step 1: Create the branch**

```bash
git switch main && git pull && git switch -c turbostats/contract-pr1
```

- [ ] **Step 2: Write the failing test**

Create `turbostats/wire/bundle_test.go`:

```go
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
```

- [ ] **Step 3: Run the test and confirm it fails**

Run: `go test ./turbostats/wire/...`
Expected: FAIL. The build reports `undefined: Bundle`.

- [ ] **Step 4: Write the types**

Create `turbostats/wire/bundle.go`:

```go
// Package wire is the TurboStats contract: the document a sqlflow process
// reports about itself, the response a control plane answers with, and the
// signature that authenticates the request.
//
// It is public so a control plane outside this module builds against the same
// types the engine does. It imports only the standard library, so importing
// it pulls in nothing of the engine.
//
// The contract is
// docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md.
// Three rules from it govern every change here:
//
//   - Sections are optional top-level objects. Presence says what the process
//     does, so there is no kind field.
//   - Every reader ignores unknown sections and unknown fields. A new one is
//     additive and stays v1.
//   - A removed or renamed field is v2, at a new path and media type.
package wire

import (
	"encoding/json"
	"time"
)

// MediaType names the version on the wire, so v1 and v2 are distinguishable
// without parsing.
const MediaType = "application/vnd.turbolytics.turbostats.v1+json"

// Version is the document version, carried in the body so a bundle stored or
// forwarded without its URL still says what it is.
const Version = 1

// Bundle is one report.
type Bundle struct {
	V      int       `json:"v"`
	SentAt time.Time `json:"sent_at"`
	// IntervalSeconds is the reporter's configured interval. A receiver needs
	// it to tell a late heartbeat from a normal one. Absent when no reporter
	// is configured.
	IntervalSeconds int `json:"interval_seconds,omitempty"`
	// LastActivityAt is the latest of the section activity timestamps. It is
	// a denormalized copy: the section fields are the source. A receiver
	// reads it for staleness without knowing which sections exist.
	LastActivityAt *time.Time `json:"last_activity_at,omitempty"`

	Instance Instance `json:"instance"`
	Process  Process  `json:"process"`

	// Pipeline is present when the process runs a consume loop.
	Pipeline *Pipeline `json:"pipeline,omitempty"`
	// Serve is present when the process answers dataset requests.
	Serve *Serve `json:"serve,omitempty"`

	// Commands is a reserved name. v1 never populates it; a later spec
	// defines command status.
	Commands []json.RawMessage `json:"commands,omitempty"`

	// Exit is present only in the last bundle a clean shutdown sends.
	Exit *Exit `json:"exit,omitempty"`
}

// Instance is what the operator and the build said this process is.
type Instance struct {
	// ID is the operator's name for the instance. Empty until a reporter is
	// configured, which requires it.
	ID string `json:"id,omitempty"`
	// Name is pipeline.name under run and serve.name under serve.
	Name       string `json:"name,omitempty"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	Arch       string `json:"arch"`
	ConfigHash string `json:"config_hash"`
}

// Process is what the runtime and the kernel say about this process.
type Process struct {
	StartedAt  time.Time `json:"started_at"`
	RSSBytes   int64     `json:"rss_bytes"`
	Goroutines int       `json:"goroutines"`
}

// Pipeline carries the consume loop's totals since Process.StartedAt.
type Pipeline struct {
	MessageCount     int64 `json:"message_count"`
	HandlerRowsRead  int64 `json:"handler_rows_read"`
	ErrorCount       int64 `json:"error_count"`
	SinkFlushCount   int64 `json:"sink_flush_count"`
	SinkRowsAccepted int64 `json:"sink_rows_accepted"`
	SinkRowsWritten  int64 `json:"sink_rows_written"`
	StateCommitCount int64 `json:"state_commit_count"`
	// A pointer so a pipeline with no state path omits the field: absent
	// state and empty state are different facts.
	StateDBSizeBytes *int64 `json:"state_db_size_bytes,omitempty"`
	// Absent until the pipeline receives anything, because zero is not a time.
	LastMessageAt *time.Time `json:"last_message_at,omitempty"`
}

// Serve carries the dataset API's totals since Process.StartedAt.
type Serve struct {
	RequestCount int64 `json:"request_count"`
	// RequestErrorCount counts 5xx answers only. A 4xx is the caller's error.
	RequestErrorCount int64 `json:"request_error_count"`
	SessionsInUse     int   `json:"sessions_in_use"`
	SessionsTotal     int   `json:"sessions_total"`
	// Absent until the server answers a dataset request.
	LastRequestAt *time.Time `json:"last_request_at,omitempty"`
	// Cache is absent on a server where no dataset opted into the cache.
	Cache *ServeCache `json:"cache,omitempty"`
}

// ServeCache carries the response cache's totals and its current size.
type ServeCache struct {
	HitCount      int64 `json:"hit_count"`
	MissCount     int64 `json:"miss_count"`
	SharedCount   int64 `json:"shared_count"`
	EvictionCount int64 `json:"eviction_count"`
	Bytes         int64 `json:"bytes"`
	Entries       int   `json:"entries"`
}

// Exit is how a clean shutdown ended.
type Exit struct {
	Reason string `json:"reason"`
	Code   int    `json:"code"`
}

// Response is the body of every 2xx answer to a heartbeat.
//
// Commands is a reserved name. A v1 reporter ignores its contents, and must
// still parse a response that carries some: instances deployed today have to
// survive the day a control plane starts sending commands.
type Response struct {
	V        int               `json:"v"`
	Commands []json.RawMessage `json:"commands"`
}
```

- [ ] **Step 5: Run the test and confirm it passes**

Run: `go test ./turbostats/wire/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add turbostats/wire/bundle.go turbostats/wire/bundle_test.go
git commit -F - <<'EOF'
wire: the TurboStats bundle as a public, sectioned document

The bundle types live in internal/turbostats, so the private control plane
cannot import them, and they describe only a consume loop, so `sqlflow serve`
has nothing to report.

turbostats/wire holds the amended types. `pipeline` and `serve` are pointers,
so a section's presence says what the process does. `last_message_at` sits in
`pipeline`, `instance.pipeline` is `instance.name`, and `last_activity_at` is
the one denormalized field at the top level. The package imports only the
standard library.

Nothing uses the package yet. If the shapes are wrong, the tests that pin
absent sections and unknown-field tolerance are the guard.
EOF
```

---

### Task 2: Signing, key id, and the credential string

**Files:**
- Create: `turbostats/wire/sign.go`
- Create: `turbostats/wire/testdata/vectors.json`
- Test: `turbostats/wire/sign_test.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  - `const HeaderKeyID, HeaderTimestamp, HeaderSignature string`
  - `const CredentialPrefix = "sfc_"`, `const MaxClockSkew = 5 * time.Minute`
  - `func CanonicalString(method, path string, timestamp int64, body []byte) string`
  - `func KeyID(pub ed25519.PublicKey) string`
  - `func FormatCredential(seed []byte) (string, error)`
  - `func ParseCredential(s string) (ed25519.PrivateKey, error)`
  - `func Sign(priv ed25519.PrivateKey, method, path string, timestamp int64, body []byte) []byte`
  - `func Verify(pub ed25519.PublicKey, method, path string, timestamp int64, body, sig []byte) bool`
  - `func SignRequest(req *http.Request, priv ed25519.PrivateKey, body []byte, now time.Time)`
  - `func ParseHeaders(h http.Header) (keyID string, timestamp int64, sig []byte, err error)`

- [ ] **Step 1: Write the vectors**

These values were computed with Go's `crypto/ed25519`. Ed25519 signatures are deterministic, so they never change. The seed is the bytes `0x00` through `0x1f`.

Create `turbostats/wire/testdata/vectors.json`:

```json
{
  "seed_hex": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
  "credential": "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
  "public_key_hex": "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8",
  "key_id": "56475aa75463474c",
  "method": "POST",
  "path": "/v1/turbostats",
  "timestamp": 1789848000,
  "body": "{\"v\":1}",
  "body_sha256": "afbf9d0f3560b0fd7795e81c42a0a79ee6b6fc67e064f77826aee642cad28d91",
  "signature_b64": "ikTk4Yy0tf8pEHExJFgNqWg4MRJc7TioFHBiEfQsaDr+vky+kg2A7BJUl85niojmb4ZqDpul+Ia29WnbKnS9AQ=="
}
```

- [ ] **Step 2: Write the failing test**

Create `turbostats/wire/sign_test.go`:

```go
package wire

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type vectors struct {
	SeedHex      string `json:"seed_hex"`
	Credential   string `json:"credential"`
	PublicKeyHex string `json:"public_key_hex"`
	KeyID        string `json:"key_id"`
	Method       string `json:"method"`
	Path         string `json:"path"`
	Timestamp    int64  `json:"timestamp"`
	Body         string `json:"body"`
	BodySHA256   string `json:"body_sha256"`
	SignatureB64 string `json:"signature_b64"`
}

func loadVectors(t *testing.T) (vectors, ed25519.PrivateKey, ed25519.PublicKey, []byte) {
	t.Helper()
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	seed, err := hex.DecodeString(v.SeedHex)
	if err != nil {
		t.Fatal(err)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	sig, err := base64.StdEncoding.DecodeString(v.SignatureB64)
	if err != nil {
		t.Fatal(err)
	}
	return v, priv, priv.Public().(ed25519.PublicKey), sig
}

// The control plane derives the key id at registration and the instance
// derives it from its own key. If the two disagree, every heartbeat is
// rejected, so the vector pins it.
func TestKeyID_MatchesTheVector(t *testing.T) {
	v, _, pub, _ := loadVectors(t)
	if hex.EncodeToString(pub) != v.PublicKeyHex {
		t.Fatalf("public key is %x", pub)
	}
	if got := KeyID(pub); got != v.KeyID {
		t.Fatalf("KeyID is %q, want %q", got, v.KeyID)
	}
}

func TestCredential_RoundTripsTheVector(t *testing.T) {
	v, priv, _, _ := loadVectors(t)
	got, err := FormatCredential(priv.Seed())
	if err != nil {
		t.Fatal(err)
	}
	if got != v.Credential {
		t.Fatalf("FormatCredential is %q, want %q", got, v.Credential)
	}
	parsed, err := ParseCredential(v.Credential)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Equal(priv) {
		t.Fatal("ParseCredential rebuilt a different key")
	}
}

func TestParseCredential_RefusesWhatIsNotOne(t *testing.T) {
	for _, s := range []string{
		"",
		"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", // no prefix
		"sfc_",             // no seed
		"sfc_AAEC",         // short seed
		"sfc_not base64!!", // not base64url
	} {
		if _, err := ParseCredential(s); err == nil {
			t.Fatalf("ParseCredential(%q) did not fail", s)
		}
	}
	if _, err := FormatCredential([]byte{1, 2, 3}); err == nil {
		t.Fatal("FormatCredential accepted a short seed")
	}
}

func TestCanonicalString_IsTheFiveLines(t *testing.T) {
	v, _, _, _ := loadVectors(t)
	got := CanonicalString(v.Method, v.Path, v.Timestamp, []byte(v.Body))
	want := "v1\nPOST\n/v1/turbostats\n1789848000\n" + v.BodySHA256
	if got != want {
		t.Fatalf("canonical string is %q, want %q", got, want)
	}
}

func TestSign_MatchesTheVectorAndVerifies(t *testing.T) {
	v, priv, pub, want := loadVectors(t)
	got := Sign(priv, v.Method, v.Path, v.Timestamp, []byte(v.Body))
	if string(got) != string(want) {
		t.Fatalf("signature is %s", base64.StdEncoding.EncodeToString(got))
	}
	if !Verify(pub, v.Method, v.Path, v.Timestamp, []byte(v.Body), want) {
		t.Fatal("the vector's signature does not verify")
	}
}

// Each part of the canonical string is covered: change any one and the
// signature fails.
func TestVerify_RefusesAnyTamperedPart(t *testing.T) {
	v, _, pub, sig := loadVectors(t)
	body := []byte(v.Body)
	otherSeed := make([]byte, ed25519.SeedSize)
	otherSeed[0] = 0xff
	otherPub := ed25519.NewKeyFromSeed(otherSeed).Public().(ed25519.PublicKey)

	cases := map[string]bool{
		"tampered body":      Verify(pub, v.Method, v.Path, v.Timestamp, []byte(`{"v":2}`), sig),
		"tampered path":      Verify(pub, v.Method, "/v1/other", v.Timestamp, body, sig),
		"tampered method":    Verify(pub, "PUT", v.Path, v.Timestamp, body, sig),
		"tampered timestamp": Verify(pub, v.Method, v.Path, v.Timestamp+1, body, sig),
		"wrong key":          Verify(otherPub, v.Method, v.Path, v.Timestamp, body, sig),
		"short signature":    Verify(pub, v.Method, v.Path, v.Timestamp, body, sig[:10]),
		"short public key":   Verify(pub[:5], v.Method, v.Path, v.Timestamp, body, sig),
	}
	for name, verified := range cases {
		if verified {
			t.Fatalf("%s verified", name)
		}
	}
}

// The query is not part of the path: a proxy that appends one must not break
// the signature, and the receiver signs the same thing the sender did.
func TestSignRequest_SetsTheHeadersAndIgnoresTheQuery(t *testing.T) {
	v, priv, pub, want := loadVectors(t)
	body := []byte(v.Body)
	req, err := http.NewRequest(v.Method, "https://control.example"+v.Path+"?x=1", strings.NewReader(v.Body))
	if err != nil {
		t.Fatal(err)
	}
	SignRequest(req, priv, body, time.Unix(v.Timestamp, 0))

	keyID, ts, sig, err := ParseHeaders(req.Header)
	if err != nil {
		t.Fatal(err)
	}
	if keyID != v.KeyID || ts != v.Timestamp || string(sig) != string(want) {
		t.Fatalf("headers are %q %d %x", keyID, ts, sig)
	}
	if !Verify(pub, req.Method, req.URL.Path, ts, body, sig) {
		t.Fatal("the signed request does not verify")
	}
}

func TestParseHeaders_RefusesMissingOrMalformed(t *testing.T) {
	v, priv, _, _ := loadVectors(t)
	good := func() http.Header {
		req, _ := http.NewRequest(v.Method, "https://control.example"+v.Path, nil)
		SignRequest(req, priv, []byte(v.Body), time.Unix(v.Timestamp, 0))
		return req.Header
	}
	breakers := map[string]func(http.Header){
		"no key id":         func(h http.Header) { h.Del(HeaderKeyID) },
		"short key id":      func(h http.Header) { h.Set(HeaderKeyID, "abc") },
		"non-hex key id":    func(h http.Header) { h.Set(HeaderKeyID, "zzzzzzzzzzzzzzzz") },
		"no timestamp":      func(h http.Header) { h.Del(HeaderTimestamp) },
		"bad timestamp":     func(h http.Header) { h.Set(HeaderTimestamp, "yesterday") },
		"no signature":      func(h http.Header) { h.Del(HeaderSignature) },
		"bad signature":     func(h http.Header) { h.Set(HeaderSignature, "!!!") },
		"short signature":   func(h http.Header) { h.Set(HeaderSignature, "AAAA") },
	}
	for name, breakIt := range breakers {
		h := good()
		breakIt(h)
		if _, _, _, err := ParseHeaders(h); err == nil {
			t.Fatalf("%s: ParseHeaders did not fail", name)
		}
	}
}
```

- [ ] **Step 3: Run the test and confirm it fails**

Run: `go test ./turbostats/wire/...`
Expected: FAIL. The build reports `undefined: KeyID`.

- [ ] **Step 4: Write the implementation**

Create `turbostats/wire/sign.go`:

```go
package wire

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The three headers a signed heartbeat carries.
const (
	HeaderKeyID     = "X-Turbostats-Key-Id"
	HeaderTimestamp = "X-Turbostats-Timestamp"
	HeaderSignature = "X-Turbostats-Signature"
)

// CredentialPrefix marks a credential string, so a scanner can find a leaked
// one and an operator can tell it from any other secret.
const CredentialPrefix = "sfc_"

// MaxClockSkew is how far a request's timestamp may sit from the receiver's
// clock. The replay defense proper is the bundle's sent_at, which the
// receiver checks against the last one it stored; this bounds how long a
// captured request is even a candidate.
const MaxClockSkew = 5 * time.Minute

// keyIDLen is 16 hex characters: 64 bits of the public key's SHA-256. It
// names a key within one control plane, where a collision needs billions of
// keys, and the signature is what authenticates.
const keyIDLen = 16

// CanonicalString is the text a signature covers.
//
// path excludes the query, so a proxy that appends one does not break the
// signature. The body is hashed rather than included so the string stays five
// short lines that a shell can rebuild.
func CanonicalString(method, path string, timestamp int64, body []byte) string {
	sum := sha256.Sum256(body)
	return "v1\n" + method + "\n" + path + "\n" +
		strconv.FormatInt(timestamp, 10) + "\n" + hex.EncodeToString(sum[:])
}

// KeyID is a fingerprint of the public key.
//
// Both sides derive it without talking to each other. That is what lets an
// operator generate a key on a device and register only its public half: the
// instance already knows the id the control plane will file the key under.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])[:keyIDLen]
}

// FormatCredential renders a seed as the one string an operator holds.
func FormatCredential(seed []byte) (string, error) {
	if len(seed) != ed25519.SeedSize {
		return "", fmt.Errorf("wire: a seed is %d bytes, not %d", ed25519.SeedSize, len(seed))
	}
	return CredentialPrefix + base64.RawURLEncoding.EncodeToString(seed), nil
}

// ParseCredential rebuilds the private key from a credential string. The
// public key and the key id follow from it, so the string carries nothing
// else.
func ParseCredential(s string) (ed25519.PrivateKey, error) {
	rest, ok := strings.CutPrefix(s, CredentialPrefix)
	if !ok {
		return nil, errors.New("wire: a credential starts with " + CredentialPrefix)
	}
	seed, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil {
		// The decode error would echo part of the secret.
		return nil, errors.New("wire: a credential's seed is unpadded base64url")
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("wire: a credential's seed is %d bytes, not %d", ed25519.SeedSize, len(seed))
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// Sign signs one request.
func Sign(priv ed25519.PrivateKey, method, path string, timestamp int64, body []byte) []byte {
	return ed25519.Sign(priv, []byte(CanonicalString(method, path, timestamp, body)))
}

// Verify reports whether sig signs the request under pub. It checks the
// signature only. The clock window, the replay check and whether the key is
// known are the receiver's, because each needs state this package has none of.
func Verify(pub ed25519.PublicKey, method, path string, timestamp int64, body, sig []byte) bool {
	// ed25519.Verify panics on a public key of the wrong size.
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, []byte(CanonicalString(method, path, timestamp, body)), sig)
}

// SignRequest sets the three headers on req. body must be the bytes the
// request sends: the caller holds them, and reading req.Body here would
// consume it.
func SignRequest(req *http.Request, priv ed25519.PrivateKey, body []byte, now time.Time) {
	ts := now.Unix()
	sig := Sign(priv, req.Method, req.URL.Path, ts, body)
	req.Header.Set(HeaderKeyID, KeyID(priv.Public().(ed25519.PublicKey)))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, base64.StdEncoding.EncodeToString(sig))
}

// ParseHeaders reads the three headers. A receiver calls it first, looks the
// key up by id, and only then can Verify.
func ParseHeaders(h http.Header) (keyID string, timestamp int64, sig []byte, err error) {
	keyID = h.Get(HeaderKeyID)
	if len(keyID) != keyIDLen {
		return "", 0, nil, fmt.Errorf("wire: %s is %d hex characters", HeaderKeyID, keyIDLen)
	}
	if _, err := hex.DecodeString(keyID); err != nil {
		return "", 0, nil, fmt.Errorf("wire: %s is not hex", HeaderKeyID)
	}
	timestamp, err = strconv.ParseInt(h.Get(HeaderTimestamp), 10, 64)
	if err != nil {
		return "", 0, nil, fmt.Errorf("wire: %s is not Unix seconds", HeaderTimestamp)
	}
	sig, err = base64.StdEncoding.DecodeString(h.Get(HeaderSignature))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return "", 0, nil, fmt.Errorf("wire: %s is not a base64 Ed25519 signature", HeaderSignature)
	}
	return keyID, timestamp, sig, nil
}
```

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `go test ./turbostats/wire/... && go vet ./turbostats/wire/...`
Expected: PASS, and `go vet` prints nothing.

- [ ] **Step 6: Confirm the package imports only the standard library**

Run: `go list -deps ./turbostats/wire | grep '\.' | grep -v '^vendor/'`
Expected: one line, `github.com/turbolytics/sql-flow/turbostats/wire`. A standard-library path has no dot, and the `vendor/` lines are the standard library's own vendored copies. Any other line is a non-standard import and fails this step.

- [ ] **Step 7: Commit**

```bash
git add turbostats/wire/sign.go turbostats/wire/sign_test.go turbostats/wire/testdata/vectors.json
git commit -F - <<'EOF'
wire: Ed25519 request signing, a fingerprint key id, and the credential string

The 2026-09-10 spec authenticates a heartbeat with a bearer token, which
crosses the wire on every request and which a receiver must store to check.

Heartbeats are signed instead. The signature covers five lines: a version,
the method, the path without its query, a Unix timestamp, and the body's
SHA-256. The key id is the first 16 hex characters of the public key's
SHA-256, so an instance and a control plane derive it independently and an
operator can later register a key the control plane never generated. The
credential string is `sfc_` plus the base64url seed; everything else follows
from the seed.

testdata/vectors.json pins a seed, a request, the key id and the signature.
The control plane tests against the same file. If the two sides ever derive
the id or the canonical string differently, every heartbeat is rejected, and
these vectors are what catches it first.
EOF
```

---

### Task 3: `Collect` builds sections from a `Source`

This task moves `internal/turbostats` onto the wire types and applies the bundle amendment to the `run` command. `serve` comes in Task 5.

**Files:**
- Modify: `internal/turbostats/bundle.go` (replace the whole file)
- Modify: `internal/turbostats/collect.go`
- Modify: `internal/turbostats/collect_test.go`
- Modify: `internal/turbostats/handler_test.go` (only if it names a moved field)
- Modify: `internal/cli/run/metrics.go:193-198` (the `collect` closure)
- Modify: `internal/cli/run/root.go:347-353` (the `Static` literal)
- Modify: `tests/release/test_image.py:1123-1127`

**Interfaces:**
- Consumes: every `wire` type from Task 1.
- Produces:
  - `type Bundle = wire.Bundle`, and the same alias for `Instance`, `Process`, `Pipeline`, `Serve`, `ServeCache`, `Exit`; `const MediaType = wire.MediaType`; `const Version = wire.Version`
  - `type Static struct { ID, Name, Version, Commit, ConfigHash string; StartedAt time.Time; IntervalSeconds int }`
  - `type PipelineSource struct { Stats func() (*core.StateStats, error) }`
  - `type ServeSource struct { Sessions func() (inUse, total int); Cache func() (bytes int64, entries int) }`
  - `type Source struct { Static Static; Reader *sdkmetric.ManualReader; Pipeline *PipelineSource; Serve *ServeSource }`
  - `func Collect(ctx context.Context, src Source) (Bundle, error)`

- [ ] **Step 1: Update the tests to the new shape**

In `internal/turbostats/collect_test.go`:

1. Replace the `static` variable:

```go
var static = Static{
	ID:         "pi-01",
	Name:       "demo",
	Version:    "v1.2.3",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 12, 44, 0, time.UTC),
}

// runSource is what the run command hands Collect: a pipeline section, and
// no serve section.
func runSource(r *sdkmetric.ManualReader, stats func() (*core.StateStats, error)) Source {
	return Source{Static: static, Reader: r, Pipeline: &PipelineSource{Stats: stats}}
}
```

2. Replace every `Collect(ctx, static, reader, nil)` with `Collect(ctx, runSource(reader, nil))`. Replace every `Collect(context.Background(), static, reader, nil)` with `Collect(context.Background(), runSource(reader, nil))`. Replace the two calls that pass `stats` with `runSource(reader, stats)`.

3. In `TestCollect_CarriesWhenMessagesLastArrived`, replace the two assertions on `b.LastMessageAt`:

```go
	assert.That(t, b.Pipeline.LastMessageAt != nil)
	assert.Equal(t, int64(1757570000), b.Pipeline.LastMessageAt.Unix())
	// The top-level field is a copy of the section's, so a receiver reads
	// staleness without knowing which sections exist.
	assert.That(t, b.LastActivityAt != nil)
	assert.Equal(t, int64(1757570000), b.LastActivityAt.Unix())
```

4. In `TestCollect_OmitsTheLastMessageBeforeAnyArrive`, replace the assertions after `assert.NoError`:

```go
	assert.That(t, b.Pipeline.LastMessageAt == nil)
	assert.That(t, b.LastActivityAt == nil)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "last_message_at"))
	assert.That(t, !strings.Contains(string(raw), "last_activity_at"))
```

5. In `TestCollect_CarriesTheStaticFactsAndTheRuntime`, replace `assert.Equal(t, "demo", b.Instance.Pipeline)` with `assert.Equal(t, "demo", b.Instance.Name)`.

6. Every other `b.Pipeline.X` access stays as written: a nil pointer would panic, and each of those tests builds a run source, so `b.Pipeline` is set.

7. Add three tests at the end of the file:

```go
// A section's presence says what the process does. A run bundle has no serve
// section, and a source with neither has neither.
func TestCollect_ARunBundleCarriesPipelineAndNoServe(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline != nil)
	assert.That(t, b.Serve == nil)

	bare, err := Collect(context.Background(), Source{Static: static, Reader: reader})
	assert.NoError(t, err)
	assert.That(t, bare.Pipeline == nil)
	assert.That(t, bare.Serve == nil)
}

// The receiver needs the interval to tell late from normal. Without a
// reporter there is no interval, and zero is not one.
func TestCollect_CarriesTheIntervalOnlyWhenThereIsOne(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "interval_seconds"))

	src := runSource(reader, nil)
	src.Static.IntervalSeconds = 60
	b, err = Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, 60, b.IntervalSeconds)
}

// v1 never populates the reserved name.
func TestCollect_LeavesCommandsUnset(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "commands"))
}
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `go test -short ./internal/turbostats/...`
Expected: FAIL. The build reports `undefined: Source`.

- [ ] **Step 3: Replace `internal/turbostats/bundle.go`**

```go
// Package turbostats builds the document a sqlflow process reports about
// itself.
//
// The document's types and its signature are the public contract in
// turbostats/wire. This package is the collector: one function builds the
// bundle, and two transports carry it. The HTTP handler here serves it to
// whatever can reach the process, and the reporter posts it outbound to a
// control plane that cannot. Neither transport computes anything.
//
// Field names are the OTel instrument names, not the Prometheus series names
// those instruments render as. The bundle reads the instrument; the suffixes
// belong to one exporter.
package turbostats

import (
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The wire types, aliased so the engine names one package for the collector
// and its document.
type (
	Bundle     = wire.Bundle
	Instance   = wire.Instance
	Process    = wire.Process
	Pipeline   = wire.Pipeline
	Serve      = wire.Serve
	ServeCache = wire.ServeCache
	Exit       = wire.Exit
)

const (
	MediaType = wire.MediaType
	Version   = wire.Version
)

// Static is what a command knows once, at startup, and the package cannot
// learn on its own.
type Static struct {
	ID, Name, Version, Commit, ConfigHash string
	StartedAt                             time.Time
	// IntervalSeconds is the reporter's interval, and 0 without a reporter.
	IntervalSeconds int
}

// Source is everything Collect reads. A nil Pipeline or Serve omits that
// section: presence says what the process does.
type Source struct {
	Static Static
	Reader *sdkmetric.ManualReader
	// Pipeline is set by `sqlflow run`.
	Pipeline *PipelineSource
	// Serve is set by `sqlflow serve`.
	Serve *ServeSource
}

// PipelineSource is what the pipeline section reads beyond the instruments.
type PipelineSource struct {
	// Stats may be nil for a pipeline with no state path.
	Stats func() (*core.StateStats, error)
}

// ServeSource is what the serve section reads beyond the instruments. Both
// are current values with no cumulative meaning, so they are read when the
// bundle is built rather than recorded.
type ServeSource struct {
	Sessions func() (inUse, total int)
	// Cache is nil on a server where no dataset opted into the cache.
	Cache func() (bytes int64, entries int)
}
```

- [ ] **Step 4: Rewrite `Collect` in `internal/turbostats/collect.go`**

Replace the `Collect` function, and keep `scalars` exactly as it is:

```go
// Collect builds one bundle. It is the only function that does.
//
// It allocates one bundle and touches nothing shared, so the HTTP handler and
// the reporter can call it at once. A stats error is the bundle's error,
// because a document with a field quietly missing reads as healthy.
func Collect(ctx context.Context, src Source) (Bundle, error) {
	var rm metricdata.ResourceMetrics
	if err := src.Reader.Collect(ctx, &rm); err != nil {
		return Bundle{}, fmt.Errorf("turbostats: collecting instruments: %w", err)
	}
	flat := scalars(rm)

	rss, err := ResidentAnonBytes()
	if err != nil {
		return Bundle{}, fmt.Errorf("turbostats: reading resident memory: %w", err)
	}

	s := src.Static
	b := Bundle{
		V:               Version,
		SentAt:          time.Now().UTC().Truncate(time.Second),
		IntervalSeconds: s.IntervalSeconds,
		Instance: Instance{
			ID:         s.ID,
			Name:       s.Name,
			Version:    s.Version,
			Commit:     s.Commit,
			Arch:       runtime.GOOS + "/" + runtime.GOARCH,
			ConfigHash: s.ConfigHash,
		},
		Process: Process{
			StartedAt:  s.StartedAt.UTC().Truncate(time.Second),
			RSSBytes:   rss,
			Goroutines: runtime.NumGoroutine(),
		},
	}

	if src.Pipeline != nil {
		p, err := pipelineSection(flat, src.Pipeline)
		if err != nil {
			return Bundle{}, err
		}
		b.Pipeline = p
		b.LastActivityAt = later(b.LastActivityAt, p.LastMessageAt)
	}
	return b, nil
}

func pipelineSection(flat map[string]int64, src *PipelineSource) (*Pipeline, error) {
	p := &Pipeline{
		MessageCount:     flat["message_count"],
		HandlerRowsRead:  flat["handler_rows_read"],
		ErrorCount:       flat["pipeline_errors"],
		SinkFlushCount:   flat["pipeline_flushes"],
		SinkRowsAccepted: flat["pipeline_rows_accepted"],
		SinkRowsWritten:  flat["pipeline_rows_written"],
		StateCommitCount: flat["pipeline_commits"],
		LastMessageAt:    unixTime(flat["pipeline_last_message_timestamp"]),
	}
	if src.Stats != nil {
		st, err := src.Stats()
		if err != nil {
			return nil, fmt.Errorf("turbostats: reading state stats: %w", err)
		}
		if st != nil {
			size := st.SizeBytes
			p.StateDBSizeBytes = &size
		}
	}
	return p, nil
}

// unixTime is nil for zero: nothing has happened yet, and zero is not a time.
// A receiver derives staleness as sent_at minus this, both from the
// instance's own clock, so the difference carries no skew.
func unixTime(seconds int64) *time.Time {
	if seconds <= 0 {
		return nil
	}
	at := time.Unix(seconds, 0).UTC()
	return &at
}

// later returns the later of two optional times. last_activity_at is the
// latest section timestamp, so a process with two sections reports whichever
// moved last.
func later(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}
	if b == nil || a.After(*b) {
		return a
	}
	return b
}
```

Remove the `"github.com/turbolytics/sql-flow/internal/core"` import from `collect.go` if the compiler reports it unused. `bundle.go` now imports it.

- [ ] **Step 5: Update the two `run` call sites**

In `internal/cli/run/metrics.go`, replace the closure body inside `if serveTurbostats {`:

```go
		collect = func(ctx context.Context) (turbostats.Bundle, error) {
			return turbostats.Collect(ctx, turbostats.Source{
				Static:   static,
				Reader:   reader,
				Pipeline: &turbostats.PipelineSource{Stats: stats},
			})
		}
```

`stats` is a `statsFunc`. If the compiler rejects the assignment, convert it: `Stats: (func() (*core.StateStats, error))(stats)`.

In `internal/cli/run/root.go`, in the `turbostats.Static` literal, rename the field `Pipeline:` to `Name:`. The value stays `conf.Pipeline.Name`.

- [ ] **Step 6: Find every other reference to a moved field**

Run: `grep -rn "LastMessageAt\|Instance.Pipeline\|\.Pipeline\.\(MessageCount\|ErrorCount\)" --include="*.go" internal/ cmd/ | grep -v "^internal/turbostats/collect"`

For each hit outside `internal/turbostats`, apply the same two rules: `b.LastMessageAt` becomes `b.Pipeline.LastMessageAt`, and `Instance.Pipeline` becomes `Instance.Name`. A hit on `b.Pipeline.X` needs no change unless the compiler reports one.

- [ ] **Step 7: Run the Go tests and confirm they pass**

Run: `go build ./... && go test -short ./internal/turbostats/... ./internal/cli/... ./turbostats/...`
Expected: PASS.

- [ ] **Step 8: Update the release test for the moved field**

In `tests/release/test_image.py`, in `test_turbostats_endpoint_serves_the_bundle`, replace these lines:

```python
    assert "last_message_at" not in bundle
    assert bundle["pipeline"]["message_count"] == 0
```

with:

```python
    assert "last_message_at" not in bundle["pipeline"]
    assert "last_activity_at" not in bundle
    assert bundle["pipeline"]["message_count"] == 0

    # A section's presence says what the process does.
    assert "serve" not in bundle
    assert bundle["instance"]["name"]
    assert "pipeline" not in bundle["instance"]
```

- [ ] **Step 9: Commit**

```bash
git add internal/turbostats internal/cli/run tests/release/test_image.py
git commit -F - <<'EOF'
turbostats: Collect builds sections from a Source, on the wire types

The bundle described only a consume loop. `last_message_at` sat at the top
level though it belongs to the pipeline, and `instance.pipeline` named a
thing `sqlflow serve` does not have.

internal/turbostats now aliases the public wire types. Collect takes a Source
whose optional Pipeline and Serve members decide which sections the bundle
carries, so `run` reports `pipeline` and nothing reports an empty section.
`last_message_at` moves into `pipeline`, `instance.pipeline` becomes
`instance.name`, `interval_seconds` is carried when a reporter sets it, and
`last_activity_at` is the latest section timestamp.

The 2026-09-10 spec calls a moved field v2. The amendment takes that
exception once: the only reader is a cron curl. If a second reader exists,
this breaks it, and the release test pins the new shape.
EOF
```

---

### Task 4: `serve` records flat totals into a provider that always exists

**Files:**
- Modify: `internal/serve/metrics.go`
- Modify: `internal/serve/server.go:50-55` (fields), `:175-195` (construction)
- Modify: `internal/serve/http.go:27` (the `/metrics` condition), `:440-447` (the `observeRequest` call)
- Test: `internal/serve/flat_test.go` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `func newMetrics(reg *prom.Registry, stats func() Stats, cacheStats func() (int64, int)) (*metrics, *sdkmetric.ManualReader, error)`; `reg` may be nil.
  - `func (m *metrics) observeRequest(dataset, grain, code string, status int, query time.Duration, ran bool, total time.Duration)`
  - `Server.reader *sdkmetric.ManualReader`, always non-nil after `New`.
  - Flat instrument names: `serve_requests`, `serve_request_errors`, `serve_last_request_timestamp`, `serve_cache_hits`, `serve_cache_misses`, `serve_cache_shared`, `serve_cache_evicted`.

**Background the implementer needs:**
- Today `New` builds `s.metrics` only when the caller passed `WithMetrics` and the config enables `/metrics`. Without it every `observe*` call is a nil-receiver no-op, so nothing records, and a bundle would have nothing to read. `sqlflow run` had the same defect and fixed it in `internal/cli/run/metrics.go:newMeterProvider`. Mirror that function.
- `observeRequest` runs only for dataset requests, because `logRequests` checks `entry.measured`. `request_count` therefore counts dataset requests, matching the attributed `sqlflow_serve_requests_total`.
- `entry.status` holds the HTTP status that `writeJSON` wrote.
- The flat names carry no `sqlflow_` prefix on purpose. `serve_requests` exports to Prometheus as `serve_requests_total`. A flat `sqlflow_serve_requests` would export as `sqlflow_serve_requests_total` and collide with the attributed instrument.

- [ ] **Step 1: Write the failing test**

Create `internal/serve/flat_test.go`:

```go
package serve

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// flatOf reads every dimensionless int64 point, the way turbostats.Collect
// does.
func flatOf(t *testing.T, r *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			}
		}
	}
	return out
}

func noStats() Stats { return Stats{Size: 1} }

// A 5xx is the server's failure and a 4xx is the caller's. The choice is made
// here, where the request is recorded, so the bundle's reader sums nothing.
func TestServeFlat_A5xxIsAnErrorAndA4xxIsNot(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, nil)
	assert.NoError(t, err)

	m.observeRequest("d", "1h", "ok", http.StatusOK, time.Millisecond, true, time.Millisecond)
	m.observeRequest("d", "1h", "bad_request", http.StatusBadRequest, 0, false, time.Millisecond)
	m.observeRequest("d", "1h", "internal", http.StatusInternalServerError, 0, false, time.Millisecond)
	m.observeRequest("d", "1h", "timeout", http.StatusGatewayTimeout, 0, false, time.Millisecond)

	flat := flatOf(t, reader)
	assert.Equal(t, int64(4), flat["serve_requests"])
	assert.Equal(t, int64(2), flat["serve_request_errors"])
	assert.That(t, flat["serve_last_request_timestamp"] > 0)
}

func TestServeFlat_CacheOutcomesAndEvictionsEachHaveATotal(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, func() (int64, int) { return 0, 0 })
	assert.NoError(t, err)

	m.observeCache("d", cacheHit)
	m.observeCache("d", cacheHit)
	m.observeCache("d", cacheMiss)
	m.observeCache("d", cacheShared)
	m.observeEviction("size")
	m.observeEviction("expired")

	flat := flatOf(t, reader)
	assert.Equal(t, int64(2), flat["serve_cache_hits"])
	assert.Equal(t, int64(1), flat["serve_cache_misses"])
	assert.Equal(t, int64(1), flat["serve_cache_shared"])
	assert.Equal(t, int64(2), flat["serve_cache_evicted"])
}

// A server without a cache publishes nothing about one.
func TestServeFlat_NoCacheInstrumentsWithoutACache(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, nil)
	assert.NoError(t, err)
	m.observeCache("d", cacheHit)
	m.observeEviction("size")

	for name := range flatOf(t, reader) {
		assert.That(t, !strings.HasPrefix(name, "serve_cache_"))
	}
}

// The defect this fixes: without /metrics the instruments recorded into
// nothing, so a bundle had nothing to read.
func TestServeFlat_RecordsWithMetricsOff(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/metrics", nil).status)

	flat := flatOf(t, ts.srv.reader)
	assert.Equal(t, int64(1), flat["serve_requests"])
	assert.Equal(t, int64(0), flat["serve_request_errors"])
}

// One instrument feeds both readers. The flat names must not collide with the
// attributed ones once the exporter appends _total, and a collision surfaces
// when the registry is gathered, not when the instrument is created.
func TestServeFlat_ExportsBesideTheAttributedSeries(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	enabled := strings.Replace(testServe, "  limits:", "  metrics: {enabled: true}\n  limits:", 1)
	assert.That(t, enabled != testServe)
	ts := newTestServerWith(t, enabled, WithMetrics(prom.NewRegistry()))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	r := ts.do(t, http.MethodGet, "/metrics", nil)
	assert.Equal(t, http.StatusOK, r.status)
	for _, name := range []string{"serve_requests_total", "sqlflow_serve_requests_total"} {
		if !strings.Contains(r.raw, "\n"+name+"{") && !strings.Contains(r.raw, "\n"+name+" ") {
			t.Fatalf("/metrics does not carry %s:\n%s", name, r.raw)
		}
	}
}
```

- [ ] **Step 2: Register the feature id**

The coverage gate rejects an id that the registry does not know. In `docs/coverage/features.yml`, directly after the `observability.turbostats` entry, add:

```yaml
  - id: observability.turbostats.serve
    description: Reports `sqlflow serve`'s totals as the bundle's serve section, served at /turbostats/v1.
    requires: [unit, release]
```

- [ ] **Step 3: Run the test and confirm it fails**

Run: `go test -short ./internal/serve/ -run TestServeFlat`
Expected: FAIL. The build reports that `newMetrics` returns 2 values and `observeRequest` takes 6 arguments.

- [ ] **Step 4: Rework `internal/serve/metrics.go`**

1. Add the flat fields to the `metrics` struct, after `cacheEvictions`:

```go
	// The flat twins. Each is the dimensionless series the TurboStats bundle
	// reads, recorded beside the attributed instrument it shadows, so which
	// measurements count is decided here and the bundle's reader sums
	// nothing. The cache ones are nil without a cache.
	flatRequests      metric.Int64Counter
	flatRequestErrors metric.Int64Counter
	flatLastRequest   metric.Int64Gauge
	flatCacheHits     metric.Int64Counter
	flatCacheMisses   metric.Int64Counter
	flatCacheShared   metric.Int64Counter
	flatCacheEvicted  metric.Int64Counter
```

2. Change the signature and the provider construction. Replace from `func newMetrics(` through the line `m := mp.Meter(meterName)`:

```go
// newMetrics builds the provider every instrument records into.
//
// The manual reader is attached always: it is what the TurboStats bundle
// reads. The Prometheus exporter is a second reader, attached only when reg
// is non-nil. Both read the same instruments. Before this the provider
// existed only for /metrics, and without it every instrument recorded into
// nothing, so a bundle had nothing to read. `sqlflow run` had the same defect;
// see newMeterProvider in internal/cli/run/metrics.go.
//
// The gauges' callbacks read stats and cacheStats whenever a reader collects.
// cacheStats is nil for a server without a cache.
func newMetrics(reg *prom.Registry, stats func() Stats, cacheStats func() (int64, int)) (*metrics, *sdkmetric.ManualReader, error) {
	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{
		sdkmetric.WithReader(reader),
		// One view, so every histogram here gets buckets chosen for this
		// workload rather than the SDK's defaults, which stop at 10 s.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
	}
	if reg != nil {
		exp, err := prometheus.New(prometheus.WithRegisterer(reg))
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, sdkmetric.WithReader(exp))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
	m := mp.Meter(meterName)
```

3. Every `return nil, err` inside `newMetrics` becomes `return nil, nil, err`. The final `return &mm, nil` becomes `return &mm, reader, nil`.

4. After the `sessionWait` histogram is created and before the `inUse` gauge, add:

```go
	if mm.flatRequests, err = m.Int64Counter("serve_requests",
		metric.WithDescription("Dataset requests answered, any dataset, any outcome. The TurboStats bundle's request_count.")); err != nil {
		return nil, nil, err
	}
	if mm.flatRequestErrors, err = m.Int64Counter("serve_request_errors",
		metric.WithDescription("Dataset requests answered 5xx. A 4xx is the caller's error and is not counted.")); err != nil {
		return nil, nil, err
	}
	if mm.flatLastRequest, err = m.Int64Gauge("serve_last_request_timestamp",
		metric.WithDescription("When a dataset request was last answered, Unix seconds."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}
```

5. Inside `if cacheStats != nil {`, after `cacheEvictions` is created, add:

```go
		if mm.flatCacheHits, err = m.Int64Counter("serve_cache_hits",
			metric.WithDescription("Cached-dataset requests answered from the cache, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheMisses, err = m.Int64Counter("serve_cache_misses",
			metric.WithDescription("Cached-dataset requests that ran the query, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheShared, err = m.Int64Counter("serve_cache_shared",
			metric.WithDescription("Cached-dataset requests that shared a query already running, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheEvicted, err = m.Int64Counter("serve_cache_evicted",
			metric.WithDescription("Entries dropped from the cache, any reason.")); err != nil {
			return nil, nil, err
		}
```

6. Replace `observeEviction`, `observeCache`, and `observeRequest`:

```go
// observeEviction counts one entry leaving the cache.
func (m *metrics) observeEviction(reason string) {
	if m == nil || m.cacheEvictions == nil {
		return
	}
	ctx := context.Background()
	m.cacheEvictions.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	m.flatCacheEvicted.Add(ctx, 1)
}

// observeCache counts one cached dataset's request by how it was answered.
func (m *metrics) observeCache(dataset string, outcome cacheOutcome) {
	if m == nil || m.cacheRequests == nil {
		return
	}
	ctx := context.Background()
	m.cacheRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("outcome", string(outcome)),
	))
	switch outcome {
	case cacheHit:
		m.flatCacheHits.Add(ctx, 1)
	case cacheMiss:
		m.flatCacheMisses.Add(ctx, 1)
	case cacheShared:
		m.flatCacheShared.Add(ctx, 1)
	}
}

// observeRequest records one finished request. code is the error code, or
// "ok". status is the HTTP status written. ran is whether the request ran a
// query: a cache hit did not, and recording its zero would flatten the query
// histogram.
func (m *metrics) observeRequest(dataset, grain, code string, status int, query time.Duration, ran bool, total time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("grain", grain),
		attribute.String("code", code),
	)
	ctx := context.Background()
	m.requests.Add(ctx, 1, attrs)
	m.requestDuration.Record(ctx, total.Seconds(), attrs)
	if ran {
		m.queryDuration.Record(ctx, query.Seconds(), attrs)
	}

	m.flatRequests.Add(ctx, 1)
	// A 5xx is the server's failure. A 4xx is the caller's, and counting it
	// would let one misbehaving client paint the server unhealthy.
	if status >= http.StatusInternalServerError {
		m.flatRequestErrors.Add(ctx, 1)
	}
	m.flatLastRequest.Record(ctx, time.Now().Unix())
}
```

Add `"net/http"` to the imports of `metrics.go`.

- [ ] **Step 5: Rewire `internal/serve/server.go`**

1. Replace the `registry`/`metrics` field comment and fields with:

```go
	// registry is nil unless the caller passed WithMetrics. serveMetrics is
	// whether /metrics is mounted: the caller handed a registry and the
	// config asked for the endpoint. The config decides, not the caller.
	registry     *prom.Registry
	serveMetrics bool
	// metrics and reader always exist after New. The instruments record
	// whether or not anything scrapes them, because the TurboStats bundle
	// reads the manual reader.
	metrics *metrics
	reader  *sdkmetric.ManualReader
```

Add `sdkmetric "go.opentelemetry.io/otel/sdk/metric"` to the imports.

2. Replace the block that starts `if s.registry != nil && conf.Serve.MetricsEnabled() {` and ends at its closing brace:

```go
	// The instruments read the executor's session counts, so they are built
	// here rather than by the caller, and the wait hook is installed on the
	// pool before the server listens.
	s.serveMetrics = s.registry != nil && conf.Serve.MetricsEnabled()
	var exportTo *prom.Registry
	if s.serveMetrics {
		exportTo = s.registry
	}
	var cacheStats func() (int64, int)
	if s.cache != nil {
		cacheStats = s.cache.stats
	}
	m, reader, err := newMetrics(exportTo, ex.Stats, cacheStats)
	if err != nil {
		return nil, err
	}
	s.metrics, s.reader = m, reader
	if s.cache != nil {
		s.cache.onEvict = m.observeEviction
	}
	if wo, ok := ex.(waitObserver); ok {
		wo.setOnWait(m.observeWait)
	}
```

If `err` is declared later in `New` with `:=`, the compiler reports a redeclaration. Change that later `:=` to `=`.

- [ ] **Step 6: Update `internal/serve/http.go`**

1. In `Handler`, change `if s.metrics != nil {` to `if s.serveMetrics {`.
2. In `logRequests`, change the `observeRequest` call to pass the status:

```go
			s.metrics.observeRequest(entry.dataset, entry.grain, code, entry.status, entry.queryDur, entry.ran, time.Since(start))
```

- [ ] **Step 7: Run the serve suite and confirm it passes**

Run: `go build ./... && go test -short ./internal/serve/...`
Expected: PASS, including the existing `TestCliServe_MetricsAreOffUnlessEnabled`, which proves `/metrics` stays absent without the config flag.

- [ ] **Step 8: Commit**

```bash
git add internal/serve docs/coverage/features.yml
git commit -F - <<'EOF'
serve: flat totals, recorded into a provider that always exists

`sqlflow serve` built its meter provider only when the config enabled
/metrics. Without it every instrument recorded into nothing, so a TurboStats
bundle had nothing to read. `sqlflow run` had the same defect before #258.

newMetrics now always attaches a manual reader, and the Prometheus exporter
is a second reader attached when /metrics is on. Each number the bundle's
serve section will report gets a dimensionless twin, recorded where the
attributed instrument is: requests, 5xx answers, the last request's time, the
three cache outcomes, and evictions. A 4xx does not count as an error,
because it is the caller's. The flat names carry no sqlflow_ prefix, so
`serve_requests_total` cannot collide with `sqlflow_serve_requests_total`; a
test gathers a real registry to prove it.

/metrics stays absent unless the config asks. If the wiring is wrong, a
server with metrics off reports zero requests forever, and
TestServeFlat_RecordsWithMetricsOff is the guard.
EOF
```

---

### Task 5: The `serve` section and `GET /turbostats/v1` on `sqlflow serve`

**Files:**
- Modify: `internal/turbostats/collect.go`
- Modify: `internal/turbostats/collect_test.go`
- Modify: `internal/serve/server.go` (one option, one field)
- Modify: `internal/serve/http.go` (one route)
- Modify: `internal/cli/serve/serve.go`
- Test: `internal/serve/turbostats_test.go` (create)

**Interfaces:**
- Consumes: `turbostats.Source`, `turbostats.ServeSource`, `turbostats.Static`, `turbostats.Collect`, `turbostats.Handler` from Task 3. `Server.reader`, the flat instrument names from Task 4.
- Produces:
  - `func WithTurbostats(static turbostats.Static) Option` in `internal/serve`
  - The `--turbostats` flag on `sqlflow serve`
  - `serveConfig(ctx, path, l, onListen, serveTurbostats bool)`: one new trailing parameter

- [ ] **Step 1: Write the failing collector tests**

Append to `internal/turbostats/collect_test.go`:

```go
// serveProvider registers the flat serve instruments by name. The names are
// the contract between internal/serve, which records them, and Collect, which
// reads them; this package cannot import internal/serve to get the real ones.
func serveProvider(t *testing.T) (*sdkmetric.ManualReader, metric.Meter) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, mp.Meter("sqlflow/serve")
}

func addCounter(t *testing.T, m metric.Meter, name string, n int64) {
	t.Helper()
	c, err := m.Int64Counter(name)
	assert.NoError(t, err)
	c.Add(context.Background(), n)
}

func TestCollect_AServeBundleCarriesServeAndNoPipeline(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	addCounter(t, meter, "serve_requests", 40)
	addCounter(t, meter, "serve_request_errors", 2)
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(context.Background(), 1757570000)

	b, err := Collect(context.Background(), Source{
		Static: static,
		Reader: reader,
		Serve:  &ServeSource{Sessions: func() (int, int) { return 2, 8 }},
	})
	assert.NoError(t, err)
	assert.That(t, b.Pipeline == nil)
	assert.That(t, b.Serve != nil)
	assert.Equal(t, int64(40), b.Serve.RequestCount)
	assert.Equal(t, int64(2), b.Serve.RequestErrorCount)
	assert.Equal(t, 2, b.Serve.SessionsInUse)
	assert.Equal(t, 8, b.Serve.SessionsTotal)
	assert.Equal(t, int64(1757570000), b.Serve.LastRequestAt.Unix())
	assert.Equal(t, int64(1757570000), b.LastActivityAt.Unix())
	// An absent cache and an empty cache are different facts.
	assert.That(t, b.Serve.Cache == nil)
}

func TestCollect_TheCacheSectionReadsTotalsAndCurrentSize(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	addCounter(t, meter, "serve_cache_hits", 30)
	addCounter(t, meter, "serve_cache_misses", 7)
	addCounter(t, meter, "serve_cache_shared", 3)
	addCounter(t, meter, "serve_cache_evicted", 5)

	b, err := Collect(context.Background(), Source{
		Static: static,
		Reader: reader,
		Serve: &ServeSource{
			Sessions: func() (int, int) { return 0, 1 },
			Cache:    func() (int64, int) { return 4096, 12 },
		},
	})
	assert.NoError(t, err)
	c := b.Serve.Cache
	assert.That(t, c != nil)
	assert.Equal(t, int64(30), c.HitCount)
	assert.Equal(t, int64(7), c.MissCount)
	assert.Equal(t, int64(3), c.SharedCount)
	assert.Equal(t, int64(5), c.EvictionCount)
	assert.Equal(t, int64(4096), c.Bytes)
	assert.Equal(t, 12, c.Entries)
}

// A process with two sections reports whichever moved last.
func TestCollect_LastActivityIsTheLaterSectionTimestamp(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, meter := provider(t)
	ctx := context.Background()
	m.PipelineLastMessage.Record(ctx, 1757570000)
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(ctx, 1757570500)

	b, err := Collect(ctx, Source{
		Static:   static,
		Reader:   reader,
		Pipeline: &PipelineSource{},
		Serve:    &ServeSource{Sessions: func() (int, int) { return 0, 1 }},
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1757570500), b.LastActivityAt.Unix())
}

func TestCollect_TheServeBundleIsUnderOneKiB(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	for _, name := range []string{"serve_requests", "serve_cache_hits", "serve_cache_misses"} {
		addCounter(t, meter, name, 184203311)
	}
	for _, name := range []string{"serve_request_errors", "serve_cache_shared", "serve_cache_evicted"} {
		addCounter(t, meter, name, 1842033)
	}
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(context.Background(), 1757570000)

	src := Source{
		Static: static,
		Reader: reader,
		Serve: &ServeSource{
			Sessions: func() (int, int) { return 64, 64 },
			Cache:    func() (int64, int) { return 1 << 30, 100000 },
		},
	}
	src.Static.IntervalSeconds = 60
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	b.Exit = &Exit{Reason: "SIGTERM", Code: 0}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, len(raw) < 1024)
}
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `go test -short ./internal/turbostats/ -run "Serve|LastActivity|CacheSection"`
Expected: FAIL with a nil-pointer panic or a failed `b.Serve != nil` assertion, because `Collect` ignores `src.Serve`.

- [ ] **Step 3: Build the serve section in `Collect`**

In `internal/turbostats/collect.go`, inside `Collect`, after the `if src.Pipeline != nil { ... }` block and before `return b, nil`, add:

```go
	if src.Serve != nil {
		sv := serveSection(flat, src.Serve)
		b.Serve = sv
		b.LastActivityAt = later(b.LastActivityAt, sv.LastRequestAt)
	}
```

Add the function below `pipelineSection`:

```go
// serveSection reads the flat series internal/serve records. The names are
// the contract between the two packages, and a test on each side pins them.
func serveSection(flat map[string]int64, src *ServeSource) *Serve {
	sv := &Serve{
		RequestCount:      flat["serve_requests"],
		RequestErrorCount: flat["serve_request_errors"],
		LastRequestAt:     unixTime(flat["serve_last_request_timestamp"]),
	}
	if src.Sessions != nil {
		sv.SessionsInUse, sv.SessionsTotal = src.Sessions()
	}
	if src.Cache != nil {
		bytes, entries := src.Cache()
		sv.Cache = &ServeCache{
			HitCount:      flat["serve_cache_hits"],
			MissCount:     flat["serve_cache_misses"],
			SharedCount:   flat["serve_cache_shared"],
			EvictionCount: flat["serve_cache_evicted"],
			Bytes:         bytes,
			Entries:       entries,
		}
	}
	return sv
}
```

- [ ] **Step 4: Run the collector tests and confirm they pass**

Run: `go test -short ./internal/turbostats/...`
Expected: PASS.

- [ ] **Step 5: Write the failing endpoint test**

Create `internal/serve/turbostats_test.go`:

```go
package serve

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

var testStatic = turbostats.Static{
	Name:       "test-serve",
	Version:    "v0.0.0",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC),
}

// A process does not answer on a route unless told to.
func TestServeTurbostats_IsOffUnlessAsked(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/turbostats/v1", nil).status)
}

// End to end through the real server: a request is answered, the flat series
// moves, and the bundle the route serves says so. This is the test that
// catches the two packages disagreeing about an instrument name.
func TestServeTurbostats_ServesAServeSection(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe, WithTurbostats(testStatic))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	assert.Equal(t, http.StatusOK, r.status)

	var b wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &b))
	assert.Equal(t, 1, b.V)
	assert.Equal(t, "test-serve", b.Instance.Name)
	assert.That(t, b.Pipeline == nil)
	assert.That(t, b.Serve != nil)
	assert.Equal(t, int64(1), b.Serve.RequestCount)
	assert.Equal(t, int64(0), b.Serve.RequestErrorCount)
	// newTestServerWith builds a pool of one.
	assert.Equal(t, 1, b.Serve.SessionsTotal)
	assert.That(t, b.Serve.LastRequestAt != nil)
	assert.That(t, b.LastActivityAt != nil)
	// testServe opts no dataset into the cache.
	assert.That(t, b.Serve.Cache == nil)
}
```

If `ts.do` does not expose the raw body as `r.raw`, read `internal/serve/http_test.go` for the response type's field names and use those.

- [ ] **Step 6: Run the test and confirm it fails**

Run: `go test -short ./internal/serve/ -run TestServeTurbostats`
Expected: FAIL. The build reports `undefined: WithTurbostats`.

- [ ] **Step 7: Add the option and the route**

In `internal/serve/server.go`, add a field to `Server` after `reader`:

```go
	// turbostats is nil unless the caller asked for /turbostats/v1.
	turbostats *turbostats.Static
```

Add the option after `WithMetrics`:

```go
// WithTurbostats serves the process's TurboStats bundle at /turbostats/v1.
// static is what only the command knows: the build's stamp, the config's
// hash, and when the process started.
func WithTurbostats(static turbostats.Static) Option {
	return func(s *Server) { s.turbostats = &static }
}
```

Add `"github.com/turbolytics/sql-flow/internal/turbostats"` to the imports. `internal/turbostats` imports `internal/core` and nothing from `internal/serve`, so this creates no cycle.

Add a method at the end of `server.go`:

```go
// collectBundle builds this server's bundle: the serve section, and no
// pipeline section.
func (s *Server) collectBundle(ctx context.Context) (turbostats.Bundle, error) {
	src := &turbostats.ServeSource{
		Sessions: func() (int, int) {
			st := s.exec.Stats()
			return st.InUse, st.Size
		},
	}
	if s.cache != nil {
		src.Cache = s.cache.stats
	}
	return turbostats.Collect(ctx, turbostats.Source{
		Static: *s.turbostats,
		Reader: s.reader,
		Serve:  src,
	})
}
```

In `internal/serve/http.go`, in `Handler`, after the `/metrics` block, add:

```go
	if s.turbostats != nil {
		// No client id, for the reason /metrics has none: it carries no row
		// data. It is absent unless the command asks, because it names the
		// build and the process's memory on a listener that is public.
		mux.Handle("/turbostats/v1", turbostats.Handler(s.collectBundle))
	}
```

Add the `turbostats` import to `http.go`.

- [ ] **Step 8: Run the serve suite and confirm it passes**

Run: `go build ./... && go test -short ./internal/serve/... ./internal/turbostats/...`
Expected: PASS.

- [ ] **Step 9: Wire the flag in `internal/cli/serve/serve.go`**

1. Beside `var enablePprof bool`, declare `var serveTurbostats bool`.
2. Register the flag beside the `--pprof` flag:

```go
	cmd.Flags().BoolVar(&serveTurbostats, "turbostats", false,
		"Serve GET /turbostats/v1 on the serve address: the process's own state as one document")
```

3. Change the call `return serveConfig(ctx, path, l, nil)` to `return serveConfig(ctx, path, l, nil, serveTurbostats)`.
4. Change the signature to `func serveConfig(ctx context.Context, path string, l *zap.Logger, onListen func(net.Addr), serveTurbostats bool) error`.
5. At the top of `serveConfig`, before `LoadServeRendered`, take the start time:

```go
	// Taken here, once. It is the bundle's started_at, and a restart is a
	// receiver noticing it changed.
	startedAt := time.Now()
```

6. Change `conf, _, err := config.LoadServeRendered(path, nil)` to `conf, rendered, err := config.LoadServeRendered(path, nil)`.
7. After the `if conf.Serve.MetricsEnabled() { ... }` block, add:

```go
	if serveTurbostats {
		opts = append(opts, api.WithTurbostats(turbostats.Static{
			Name:       conf.Serve.Name,
			Version:    buildinfo.Version,
			Commit:     buildinfo.Commit,
			ConfigHash: turbostats.HashConfig(rendered),
			StartedAt:  startedAt,
		}))
	}
```

8. Add the imports `"time"`, `"github.com/turbolytics/sql-flow/internal/buildinfo"`, and `"github.com/turbolytics/sql-flow/internal/turbostats"`.
9. Check `turbostats.HashConfig`'s parameter type in `internal/turbostats/hash.go`. If it takes a `string`, pass `string(rendered)`.
10. Update every test call: run `grep -n "serveConfig(" internal/cli/serve/*_test.go` and append `, false` to each call's arguments.

- [ ] **Step 10: Run the CLI suite and confirm it passes**

Run: `go build ./... && go vet ./internal/... ./turbostats/... && go test -short ./internal/cli/... ./internal/serve/... ./internal/turbostats/... ./turbostats/...`
Expected: PASS, and `go vet` prints nothing.

- [ ] **Step 11: Commit**

```bash
git add internal/turbostats internal/serve internal/cli/serve
git commit -F - <<'EOF'
serve: a TurboStats serve section, at GET /turbostats/v1 behind --turbostats

`sqlflow serve` reported nothing about itself. The control plane's first
deliverable shows two serve processes, and had nothing to show for them.

Collect builds a `serve` section when the Source carries one: request and
5xx totals and the last request's time from the flat series, the pool's
sessions and the cache's size read when the bundle is built. `cache` is
absent on a server without one. `last_activity_at` takes the later of the
two section timestamps.

`sqlflow serve --turbostats` mounts the route on the serve address. It is off
by default, as it is under `run`: the bundle names the build and the
process's memory, and the serve listener is public. The route takes no client
id, as /metrics takes none, because it carries no row data.

internal/serve records the flat series and internal/turbostats reads them by
name. If the names drift, the section reports zeros, and
TestServeTurbostats_ServesAServeSection drives the real server to catch it.
EOF
```

---

### Task 6: Release test, registry, changelog, and the spec pointer

**Files:**
- Modify: `tests/release/test_image.py` (add one test after `test_cli_serve_answers_a_second_request_from_the_cache`)
- Modify: `CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-09-10-turbostats-v1-design.md` (one note under the title)
- Regenerate: `docs/coverage/matrix.md` and `docs/coverage/status/features.yml`

**Interfaces:**
- Consumes: the `--turbostats` flag from Task 5.
- Produces: nothing later tasks use.

- [ ] **Step 1: Add the release test**

In `tests/release/test_image.py`, after `test_cli_serve_answers_a_second_request_from_the_cache`, add:

```python
@pytest.mark.covers("observability.turbostats.serve")
def test_turbostats_serve_bundle_carries_a_serve_section(image):
    """`sqlflow serve --turbostats` answers /turbostats/v1 with a serve
    section, and no pipeline section.

    Driven against the image because what ships is the flag, the route on the
    serve listener, and the link-time stamp together. The cached config is
    used so the cache block is present.
    """
    client_id = "release-client"
    container = DockerContainer(image) \
        .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
        .with_env("SQLFLOW_SERVE_CLIENT_ID", client_id) \
        .with_exposed_ports(8080) \
        .with_command("serve /tmp/conf/config/serve/local.cached.yml --turbostats")
    container.start()
    try:
        wait_for_logs(container, "serving", timeout=60)
        base = f"http://localhost:{container.get_exposed_port(8080)}"
        listing = requests.get(
            f"{base}/v1/datasets", params={"client_id": client_id}, timeout=10)
        name = listing.json()["datasets"][0]["name"]
        first = requests.get(
            f"{base}/v1/datasets/{name}", params={"client_id": client_id}, timeout=10)
        resp = requests.get(f"{base}/turbostats/v1", timeout=10)
    finally:
        container.stop()

    assert resp.status_code == 200, resp.text
    assert resp.headers["Content-Type"] == \
        "application/vnd.turbolytics.turbostats.v1+json"

    bundle = resp.json()
    assert bundle["v"] == 1
    assert "pipeline" not in bundle
    assert bundle["instance"]["name"]
    assert bundle["instance"]["config_hash"].startswith("sha256:")

    stdout, _ = run_docker_container(image, "version")
    stamped = stdout.splitlines()[0].split()[1]
    assert bundle["instance"]["version"] == stamped

    serve = bundle["serve"]
    assert serve["sessions_total"] >= 1
    assert "cache" in serve, "local.cached.yml opts a dataset into the cache"

    # A dataset that needs params answers 400, which is still a request the
    # server answered and never a server error.
    assert serve["request_count"] == 1, (first.status_code, first.text)
    assert serve["request_error_count"] == 0
    assert "last_request_at" in serve
    assert bundle["last_activity_at"] == serve["last_request_at"]
    assert "latency" not in json.dumps(bundle)
```

- [ ] **Step 2: Build the image and run the two TurboStats release tests**

Run: `make sqlflow-image && SQLFLOW_IMAGE=$(make -s print-SQLFLOW_IMAGE 2>/dev/null || echo turbolytics/sql-flow:dev) uv run --locked pytest tests/release -k turbostats -v`

If `print-SQLFLOW_IMAGE` does not exist, read the `SQLFLOW_IMAGE` default at the top of the `Makefile` and pass it explicitly. `make test-image` runs the whole release suite and is the fallback.

Expected: 2 passed. `test_turbostats_endpoint_serves_the_bundle` needs the Kafka stack fixture, which the suite starts itself.

If `request_count` is 0 because the first dataset refuses the request before `measured` is set, read how `local.cached.yml` names its datasets and params, send a request that the dataset answers `200`, and keep the assertion at 1.

- [ ] **Step 3: Add the changelog entries**

In `CHANGELOG.md`, under `## Unreleased`, add an `### Added` section above `### Changed` if none exists, and put this in it:

```markdown
- `sqlflow serve --turbostats` serves `GET /turbostats/v1` on the serve
  address. The bundle carries a `serve` section: request and 5xx totals, the
  pool's sessions, and the cache's outcomes and size. The flag is off by
  default.
- `github.com/turbolytics/sql-flow/turbostats/wire`: the TurboStats bundle
  types and Ed25519 request signing, as a public package with no dependencies
  outside the standard library.
```

Under `### Changed`, add:

```markdown
- The TurboStats bundle is sectioned. `last_message_at` moved from the top
  level into `pipeline`, and `instance.pipeline` is now `instance.name`. A new
  top-level `last_activity_at` carries the latest section timestamp. The
  document stays v1: no receiver existed when the fields moved. Anything that
  parses `/turbostats/v1` output must read the new paths.
- `sqlflow serve` exports seven new Prometheus series without labels, beside
  the labeled ones: `serve_requests_total`, `serve_request_errors_total`,
  `serve_last_request_timestamp_seconds`, `serve_cache_hits_total`,
  `serve_cache_misses_total`, `serve_cache_shared_total`, and
  `serve_cache_evicted_total`.
```

Then verify the exported names, because the exporter's suffix rules are easy to misremember. Run:

```bash
go test -short ./internal/serve/ -run TestServeFlat_ExportsBesideTheAttributedSeries -v
```

Add a temporary `t.Log(r.raw)` to that test, read the seven `serve_` names from the output, correct the changelog to match exactly, and remove the `t.Log`.

- [ ] **Step 4: Point the old spec at the amendment**

In `docs/superpowers/specs/2026-09-10-turbostats-v1-design.md`, directly under the `# TurboStats v1: design` title, add:

```markdown
> **Amended 2026-09-19.** `2026-09-19-turbostats-contract-amendment-design.md`
> reshapes the bundle into sections, moves `last_message_at` into `pipeline`,
> renames `instance.pipeline` to `instance.name`, and replaces the bearer
> token with signed requests. Where the two disagree, the amendment wins.
```

- [ ] **Step 5: Regenerate the coverage matrix and run the gate**

Run: `make coverage-write && make coverage-check`
Expected: `coverage-check` passes, and `observability.turbostats.serve` shows `unit` and `release` as proven. If the gate reports the release level unproven, the release report from Step 2 was not written where `coverage-write` reads it. Read the `coverage-write` target in the `Makefile` for the report path and rerun Step 2 so that it writes there.

- [ ] **Step 6: Run the whole short suite**

Run: `go build ./... && go vet ./... && go test -short ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/release/test_image.py CHANGELOG.md docs/
git commit -F - <<'EOF'
turbostats: prove the serve section against the image, and record the change

The serve section was proven only in-process. What ships is the flag, the
route on the serve listener and the link-time stamp together, and no unit
test proves those.

A release test runs the image with `serve --turbostats`, answers one dataset
request, and asserts the bundle carries a serve section with a cache block
and no pipeline section. The changelog names the two moved fields and the
seven new unlabeled Prometheus series, whose exported names were read from a
real registry rather than reasoned about. The 2026-09-10 spec now points at
the amendment.

If the release test is wrong, a broken flag registration ships green.
EOF
```

- [ ] **Step 8: Run a memory soak**

`CONTRIBUTING.md` requires soak evidence for a change on a process that runs for weeks, and Task 4 adds a manual reader that lives for the life of every `serve` process. Invoke the `memory-soak` skill against `sqlflow serve` with `--turbostats` on and a loop that requests `/turbostats/v1` and one dataset. Attach its report to the PR.

If the soak harness cannot drive `sqlflow serve`, say so in the PR with the reason. Do not substitute a `run` soak and call it evidence for `serve`.

- [ ] **Step 9: Push and open the PR**

```bash
git push -u origin turbostats/contract-pr1
```

Open the PR against `main`. The body names the spec, lists the two moved fields as the breaking change, links the soak report, and ends at the content. No attribution lines and no session links.

---

## Spec coverage

| Spec section | Task |
|---|---|
| One exception to the versioning rule: the field move and the rename | 3 |
| Extensibility rules 1 to 4 | 1 (types, unknown-field test), 3 (`commands` unset) |
| `interval_seconds`, `last_activity_at`, `instance.name` | 1, 3, 5 |
| The `serve` section and its recording table | 4 (flat totals), 5 (section) |
| `serve` builds its meter provider always | 4 |
| Size: a full `run` bundle and a full `serve` bundle under 1 KiB | 3 (existing test), 5 |
| The response envelope type | 1 |
| Signing, headers, key id, credential string | 2 |
| The public package and `testdata/vectors.json` | 1, 2 |
| Release test for `serve` | 6 |
| Feature registry: `observability.turbostats.serve` | 4 |
| Fix the cron parser | Not code in this repository. The PR body must remind the operator to update the Bluesky host's cron parser before deploying. |
| The reporter, its config, response handling rules, `observability.turbostats.reporter` | PR 2, a separate plan |
