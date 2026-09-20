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
	got, err := Sign(priv, v.Method, v.Path, v.Timestamp, []byte(v.Body))
	if err != nil {
		t.Fatal(err)
	}
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
	if err := SignRequest(req, priv, body, time.Unix(v.Timestamp, 0)); err != nil {
		t.Fatal(err)
	}

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
		if err := SignRequest(req, priv, []byte(v.Body), time.Unix(v.Timestamp, 0)); err != nil {
			t.Fatal(err)
		}
		return req.Header
	}
	breakers := map[string]func(http.Header){
		"no key id":       func(h http.Header) { h.Del(HeaderKeyID) },
		"short key id":    func(h http.Header) { h.Set(HeaderKeyID, "abc") },
		"non-hex key id":  func(h http.Header) { h.Set(HeaderKeyID, "zzzzzzzzzzzzzzzz") },
		"no timestamp":    func(h http.Header) { h.Del(HeaderTimestamp) },
		"bad timestamp":   func(h http.Header) { h.Set(HeaderTimestamp, "yesterday") },
		"no signature":    func(h http.Header) { h.Del(HeaderSignature) },
		"bad signature":   func(h http.Header) { h.Set(HeaderSignature, "!!!") },
		"short signature": func(h http.Header) { h.Set(HeaderSignature, "AAAA") },
	}
	for name, breakIt := range breakers {
		h := good()
		breakIt(h)
		if _, _, _, err := ParseHeaders(h); err == nil {
			t.Fatalf("%s: ParseHeaders did not fail", name)
		}
	}
}

// Verify refuses a wrong-sized key, and Sign must too. ed25519.Sign panics on
// one, and PrivateKey.Public slices out of range. ParseCredential cannot
// produce such a key, but this package is public: a caller holding key bytes
// from its own store gets an error, not a panic in its reporter.
func TestSign_RefusesAWrongSizedKeyWithoutPanicking(t *testing.T) {
	for _, priv := range []ed25519.PrivateKey{nil, {}, {1, 2, 3}, make([]byte, ed25519.SeedSize)} {
		if _, err := Sign(priv, "POST", "/v1/turbostats", 1, nil); err == nil {
			t.Fatalf("Sign accepted a %d-byte key", len(priv))
		}
		req, _ := http.NewRequest("POST", "https://control.example/v1/turbostats", nil)
		if err := SignRequest(req, priv, nil, time.Unix(1, 0)); err == nil {
			t.Fatalf("SignRequest accepted a %d-byte key", len(priv))
		}
		// A refused request must not go out half-signed.
		for _, h := range []string{HeaderKeyID, HeaderTimestamp, HeaderSignature} {
			if req.Header.Get(h) != "" {
				t.Fatalf("a refused SignRequest set %s", h)
			}
		}
	}
}

// A URL with no path signs what the wire carries, which is "/".
//
// Without this a reporter pointed at https://control.example signs the empty
// string while the receiver verifies against the "/" its own server reports,
// and every heartbeat is refused with a good key and an intact body.
func TestSignRequest_NormalizesAnEmptyPath(t *testing.T) {
	_, priv, pub, _ := loadVectors(t)
	body := []byte(`{"v":1}`)

	req, err := http.NewRequest(http.MethodPost, "https://control.example", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL.Path != "" {
		t.Fatalf("this test is pointless: the path is %q", req.URL.Path)
	}
	if err := SignRequest(req, priv, body, time.Unix(1789848000, 0)); err != nil {
		t.Fatal(err)
	}

	_, ts, sig, err := ParseHeaders(req.Header)
	if err != nil {
		t.Fatal(err)
	}
	// What a receiver verifies: its own request's path, which is never empty.
	if !Verify(pub, http.MethodPost, "/", ts, body, sig) {
		t.Fatal("a pathless URL does not verify against the path the server sees")
	}
}

func TestRequestPath_LeavesARealPathAlone(t *testing.T) {
	if RequestPath("") != "/" {
		t.Fatal("an empty path is /")
	}
	if RequestPath("/v1/turbostats") != "/v1/turbostats" {
		t.Fatal("a real path is itself")
	}
}
