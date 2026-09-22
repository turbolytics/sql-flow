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

// The scopes a credential may carry. A scope names what a control plane may
// do with an instance that signs with it, and both sides need the same
// spelling: the instance lists what it permits, and the receiver refuses a
// request whose credential does not hold what the route needs.
//
// v1 issues ScopeRead. ScopeExecute is reserved, so the column and the config
// key are a set from the start and a later spec defines what it permits.
const (
	ScopeRead    = "read"
	ScopeExecute = "execute"
)

// CredentialPrefix marks a credential string, so a scanner can find a leaked
// one and an operator can tell it from any other secret.
const CredentialPrefix = "sfc_"

// MaxClockSkew is how far a request's timestamp may sit from the receiver's
// clock. The replay defense proper is the bundle's sent_at, which the
// receiver checks against the last one it stored; this bounds how long a
// captured request is even a candidate.
//
// Nothing in this package enforces it. Verify checks a signature and has no
// clock; the receiver compares the timestamp ParseHeaders returns against its
// own. It is exported so every receiver uses the same window.
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
//
// Two things follow from what the string leaves out, and a receiver must hold
// to both:
//
//   - The query is unauthenticated. A receiver must not read a query
//     parameter on a signed route, because anyone on the path can set one.
//   - The host is unbound, so a signature is valid at any receiver that knows
//     the key. A request captured on its way to one control plane replays
//     against another within MaxClockSkew, if the same key is registered at
//     both. Register a key at one control plane only. The host is left out
//     because a receiver behind a reverse proxy often cannot know the name the
//     sender used, and a signature that fails there fails for every request.
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
//
// It returns an error for a private key of the wrong size rather than
// panicking, as ed25519.Sign would. ParseCredential never yields such a key,
// but a caller outside this module may hold key bytes from its own store.
func Sign(priv ed25519.PrivateKey, method, path string, timestamp int64, body []byte) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("wire: a private key is %d bytes, not %d", ed25519.PrivateKeySize, len(priv))
	}
	return ed25519.Sign(priv, []byte(CanonicalString(method, path, timestamp, body))), nil
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
//
// On error it sets no header, so a refused request cannot go out half-signed.
func SignRequest(req *http.Request, priv ed25519.PrivateKey, body []byte, now time.Time) error {
	ts := now.Unix()
	// Sign checks the key's size, which is also what makes Public safe below.
	sig, err := Sign(priv, req.Method, RequestPath(req.URL.Path), ts, body)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderKeyID, KeyID(priv.Public().(ed25519.PublicKey)))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, base64.StdEncoding.EncodeToString(sig))
	return nil
}

// RequestPath is the path a signature covers, normalized.
//
// A URL written without one -- https://control.example -- parses to an empty
// path, but HTTP puts "/" on the wire and that is what a receiver reads back
// from its own request. Signing the empty string would make every such
// heartbeat verify against a different canonical string than the one it was
// signed from, and the receiver would reject a correctly signed bundle.
//
// Both sides call this. A receiver reading r.URL.Path already has "/", so it
// is a no-op there, and it stays in the contract so nobody has to know that.
func RequestPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
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
