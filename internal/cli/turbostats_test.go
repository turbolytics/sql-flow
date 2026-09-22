package cli

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// vector is the shared wire vector, which pins the strings a control plane
// must agree with.
func vector(t *testing.T) (seed []byte, credential, publicKey, keyID string) {
	t.Helper()
	raw, err := os.ReadFile("../../turbostats/wire/testdata/vectors.json")
	assert.NoError(t, err)
	var v struct {
		SeedHex    string `json:"seed_hex"`
		Credential string `json:"credential"`
		PublicKey  string `json:"public_key"`
		KeyID      string `json:"key_id"`
	}
	assert.NoError(t, json.Unmarshal(raw, &v))
	seed, err = hex.DecodeString(v.SeedHex)
	assert.NoError(t, err)
	return seed, v.Credential, v.PublicKey, v.KeyID
}

// The private half goes to a file only readable by its owner, and the public
// half and key id to stdout, byte for byte what the wire vector says.
func TestTurbostatsKeygen_WritesThePrivateHalfAndPrintsThePublicHalf(t *testing.T) {
	coverage.Covers(t, "cli.turbostats_keygen")
	seed, credential, publicKey, keyID := vector(t)
	out := filepath.Join(t.TempDir(), "turbostats.key")
	var stdout bytes.Buffer

	assert.NoError(t, keygen(out, bytes.NewReader(seed), &stdout))

	written, err := os.ReadFile(out)
	assert.NoError(t, err)
	assert.Equal(t, credential+"\n", string(written))
	info, err := os.Stat(out)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	assert.That(t, strings.Contains(stdout.String(), "public key: "+publicKey))
	assert.That(t, strings.Contains(stdout.String(), "key id:     "+keyID))
	// The credential reaches the file and nothing else: stdout ends up in
	// terminal scrollback and CI logs.
	assert.That(t, !strings.Contains(stdout.String(), credential))
}

// A key file that exists is someone's key. Overwriting it would strand every
// instance that uses it.
func TestTurbostatsKeygen_RefusesToOverwrite(t *testing.T) {
	coverage.Covers(t, "cli.turbostats_keygen")
	seed, _, _, _ := vector(t)
	out := filepath.Join(t.TempDir(), "turbostats.key")
	assert.NoError(t, os.WriteFile(out, []byte("existing\n"), 0o600))

	err := keygen(out, bytes.NewReader(seed), &bytes.Buffer{})
	assert.That(t, err != nil)
	assert.That(t, strings.Contains(err.Error(), "exists"))
	kept, _ := os.ReadFile(out)
	assert.Equal(t, "existing\n", string(kept))
}

// Two runs make two different keys. The test's fixed seed must not hide a
// command that ignores its randomness.
func TestTurbostatsKeygen_MakesADifferentKeyEachRun(t *testing.T) {
	coverage.Covers(t, "cli.turbostats_keygen")
	dir := t.TempDir()
	cmd := NewRootCommand()
	var first, second bytes.Buffer
	cmd.SetOut(&first)
	cmd.SetArgs([]string{"turbostats", "keygen", "--out", filepath.Join(dir, "a.key")})
	assert.NoError(t, cmd.Execute())
	cmd = NewRootCommand()
	cmd.SetOut(&second)
	cmd.SetArgs([]string{"turbostats", "keygen", "--out", filepath.Join(dir, "b.key")})
	assert.NoError(t, cmd.Execute())

	a, b := publicKeyLine(first.String()), publicKeyLine(second.String())
	assert.That(t, strings.HasPrefix(a, "public key: sfp_"))
	assert.That(t, a != b)
}

func publicKeyLine(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "public key: ") {
			return line
		}
	}
	return ""
}

func TestTurbostatsKeygen_RequiresOut(t *testing.T) {
	coverage.Covers(t, "cli.turbostats_keygen")
	cmd := NewRootCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"turbostats", "keygen"})
	assert.That(t, cmd.Execute() != nil)
}
