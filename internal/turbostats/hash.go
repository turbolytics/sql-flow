package turbostats

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashConfig identifies a rendered config by content.
//
// Hashed after templating and before parsing, so the same file under two
// environments hashes differently: those are two configs. The prefix names
// the algorithm so a stored hash stays readable when the algorithm changes.
func HashConfig(rendered []byte) string {
	sum := sha256.Sum256(rendered)
	return "sha256:" + hex.EncodeToString(sum[:])
}
