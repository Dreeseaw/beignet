package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrNotFound means the immutable bytes do not exist in the backing store.
var ErrNotFound = errors.New("artifact not found")

// Store owns immutable bytes. Raft owns only the small metadata that says
// which hashes are safe for steps to reference.
type Store interface {
	Put(ctx context.Context, hash string, data []byte) error
	Get(ctx context.Context, hash string) ([]byte, error)
}

// ValidHash reports whether hash is a canonical lowercase SHA-256 digest.
func ValidHash(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(hash)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == hash
}

// Verify checks both the digest syntax and the bytes it identifies.
func Verify(hash string, data []byte) error {
	if !ValidHash(hash) {
		return fmt.Errorf("invalid artifact hash %q", hash)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return fmt.Errorf("artifact hash mismatch")
	}
	return nil
}
