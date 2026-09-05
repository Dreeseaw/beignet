package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

var ErrArtifactNotFound = errors.New("artifact not found")

// ArtifactStore owns immutable bytes. Raft owns only the small metadata that
// says which hashes are safe for steps to reference.
type ArtifactStore interface {
	Put(ctx context.Context, hash string, data []byte) error
	Get(ctx context.Context, hash string) ([]byte, error)
}

func validArtifactHash(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(hash)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == hash
}

func verifyArtifact(hash string, data []byte) error {
	if !validArtifactHash(hash) {
		return fmt.Errorf("invalid artifact hash %q", hash)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return fmt.Errorf("artifact hash mismatch")
	}
	return nil
}
