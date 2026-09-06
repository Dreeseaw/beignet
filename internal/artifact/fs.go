package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// FileStore keeps content-addressed artifacts on a local or shared filesystem.
type FileStore struct {
	root string
}

// NewFile creates a filesystem store rooted at root.
func NewFile(root string) (*FileStore, error) {
	if root == "" {
		return nil, fmt.Errorf("artifact directory is required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact directory: %w", err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact directory: %w", err)
	}
	return &FileStore{root: abs}, nil
}

func (s *FileStore) path(hash string) (string, error) {
	if !ValidHash(hash) {
		return "", fmt.Errorf("invalid artifact hash %q", hash)
	}
	return filepath.Join(s.root, "blobs", "sha256", hash[:2], hash), nil
}

func (s *FileStore) Put(_ context.Context, hash string, data []byte) error {
	if err := Verify(hash, data); err != nil {
		return err
	}
	dest, err := s.path(hash)
	if err != nil {
		return err
	}
	if existing, err := os.ReadFile(dest); err == nil {
		return Verify(hash, existing)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing artifact: %w", err)
	}

	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create artifact shard directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".upload-*")
	if err != nil {
		return fmt.Errorf("create artifact temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write artifact: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close artifact: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		// Another writer may have published the same content-addressed object.
		if existing, readErr := os.ReadFile(dest); readErr == nil {
			return Verify(hash, existing)
		}
		return fmt.Errorf("publish artifact: %w", err)
	}
	return nil
}

func (s *FileStore) Get(_ context.Context, hash string) ([]byte, error) {
	path, err := s.path(hash)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read artifact: %w", err)
	}
	if err := Verify(hash, data); err != nil {
		return nil, fmt.Errorf("corrupt artifact %s: %w", hash, err)
	}
	return data, nil
}
