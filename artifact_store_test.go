package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
)

func artifactHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testArtifactStoreContract(t *testing.T, store ArtifactStore) {
	t.Helper()
	ctx := context.Background()
	data := []byte("beignet artifact contract")
	hash := artifactHash(data)

	if _, err := store.Get(ctx, hash); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("missing Get error = %v, want ErrArtifactNotFound", err)
	}
	if err := store.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, hash, data); err != nil {
		t.Fatalf("idempotent Put: %v", err)
	}
	got, err := store.Get(ctx, hash)
	if err != nil || string(got) != string(data) {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if err := store.Put(ctx, artifactHash([]byte("other")), data); err == nil {
		t.Fatal("Put accepted bytes that do not match their hash")
	}
	for _, invalid := range []string{"", "abc", "../escape", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
		if err := store.Put(ctx, invalid, data); err == nil {
			t.Fatalf("Put accepted invalid hash %q", invalid)
		}
	}
}

func TestFileArtifactStoreContract(t *testing.T) {
	store, err := NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	testArtifactStoreContract(t, store)
}

func TestFileArtifactStoreDetectsCorruption(t *testing.T) {
	store, err := NewFileArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("original")
	hash := artifactHash(data)
	if err := store.Put(context.Background(), hash, data); err != nil {
		t.Fatal(err)
	}
	path, err := store.path(hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), hash); err == nil {
		t.Fatal("Get accepted corrupt bytes")
	}
}

func TestS3ArtifactStoreContract(t *testing.T) {
	bucket := os.Getenv("BEIGNET_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("BEIGNET_TEST_S3_BUCKET is not set")
	}
	store, err := NewS3ArtifactStore(context.Background(), S3ArtifactStoreOptions{
		Bucket:    bucket,
		Prefix:    os.Getenv("BEIGNET_TEST_S3_PREFIX"),
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("BEIGNET_TEST_S3_ENDPOINT"),
		PathStyle: strings.EqualFold(os.Getenv("BEIGNET_TEST_S3_PATH_STYLE"), "true"),
	})
	if err != nil {
		t.Fatal(err)
	}
	testArtifactStoreContract(t, store)
}
