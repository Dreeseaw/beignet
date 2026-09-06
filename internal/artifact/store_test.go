package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func artifactHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testStoreContract(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	data := []byte("beignet artifact contract")
	hash := artifactHash(data)

	if _, err := store.Get(ctx, hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Get error = %v, want ErrNotFound", err)
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

func TestFileStoreContract(t *testing.T) {
	store, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	testStoreContract(t, store)
}

func TestFileStoreDetectsCorruption(t *testing.T) {
	store, err := NewFile(t.TempDir())
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

func TestS3StoreContract(t *testing.T) {
	bucket := os.Getenv("BEIGNET_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("BEIGNET_TEST_S3_BUCKET is not set")
	}
	ctx := context.Background()
	store, err := NewS3(ctx, S3Options{
		Bucket:    bucket,
		Prefix:    os.Getenv("BEIGNET_TEST_S3_PREFIX"),
		Region:    os.Getenv("AWS_REGION"),
		Endpoint:  os.Getenv("BEIGNET_TEST_S3_ENDPOINT"),
		PathStyle: strings.EqualFold(os.Getenv("BEIGNET_TEST_S3_PATH_STYLE"), "true"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(os.Getenv("BEIGNET_TEST_S3_CREATE_BUCKET"), "true") {
		if _, err := store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("create test bucket: %v", err)
		}
	}
	testStoreContract(t, store)

	data := []byte("beignet artifact contract")
	hash := artifactHash(data)
	expectedKey := path.Join(strings.Trim(os.Getenv("BEIGNET_TEST_S3_PREFIX"), "/"), "blobs", "sha256", hash[:2], hash)
	if _, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(expectedKey),
	}); err != nil {
		t.Fatalf("artifact missing at expected S3 key %q: %v", expectedKey, err)
	}
	if _, err := store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(expectedKey),
		Body:   bytes.NewReader([]byte("corrupt")),
	}); err != nil {
		t.Fatalf("corrupt test artifact: %v", err)
	}
	if _, err := store.Get(ctx, hash); err == nil {
		t.Fatal("Get accepted corrupt S3 bytes")
	}
}

func TestS3StoreClassifiesOnlyMissingKeysAsNotFound(t *testing.T) {
	for _, test := range []struct {
		code         string
		wantNotFound bool
	}{
		{code: "NoSuchKey", wantNotFound: true},
		{code: "NotFound", wantNotFound: true},
		{code: "NoSuchBucket", wantNotFound: false},
	} {
		t.Run(test.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "<Error><Code>%s</Code><Message>not found</Message></Error>", test.code)
			}))
			defer server.Close()

			client := s3.New(s3.Options{
				BaseEndpoint: aws.String(server.URL),
				Region:       "us-east-1",
				Credentials:  aws.AnonymousCredentials{},
				UsePathStyle: true,
			})
			store := &S3Store{client: client, bucket: "bucket"}
			_, err := store.Get(context.Background(), artifactHash([]byte("missing")))
			if got := errors.Is(err, ErrNotFound); got != test.wantNotFound {
				t.Fatalf("errors.Is(%v, ErrNotFound) = %v, want %v", err, got, test.wantNotFound)
			}
		})
	}
}
