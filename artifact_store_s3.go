package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3ArtifactStoreOptions struct {
	Bucket    string
	Prefix    string
	Region    string
	Endpoint  string
	PathStyle bool
}

type S3ArtifactStore struct {
	client *s3.Client
	bucket string
	prefix string
}

func NewS3ArtifactStore(ctx context.Context, options S3ArtifactStoreOptions) (*S3ArtifactStore, error) {
	if options.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if options.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(options.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	client := s3.NewFromConfig(cfg, func(s3Options *s3.Options) {
		s3Options.UsePathStyle = options.PathStyle
		if options.Endpoint != "" {
			s3Options.BaseEndpoint = aws.String(options.Endpoint)
		}
	})
	return &S3ArtifactStore{
		client: client,
		bucket: options.Bucket,
		prefix: strings.Trim(options.Prefix, "/"),
	}, nil
}

func (s *S3ArtifactStore) key(hash string) (string, error) {
	if !validArtifactHash(hash) {
		return "", fmt.Errorf("invalid artifact hash %q", hash)
	}
	return path.Join(s.prefix, "blobs", "sha256", hash[:2], hash), nil
}

func (s *S3ArtifactStore) Put(ctx context.Context, hash string, data []byte) error {
	if err := verifyArtifact(hash, data); err != nil {
		return err
	}
	key, err := s.key(hash)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put S3 artifact: %w", err)
	}
	return nil
}

func (s *S3ArtifactStore) Get(ctx context.Context, hash string) ([]byte, error) {
	key, err := s.key(hash)
	if err != nil {
		return nil, err
	}
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && strings.EqualFold(apiErr.ErrorCode(), "NoSuchKey") {
			return nil, ErrArtifactNotFound
		}
		return nil, fmt.Errorf("get S3 artifact: %w", err)
	}
	defer output.Body.Close()
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("read S3 artifact: %w", err)
	}
	if err := verifyArtifact(hash, data); err != nil {
		return nil, fmt.Errorf("corrupt S3 artifact %s: %w", hash, err)
	}
	return data, nil
}
