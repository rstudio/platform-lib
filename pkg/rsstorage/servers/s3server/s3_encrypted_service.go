package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/internal"
)

type EncryptedS3Service struct {
	client S3API
}

// NewEncryptedS3Wrapper constructs a S3Wrapper backed by a client-side
// encryption-aware S3 client (typically *encryptClient.S3EncryptionClientV3).
// Callers are responsible for constructing the encryption client themselves
// — including the underlying *s3.Client, *kms.Client, and
// CryptographicMaterialsManager — and passing it in here.
func NewEncryptedS3Wrapper(client S3API) (*EncryptedS3Service, error) {
	if client == nil {
		return nil, fmt.Errorf("an S3 client must be provided to NewEncryptedS3Wrapper")
	}
	return &EncryptedS3Service{
		client: client,
	}, nil
}

func (s *EncryptedS3Service) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	return s.client.CreateBucket(ctx, input)
}

func (s *EncryptedS3Service) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	return s.client.DeleteBucket(ctx, input)
}

func (s *EncryptedS3Service) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, input)
}

func (s *EncryptedS3Service) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(ctx, input)
}

func (s *EncryptedS3Service) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	head, err := s.HeadObject(ctx, &s3.HeadObjectInput{Key: &oldKey, Bucket: &oldBucket})
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while getting the HEAD for an S3 object; try checking your configuration: %w",
			err,
		)
	}

	copySource := internal.NotEmptyJoin([]string{oldBucket, oldKey}, "/")
	out, err := s.client.CopyObject(
		ctx, &s3.CopyObjectInput{
			Bucket:            &newBucket,
			Key:               &newKey,
			CopySource:        &copySource,
			MetadataDirective: types.MetadataDirectiveReplace,
			Metadata:          head.Metadata,
		},
	)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *EncryptedS3Service) MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Key:    &oldKey,
		Bucket: &oldBucket,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while getting the HEAD for an S3 object; try checking your configuration: %w",
			err,
		)
	}
	copySource := internal.NotEmptyJoin([]string{oldBucket, oldKey}, "/")
	input := s3.CopyObjectInput{
		Bucket:            &newBucket,
		Key:               &newKey,
		CopySource:        &copySource,
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata:          head.Metadata,
	}
	out, err := s.client.CopyObject(ctx, &input)
	if err != nil {
		return nil, fmt.Errorf("error encountered while moving an S3 object; try checking your configuration: %w", err)
	}
	return out, nil
}

func (s *EncryptedS3Service) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return s.client.ListObjectsV2(ctx, input)
}

// Upload takes the same input as the defaultS3Service *s3.UploadInput,
func (s *EncryptedS3Service) Upload(ctx context.Context, input *s3.PutObjectInput, options ...func(uploader *manager.Uploader)) (*manager.UploadOutput, error) {
	uploader := manager.NewUploader(s.client)
	return uploader.Upload(ctx, input, options...)
}

// GetObject downloads an encrypted file from S3 and returns the plaintext value using client-side KMS decryption
func (s *EncryptedS3Service) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s.client.GetObject(ctx, input)
}

func (s *EncryptedS3Service) KmsEncrypted() bool {
	return true
}
