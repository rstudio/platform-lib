package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/internal"
)

type EncryptedS3Wrapper struct {
	client S3API
}

// NewEncryptedS3Wrapper constructs a S3Wrapper backed by a client-side
// encryption-aware S3 client (typically *encryptClient.S3EncryptionClientV3).
// Callers are responsible for constructing the encryption client themselves
// — including the underlying *s3.Client, *kms.Client, and
// CryptographicMaterialsManager — and passing it in here.
func NewEncryptedS3Wrapper(client S3API) (*EncryptedS3Wrapper, error) {
	if client == nil {
		return nil, errors.New("unable to create S3 encrypted wrapper, S3 client is nil")
	}
	return &EncryptedS3Wrapper{
		client: client,
	}, nil
}

func (s *EncryptedS3Wrapper) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	return s.client.CreateBucket(ctx, input)
}

func (s *EncryptedS3Wrapper) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	return s.client.DeleteBucket(ctx, input)
}

func (s *EncryptedS3Wrapper) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, input)
}

func (s *EncryptedS3Wrapper) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(ctx, input)
}

func (s *EncryptedS3Wrapper) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *EncryptedS3Wrapper) MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *EncryptedS3Wrapper) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	// In AWS SDK v2, we need to handle pagination manually
	// Create a paginator to iterate through all pages. S3API satisfies the
	// s3.ListObjectsV2APIClient interface implicitly because it declares
	// the same ListObjectsV2 method.
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	var allObjects []types.Object
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error encountered while listing objects: %w", err)
		}
		allObjects = append(allObjects, page.Contents...)
	}

	return &s3.ListObjectsV2Output{
		Contents:    allObjects,
		IsTruncated: new(false),
	}, nil
}

// Upload takes the same input as the defaultS3Service *s3.UploadInput,
func (s *EncryptedS3Wrapper) Upload(ctx context.Context, input *s3.PutObjectInput, options ...func(uploader *manager.Uploader)) (*manager.UploadOutput, error) {
	uploader := manager.NewUploader(s.client)
	return uploader.Upload(ctx, input, options...)
}

// GetObject downloads an encrypted file from S3 and returns the plaintext value using client-side KMS decryption
func (s *EncryptedS3Wrapper) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s.client.GetObject(ctx, input)
}

func (s *EncryptedS3Wrapper) KmsEncrypted() bool {
	return true
}
