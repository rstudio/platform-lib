package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"

	encryptClient "github.com/aws/amazon-s3-encryption-client-go/v3/client"
	"github.com/aws/amazon-s3-encryption-client-go/v3/materials"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal"
)

type encryptedS3Service struct {
	client *encryptClient.S3EncryptionClientV3
}

func NewEncryptedS3Wrapper(s3Opts s3.Options, kmsOpts kms.Options, keyID string) (S3Wrapper, error) {
	if s3Opts.Region == "" || kmsOpts.Region == "" {
		return nil, fmt.Errorf("AWS configuration option 'region' is required")
	}

	s3Client := s3.New(s3Opts)
	kmsClient := kms.New(kmsOpts)

	cmm, err := materials.NewCryptographicMaterialsManager(materials.NewKmsKeyring(kmsClient, keyID))
	if err != nil {
		return nil, fmt.Errorf("error encounted while initializing crytographic manager: %w", err)
	}
	client, err := encryptClient.New(s3Client, cmm)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize S3 encryption client: %w", err)
	}

	return &encryptedS3Service{
		client: client,
	}, nil
}

func (s *encryptedS3Service) getConfig() config.Config {
	return s.client.Options
}

func (s *encryptedS3Service) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	return s.client.CreateBucket(ctx, input)
}

func (s *encryptedS3Service) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	return s.client.DeleteBucket(ctx, input)
}

func (s *encryptedS3Service) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, input)
}

func (s *encryptedS3Service) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(ctx, input)
}

func (s *encryptedS3Service) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *encryptedS3Service) MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *encryptedS3Service) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return s.client.ListObjectsV2(ctx, input)
}

// Upload takes the same input as the defaultS3Service *s3.UploadInput,
func (s *encryptedS3Service) Upload(ctx context.Context, input *s3.UploadPartInput, options ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return s.client.UploadPart(ctx, input, options...)
}

// GetObject downloads an encrypted file from S3 and returns the plaintext value using client-side KMS decryption
func (s *encryptedS3Service) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s.client.GetObject(ctx, input)
}

func (s *encryptedS3Service) KmsEncrypted() bool {
	return true
}
