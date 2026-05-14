package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/internal"
)

const S3Concurrency = 20

// S3API is the subset of the AWS S3 client surface that the wrappers in this
// package depend on. The concrete *s3.Client from the AWS SDK satisfies this
// interface, as does *encryptClient.S3EncryptionClientV3 (via its embedded
// *s3.Client). Callers construct the client of their choice and pass it to
// NewS3Wrapper or NewEncryptedS3Wrapper. The interface also covers the
// methods required by manager.NewUploader and s3.NewListObjectsV2Paginator
// so that the wrapper implementations can use them without type assertions.
type S3API interface {
	// Bucket operations
	CreateBucket(ctx context.Context, input *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)

	// Object operations
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CopyObject(ctx context.Context, input *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)

	// Multipart upload operations (required for manager.Uploader)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3Wrapper encapsulates the S3 services we need
type S3Wrapper interface {
	CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	Upload(ctx context.Context, input *s3.PutObjectInput, optFns ...func(uploader *manager.Uploader)) (*manager.UploadOutput, error)
	CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	KmsEncrypted() bool
}

type DefaultS3Wrapper struct {
	client S3API
}

// NewS3Wrapper constructs a S3Wrapper backed by the provided S3 client. The
// caller is responsible for configuring and constructing the client (e.g.
// region, credentials, endpoint). Passing in the client rather than
// configuration values lets callers wrap or replace the AWS SDK client with
// their own implementation, which is useful for testing and for layering on
// behaviors such as Object Lock-aware deletes.
func NewS3Wrapper(client S3API) (*DefaultS3Wrapper, error) {
	if client == nil {
		return nil, errors.New("unable to create S3 wrapper, S3 client is nil")
	}
	return &DefaultS3Wrapper{
		client: client,
	}, nil
}

func (s *DefaultS3Wrapper) KmsEncrypted() bool {
	return false
}

func (s *DefaultS3Wrapper) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	out, err := s.client.CreateBucket(ctx, input)
	if err != nil {
		return nil, err
	}
	return out, err
}

func (s *DefaultS3Wrapper) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	out, err := s.client.DeleteBucket(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while deleting an S3 bucket; try checking your configuration:: %w",
			err,
		)
	}
	return out, nil
}

func (s *DefaultS3Wrapper) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	out, err := s.client.HeadObject(ctx, input)
	if err != nil {
		var nskErr *types.NoSuchKey
		var nfErr *types.NotFound
		if errors.As(err, &nskErr) || errors.As(err, &nfErr) {
			return nil, err
		}
		return nil, fmt.Errorf(
			"error encountered while getting the HEAD for an S3 object; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *DefaultS3Wrapper) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	out, err := s.client.GetObject(ctx, input)
	if err != nil {
		var nskErr *types.NoSuchKey
		var nfErr *types.NotFound
		if errors.As(err, &nskErr) || errors.As(err, &nfErr) {
			return nil, err
		}
		return nil, fmt.Errorf(
			"error encountered while getting an S3 object; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *DefaultS3Wrapper) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	out, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while deleting an S3 object, try checking your configuration: %w",
			err,
		)
	}
	return out, nil
}

func (s *DefaultS3Wrapper) Upload(
	ctx context.Context,
	input *s3.PutObjectInput,
	optFns ...func(uploader *manager.Uploader),
) (*manager.UploadOutput, error) {

	uploader := manager.NewUploader(s.client)

	out, err := uploader.Upload(ctx, input, optFns...)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while uploading to S3; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *DefaultS3Wrapper) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *DefaultS3Wrapper) MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

	// After successful copy, delete the source object (this is what makes it a "move")
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &oldBucket,
		Key:    &oldKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error encountered while deleting source object after move: %w", err)
	}

	return out, nil
}

func (s *DefaultS3Wrapper) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	// In AWS SDK v2, we need to handle pagination manually
	// Create a paginator to iterate through all pages. S3API satisfies the
	// s3.ListObjectsV2APIClient interface implicitly because it declares
	// the same ListObjectsV2 method.
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	// Collect all objects from all pages
	var allObjects []types.Object

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error encountered while listing objects: %w", err)
		}

		// Collect all objects from this page
		allObjects = append(allObjects, page.Contents...)
	}

	// Return all collected objects in a single response
	return &s3.ListObjectsV2Output{
		Contents:    allObjects,
		IsTruncated: new(false),
	}, nil
}
