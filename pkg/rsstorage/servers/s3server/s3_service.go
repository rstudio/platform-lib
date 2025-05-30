package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal"
)

const S3Concurrency = 20

// S3Wrapper encapsulates the S3 services we need
type S3Wrapper interface {
	CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	Upload(ctx context.Context, input *s3.UploadPartInput, optFns ...func(options *s3.Options)) (*s3.UploadPartOutput, error)
	CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	KmsEncrypted() bool
}

type defaultS3Wrapper struct {
	client *s3.Client
}

func NewS3Wrapper(s3Opts s3.Options) (S3Wrapper, error) {
	if s3Opts.Region == "" {
		return nil, fmt.Errorf("'region' field of ConfigS3 is required")
	}

	return &defaultS3Wrapper{
		client: s3.New(s3Opts),
	}, nil
}

func (s *defaultS3Wrapper) KmsEncrypted() bool {
	return false
}

func (s *defaultS3Wrapper) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	out, err := s.client.CreateBucket(ctx, input)
	if err != nil {
		return nil, err
	}
	return out, err
}

func (s *defaultS3Wrapper) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	out, err := s.client.DeleteBucket(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while deleting an S3 bucket; try checking your configuration:: %w",
			err,
		)
	}
	return out, nil
}

func (s *defaultS3Wrapper) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	out, err := s.client.HeadObject(ctx, input)
	if err != nil {
		var nskErr *types.NoSuchKey
		if errors.As(err, &nskErr) {
			return nil, err
		}
		return nil, fmt.Errorf(
			"error encountered while getting the HEAD for an S3 object; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *defaultS3Wrapper) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	out, err := s.client.GetObject(ctx, input)
	if err != nil {
		var nskErr *types.NoSuchKey
		if errors.As(err, &nskErr) {
			return nil, err
		}
		return nil, fmt.Errorf(
			"error encountered while getting an S3 object; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *defaultS3Wrapper) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	out, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while deleting an S3 object, try checking your configuration: %w",
			err,
		)
	}
	return out, nil
}

func (s *defaultS3Wrapper) Upload(
	ctx context.Context,
	input *s3.UploadPartInput,
	optFns ...func(options *s3.Options),
) (*s3.UploadPartOutput, error) {

	out, err := s.client.UploadPart(ctx, input, optFns...)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while uploading to S3; try checking your configuration: %w",
			err,
		)
	}
	return out, err
}

func (s *defaultS3Wrapper) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *defaultS3Wrapper) MoveObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *defaultS3Wrapper) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return s.client.ListObjectsV2(ctx, input)
}

func getS3Options(configInput rsstorage.ConfigS3) (*s3.Client, error) {
	if configInput.Region == "" {
		return nil, fmt.Errorf("'region' field of ConfigS3 is required")
	}

	options := s3.Options{
		BaseEndpoint:    &configInput.Endpoint,
		EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: configInput.DisableSSL},
		UsePathStyle:    configInput.S3ForcePathStyle,
		Region:          configInput.Region,
	}

	return s3.New(options), nil
}
