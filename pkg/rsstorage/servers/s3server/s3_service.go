package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
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
	CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	ListObjects(ctx context.Context, input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error)
	KmsEncrypted() bool
}

type defaultS3Wrapper struct {
	client *s3.Client
}

func NewS3Wrapper(configInput rsstorage.ConfigS3, keyID string) (S3Wrapper, error) {
	// Create a session
	s3Client, err := getS3Options(configInput)
	if err != nil {
		return nil, err
	}

	if keyID != "" {
		svc := &encryptedS3Service{
			keyID: keyID,
		}
		svc.client = s3Client
		return svc, nil
	}

	return &defaultS3Wrapper{
		client: s3Client,
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
			"error encountered while deleting an S3 bucket; try checking your configuration, error: %s",
			err.Error(),
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
			"error encountered while getting the HEAD for an S3 object; try checking your configuration, error: %s",
			err.Error(),
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
			"error encountered while getting an S3 object; try checking your configuration, error: %s",
			err.Error(),
		)
	}
	return out, err
}

func (s *defaultS3Wrapper) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	out, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"error encountered while deleting an S3 object, try checking your configuration, error: %s",
			err.Error(),
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
			"error encountered while uploading to S3; try checking your configuration, error: %s",
			err.Error(),
		)
	}
	return out, err
}

func (s *defaultS3Wrapper) CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	out, err := s.client.CopyObject(ctx, input)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *defaultS3Wrapper) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	return s.client.ListObjects(ctx, input)
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
