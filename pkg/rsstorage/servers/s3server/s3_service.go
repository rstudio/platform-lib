package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal"
)

const S3Concurrency = 20

// Encapsulates the S3 services we need
type S3Wrapper interface {
	CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error)
	DeleteBucket(input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)
	HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	Upload(input *s3manager.UploadInput, ctx context.Context, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
	CopyObject(bucket, key, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	MoveObject(bucket, key, newBucket, newKey string) (*s3.CopyObjectOutput, error)
	ListObjects(bucket, prefix string) ([]string, error)
}

type defaultS3Wrapper struct {
	session *session.Session
}

func NewS3Wrapper(configInput *rsstorage.ConfigS3) (S3Wrapper, error) {
	// Create a session
	options := getS3Options(configInput)
	sess, err := session.NewSessionWithOptions(options)
	if err != nil {
		return nil, fmt.Errorf("Error starting AWS session: %s", err)
	}

	return &defaultS3Wrapper{
		session: sess,
	}, nil
}

func (s *defaultS3Wrapper) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.CreateBucket(input)
	if err != nil {
		return nil, err
	}
	return out, err
}

func (s *defaultS3Wrapper) DeleteBucket(input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.DeleteBucket(input)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong deleting an S3 bucket. You may want to check your configuration, error: %s", err.Error())
	}
	return out, err
}

func (s *defaultS3Wrapper) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey || aerr.Code() == "NotFound" {
				return nil, err
			} else {
				return nil, fmt.Errorf("Something went wrong getting the HEAD for an object from S3. You may want to check your configuration, error: %s", err.Error())
			}
		}
	}
	return out, err
}

func (s *defaultS3Wrapper) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, err
			} else {
				return nil, fmt.Errorf("Something went wrong getting an object from S3. You may want to check your configuration, error: %s", err.Error())
			}
		}
	}
	return out, err
}

func (s *defaultS3Wrapper) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.DeleteObject(input)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong deleting from S3. You may want to check your configuration, error: %s", err.Error())
	}
	return out, nil
}

func (s *defaultS3Wrapper) Upload(input *s3manager.UploadInput, ctx context.Context, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	uploader := s3manager.NewUploader(s.session)
	out, err := uploader.UploadWithContext(ctx, input, options...)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong uploading to S3. You may want to check your configuration, error: %s", err.Error())
	}
	return out, err
}

func (s *defaultS3Wrapper) copyObject(svc *s3.S3, bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	copier := NewCopierWithClient(svc)

	// First, copy the object
	out, err := copier.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String(newBucket),
		Key:        aws.String(newKey),
		CopySource: aws.String(internal.NotEmptyJoin([]string{bucket, oldKey}, "/")),
	})
	if err != nil {
		return nil, fmt.Errorf("Something went wrong moving an object on S3. You may want to check your configuration, copy error: %s", err.Error())
	}

	return out, nil
}

func (s *defaultS3Wrapper) CopyObject(bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	svc := s3.New(s.session)

	out, err := s.copyObject(svc, bucket, oldKey, newBucket, newKey)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *defaultS3Wrapper) MoveObject(bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	svc := s3.New(s.session)

	out, err := s.copyObject(svc, bucket, oldKey, newBucket, newKey)
	if err != nil {
		return nil, err
	}

	// Then, delete the original
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(oldKey)})
	if err != nil {
		return nil, fmt.Errorf("Something went wrong moving an object on S3. You may want to check your configuration, delete error: %s", err.Error())
	}

	return out, nil
}

func (s *defaultS3Wrapper) ListObjects(bucket, prefix string) ([]string, error) {
	ops := NewAwsOps(s.session)
	// prefix must end with a slash
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return ops.BucketObjects(bucket, prefix, S3Concurrency, true, nil)
}

func getS3Options(configInput *rsstorage.ConfigS3) session.Options {
	// By default decide whether to use shared config (e.g., `~/.aws/config`) from the
	// environment. If the environment contains a truthy value for AWS_SDK_LOAD_CONFIG,
	// then we'll use the shared config automatically. However, if
	// `SharedConfigEnable == true`, then we forcefully enable it.
	sharedConfig := session.SharedConfigStateFromEnv
	if configInput.EnableSharedConfig {
		sharedConfig = session.SharedConfigEnable
	}

	// Optionally support a configured region
	s3config := aws.Config{}
	if configInput.Region != "" {
		s3config.Region = aws.String(configInput.Region)
	}

	// Optionally support a configured endpoint
	if configInput.Endpoint != "" {
		s3config.Endpoint = aws.String(configInput.Endpoint)
	}

	s3config.DisableSSL = aws.Bool(configInput.DisableSSL)
	s3config.S3ForcePathStyle = aws.Bool(configInput.S3ForcePathStyle)

	return session.Options{
		Config:            s3config,
		Profile:           configInput.Profile,
		SharedConfigState: sharedConfig,
	}
}
