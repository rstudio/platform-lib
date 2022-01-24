package storage

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
)

const S3Concurrency = 20

// Encapsulates the S3 services we need
type S3Service interface {
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

type defaultS3Service struct {
	session *session.Session
}

func NewS3Service(sess *session.Session) *defaultS3Service {
	return &defaultS3Service{
		session: sess,
	}
}

func (s *defaultS3Service) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.CreateBucket(input)
	if err != nil {
		return nil, err
	}
	return out, err
}

func (s *defaultS3Service) DeleteBucket(input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.DeleteBucket(input)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong deleting an S3 bucket. You may want to check your configuration, error: %s", err.Error())
	}
	return out, err
}

func (s *defaultS3Service) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
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

func (s *defaultS3Service) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
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

func (s *defaultS3Service) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	svc := s3.New(s.session)
	out, err := svc.DeleteObject(input)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong deleting from S3. You may want to check your configuration, error: %s", err.Error())
	}
	return out, nil
}

func (s *defaultS3Service) Upload(input *s3manager.UploadInput, ctx context.Context, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	uploader := s3manager.NewUploader(s.session)
	out, err := uploader.UploadWithContext(ctx, input, options...)
	if err != nil {
		return nil, fmt.Errorf("Something went wrong uploading to S3. You may want to check your configuration, error: %s", err.Error())
	}
	return out, err
}

func (s *defaultS3Service) copyObject(svc *s3.S3, bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	copier := NewCopierWithClient(svc)

	// First, copy the object
	out, err := copier.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String(newBucket),
		Key:        aws.String(newKey),
		CopySource: aws.String(NotEmptyJoin([]string{bucket, oldKey}, "/")),
	})
	if err != nil {
		return nil, fmt.Errorf("Something went wrong moving an object on S3. You may want to check your configuration, copy error: %s", err.Error())
	}

	return out, nil
}

func (s *defaultS3Service) CopyObject(bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	svc := s3.New(s.session)

	out, err := s.copyObject(svc, bucket, oldKey, newBucket, newKey)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s *defaultS3Service) MoveObject(bucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
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

func (s *defaultS3Service) ListObjects(bucket, prefix string) ([]string, error) {
	ops := NewAwsOps(s.session)
	// prefix must end with a slash
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return ops.BucketObjects(bucket, prefix, S3Concurrency, true, nil)
}
