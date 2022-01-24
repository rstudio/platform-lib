package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"net/url"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/check.v1"
)

// From https://github.com/aws/aws-sdk-go/pull/2653
//
// This implements multi-part copying to support copying files larger than
// 5GB on S3. This may eventually be included in the aws SDK; when that
// happens, we should remove `s3_copier.go`, `s3_copier_internal_test.go`,
// and `s3_copier_test.go`.

// TODO Watch https://github.com/aws/aws-sdk-go/pull/2653 and remove the
// TODO files listed above when this code is incorporated in the AWS SDK.

type S3CopierInternalSuite struct{}

var _ = check.Suite(&S3CopierInternalSuite{})

func (s *S3CopierInternalSuite) TestCopySourceRange(c *check.C) {
	tests := []struct {
		expect     string
		partNum    int64
		sourceSize int64
		partSize   int64
	}{
		{"bytes=0-9", 1, 10, 999},
		{"bytes=0-4", 1, 10, 5},
		{"bytes=5-9", 2, 10, 5},
	}

	for _, test := range tests {
		rng := copySourceRange(test.sourceSize, test.partSize, test.partNum)
		if rng != test.expect {
			c.Fatalf("expected %v, got %v", test.expect, rng)
		}
	}
}

func (s *S3CopierInternalSuite) TestOptimalPartSize(c *check.C) {
	tests := []struct {
		expect      int64
		sourceSize  int64
		concurrency int
	}{
		{MaxUploadPartSize, 2 * MaxUploadPartSize, 2},
		{MaxUploadPartSize, 3 * MaxUploadPartSize, 2},
		{s3manager.MinUploadPartSize, 2 * s3manager.MinUploadPartSize, 2},
		{s3manager.MinUploadPartSize + 1, 2*s3manager.MinUploadPartSize + 2, 2},
		{s3manager.MinUploadPartSize, 1, 2},
	}

	for _, test := range tests {
		size := optimalPartSize(test.sourceSize, test.concurrency)
		if size != test.expect {
			c.Fatalf("expected %v, got %v", test.expect, size)
		}
	}
}

func (s *S3CopierInternalSuite) TestCopierInitSource(c *check.C) {
	tests := []struct {
		input   string
		bucket  string
		key     string
		version *string
		ok      bool
	}{
		{"a/b/c.txt", "a", "b/c.txt", nil, true},
		{"a/b/c.txt?versionId=foo", "a", "b/c.txt", aws.String("foo"), true},
		{"", "", "", nil, false},
		{"a", "", "", nil, false},
		{"a/", "", "", nil, false},
	}

	for _, test := range tests {
		cc := copier{
			in: &s3.CopyObjectInput{CopySource: aws.String(url.QueryEscape(test.input))},
		}

		err := cc.initSource()
		if !test.ok {
			if err == nil {
				c.Fatalf("expected error; got nil")
			}
		} else {
			if err != nil {
				c.Fatalf("expected no error; got %+v", err)
			}

			if cc.src.bucket != test.bucket {
				c.Fatalf("expected bucket %v; got %v", test.bucket, cc.src.bucket)
			}

			if cc.src.key != test.key {
				c.Fatalf("expected key %v; got %v", test.key, cc.src.key)
			}

			if !reflect.DeepEqual(cc.src.version, test.version) {
				c.Fatalf("expected version %v; got %v", test.version, cc.src.version)
			}
		}
	}
}
