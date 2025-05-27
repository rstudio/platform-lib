package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"
)

type MetaTestSuite struct{}

var _ = check.Suite(&MetaTestSuite{})

func (s *MetaTestSuite) TestBucketDirs(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/</Prefix>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes>
    <Prefix>bin/3.4-xenial/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>bin/3.5-xenial/</Prefix>
  </CommonPrefixes>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	dirs, err := awsOps.BucketDirs(ctx, "no-sync", "bin/")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	dirs, err = awsOps.BucketDirs(ctx, "sync", "bin/")
	c.Assert(err, check.IsNil)
	c.Check(dirs, check.DeepEquals, []string{"3.4-xenial", "3.5-xenial"})
}

func (s *MetaTestSuite) TestBucketObjects(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/3.5-xenial/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ABCDEFG.json</Key>
  </Contents>
  <Contents>
    <Key>HIJKLMN.tar.gz</Key>
  </Contents>
  <Contents>
    <Key>OPQRSTU.zip</Key>
  </Contents>
  <Contents>
    <Key>nothing</Key>
  </Contents>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	files, err := awsOps.BucketObjects(ctx, "no-sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	files, err = awsOps.BucketObjects(ctx, "sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"HIJKLMN", "OPQRSTU"})
}

func (s *MetaTestSuite) TestBucketObjectsMap(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/3.5-xenial/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ABCDEFG.json</Key>
    <ETag>123</ETag>
  </Contents>
  <Contents>
    <Key>HIJKLMN.tar.gz</Key>
    <ETag>456</ETag>
  </Contents>
  <Contents>
    <Key>OPQRSTU.zip</Key>
    <ETag>789</ETag>
  </Contents>
  <Contents>
    <Key>nothing</Key>
    <ETag>0</ETag>
  </Contents>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	files, err := awsOps.BucketObjectsETagMap(ctx, "no-sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.NotNil)
	c.Assert(
		err.Error(),
		check.Equals,
		"something went wrong listing objects: NotFound: Not Found\n"+
			"\tstatus code: 404, request id: , host id: ",
	)

	files, err = awsOps.BucketObjectsETagMap(ctx, "sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, map[string]string{
		"HIJKLMN": "456",
		"OPQRSTU": "789",
	})
}
