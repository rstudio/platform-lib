package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal/servertest"
)

type S3WrapperSuite struct{}

var _ = check.Suite(&S3WrapperSuite{})

func (s *S3WrapperSuite) TestCreateBucket(c *check.C) {
	client := http.Client{}
	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"PUT",
		`https://foo.s3.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	input := &s3.CreateBucketInput{
		Bucket: aws.String("foo"),
	}
	_, err = wrapper.CreateBucket(context.Background(), input)
	c.Assert(err, check.NotNil)
	expected := "NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3WrapperSuite) TestHeadObject(c *check.C) {
	client := http.Client{}
	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"HEAD",
		`https://foo.s3.us-east-1.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		rsstorage.ConfigS3{
			Region:         "us-east-1",
			SkipValidation: true,
		},
		&client,
	)
	c.Assert(err, check.IsNil)

	input := &s3.HeadObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = wrapper.HeadObject(context.Background(), input)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
}

func (s *S3WrapperSuite) TestGetObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"GET",
		`https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	input := &s3.GetObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = wrapper.GetObject(context.Background(), input)
	c.Assert(err, check.NotNil)
	expected := "Something went wrong getting an object from S3. You may want to check your configuration, error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3WrapperSuite) TestDeleteObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"DELETE",
		`https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = wrapper.DeleteObject(context.Background(), input)
	c.Assert(err, check.NotNil)
	expected := "Something went wrong deleting from S3. You may want to check your configuration, error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3WrapperSuite) TestMoveObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"HEAD",
		`https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	_, err = wrapper.MoveObject(context.Background(), "foo", "foo", "foo2", "newFoo")
	c.Assert(err, check.NotNil)
	expected := "Something went wrong checking an object on S3. You may want to check your configuration, copy error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3WrapperSuite) TestCopyObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"HEAD",
		`https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	_, err = wrapper.CopyObject(context.Background(), "foo", "foo", "foo2", "newFoo")
	c.Assert(err, check.NotNil)
	expected := "Something went wrong checking an object on S3. You may want to check your configuration, copy error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3WrapperSuite) TestListObjects(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.amazonaws.com/?prefix=bin%2F3.5-xenial%2F`,
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
	httpmock.RegisterResponder("GET", `https://no-sync.s3.amazonaws.com/?prefix=bin%2F3.5-xenial%2F`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	wrapper, err := NewS3Wrapper(rsstorage.ConfigS3{Region: "us-east-1"}, &client)
	c.Assert(err, check.IsNil)

	ctx := context.Background()
	bucket := "no-sync"
	prefix := "bin/3.5-xenial"
	files, err := wrapper.ListObjects(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	bucket = "sync"
	files, err = wrapper.ListObjects(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"ABCDEFG.json", "HIJKLMN.tar.gz", "OPQRSTU.zip", "nothing"})
}

func (s *S3WrapperSuite) TestGetS3Options(c *check.C) {
	// Test minimum configuration
	ops, err := getS3Options(rsstorage.ConfigS3{Region: "us-east-1"})
	c.Assert(err, check.IsNil)
	c.Check(
		ops,
		check.DeepEquals,
		s3.Options{
			UsePathStyle:    false,
			EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: false},
			Region:          "us-east-1",
		},
	)

	baseEndpoint := "http://localhost:9000"

	// Test maximum configuration
	cfg := rsstorage.ConfigS3{
		Profile:            "test-profile",
		Region:             "us-east-1",
		Endpoint:           baseEndpoint,
		EnableSharedConfig: true,
	}
	ops, err = getS3Options(cfg)
	c.Check(
		ops,
		check.DeepEquals,
		s3.Options{
			Region:          "us-east-1",
			EndpointOptions: s3.EndpointResolverOptions{DisableHTTPS: false},
			BaseEndpoint:    &baseEndpoint,
			UsePathStyle:    false,
		},
	)
}

func (s *S3WrapperSuite) TestSetStorageS3Validate(c *check.C) {
	cfg := rsstorage.ConfigS3{
		Region:     "testregion",
		DisableSSL: false,
	}
	svc, err := NewS3Wrapper(cfg, nil)
	c.Assert(err, check.IsNil)

	wn := &servertest.DummyWaiterNotifier{}
	s3srv := NewStorageServer(StorageServerArgs{
		Bucket:    "packages",
		Prefix:    "s3",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})

	err = s3srv.(*StorageServer).Validate(context.Background())
	c.Assert(err, check.NotNil)
}
