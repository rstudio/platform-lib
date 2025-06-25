package s3server

// Copyright (C) 2025 by Posit Software, PBC

import (
	"context"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"

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
		`https://foo.s3.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			HTTPClient:  &client,
			Credentials: aws.AnonymousCredentials{},
		},
	)
	c.Assert(err, check.IsNil)

	input := &s3.CreateBucketInput{
		Bucket: aws.String("foo"),
	}
	_, err = wrapper.CreateBucket(context.Background(), input)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
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
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
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
		`https://foo.s3.us-east-1.amazonaws.com/foo?x-id=GetObject`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	c.Assert(err, check.IsNil)

	input := &s3.GetObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = wrapper.GetObject(context.Background(), input)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
}

func (s *S3WrapperSuite) TestDeleteObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"DELETE",
		`https://foo.s3.us-east-1.amazonaws.com/foo?x-id=DeleteObject`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	c.Assert(err, check.IsNil)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = wrapper.DeleteObject(context.Background(), input)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
}

func (s *S3WrapperSuite) TestMoveObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"HEAD",
		`https://foo.s3.us-east-1.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	c.Assert(err, check.IsNil)

	_, err = wrapper.MoveObject(context.Background(), "foo", "foo", "foo2", "newFoo")
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
}

func (s *S3WrapperSuite) TestCopyObject(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"HEAD",
		`https://foo.s3.us-east-1.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``),
	)

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	c.Assert(err, check.IsNil)

	_, err = wrapper.CopyObject(context.Background(), "foo", "foo", "foo2", "newFoo")
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
}

func (s *S3WrapperSuite) TestListObjects(c *check.C) {
	client := http.Client{}

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.us-east-1.amazonaws.com/?list-type=2&prefix=bin%2F3.5-xenial`,
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
	httpmock.RegisterResponder("GET", `https://no-sync.s3.us-east-1.amazonaws.com/?list-type=2&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	wrapper, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	c.Assert(err, check.IsNil)

	ctx := context.Background()
	bucket := "no-sync"
	prefix := "bin/3.5-xenial"
	files, err := wrapper.ListObjects(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)

	bucket = "sync"
	files, err = wrapper.ListObjects(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	c.Assert(err, check.IsNil)

	var contents []string
	for _, content := range files.Contents {
		contents = append(contents, *content.Key)
	}
	c.Check(contents, check.DeepEquals, []string{"ABCDEFG.json", "HIJKLMN.tar.gz", "OPQRSTU.zip", "nothing"})
}

func (s *S3WrapperSuite) TestSetStorageS3Validate(c *check.C) {

	svc, err := NewS3Wrapper(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
		},
	)
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
