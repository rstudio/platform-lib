package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
)

type S3ServiceSuite struct{}

var _ = check.Suite(&S3ServiceSuite{})

func (s *S3ServiceSuite) TestCreateBucket(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("PUT", `https://foo.s3.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	input := &s3.CreateBucketInput{
		Bucket: aws.String("foo"),
	}
	_, err = s3service.CreateBucket(input)
	expected := "NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestHeadObject(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("HEAD", `https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	input := &s3.HeadObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = s3service.HeadObject(input)
	expected := "NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestGetObject(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	input := &s3.GetObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = s3service.GetObject(input)
	expected := "Something went wrong getting an object from S3. You may want to check your configuration, error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestDeleteObject(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("DELETE", `https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String("foo"),
		Key:    aws.String("foo"),
	}
	_, err = s3service.DeleteObject(input)
	expected := "Something went wrong deleting from S3. You may want to check your configuration, error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestMoveObject(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("HEAD", `https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	_, err = s3service.MoveObject("foo", "foo", "foo2", "newFoo")
	expected := "Something went wrong moving an object on S3. You may want to check your configuration, copy error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestCopyObject(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("HEAD", `https://foo.s3.amazonaws.com/foo`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	s3service := &defaultS3Service{session: sess}

	_, err = s3service.CopyObject("foo", "foo", "foo2", "newFoo")
	expected := "Something went wrong moving an object on S3. You may want to check your configuration, copy error: NotFound: Not Found\tstatus code: 404, request id: , host id: "
	c.Assert(strings.Replace(err.Error(), "\n", "", -1), check.Equals, expected)
}

func (s *S3ServiceSuite) TestListObjects(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
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

	s3service := &defaultS3Service{session: sess}

	files, err := s3service.ListObjects("no-sync", "bin/3.5-xenial")
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	files, err = s3service.ListObjects("sync", "bin/3.5-xenial")
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"ABCDEFG.json", "HIJKLMN.tar.gz", "OPQRSTU.zip", "nothing"})
}

func (s *S3ServiceSuite) TestGetS3Options(c *check.C) {
	// Test minimum configuration
	s3 := &rsstorage.ConfigS3{}
	o := getS3Options(s3)
	c.Check(o, check.DeepEquals, session.Options{
		Config: aws.Config{
			DisableSSL:       aws.Bool(false),
			S3ForcePathStyle: aws.Bool(false),
		},
		SharedConfigState: session.SharedConfigStateFromEnv,
	})

	// Test maximum configuration
	s3 = &rsstorage.ConfigS3{
		Profile:            "test-profile",
		Region:             "us-east-1",
		Endpoint:           "http://localhost:9000",
		EnableSharedConfig: true,
	}
	o = getS3Options(s3)
	c.Check(o, check.DeepEquals, session.Options{
		Config: aws.Config{
			Region:           aws.String("us-east-1"),
			Endpoint:         aws.String("http://localhost:9000"),
			DisableSSL:       aws.Bool(false),
			S3ForcePathStyle: aws.Bool(false),
		},
		Profile:           "test-profile",
		SharedConfigState: session.SharedConfigEnable,
	})
}

func (s *S3ServiceSuite) TestSetStorageS3Validate(c *check.C) {
	s3 := &rsstorage.ConfigS3{
		Region:     "testregion",
		DisableSSL: false,
	}
	svc, err := NewS3Service(s3)
	c.Assert(err, check.IsNil)

	wn := &rsstorage.DummyWaiterNotifier{}
	s3srv := NewS3StorageServer("packages", "s3", svc, 4096, wn, wn)

	err = s3srv.(*S3StorageServer).Validate()
	c.Assert(err, check.NotNil)
}
