package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"
)

type MetaTestSuite struct{}

var _ = check.Suite(&MetaTestSuite{})

func (s *MetaTestSuite) TestNewAwsOps(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-2"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)
	ops := NewAwsOps(sess)
	c.Assert(ops, check.DeepEquals, &DefaultAwsOps{sess: sess})
}

func (s *MetaTestSuite) TestBucketDirs(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
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

	ops := &DefaultAwsOps{sess: sess}
	dirs, err := ops.BucketDirs("no-sync", "bin/")
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	dirs, err = ops.BucketDirs("sync", "bin/")
	c.Assert(err, check.IsNil)
	c.Check(dirs, check.DeepEquals, []string{"3.4-xenial", "3.5-xenial"})
}

func (s *MetaTestSuite) TestBucketObjects(c *check.C) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(sess.Config.HTTPClient)
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

	ops := &DefaultAwsOps{sess: sess}
	files, err := ops.BucketObjects("no-sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err.Error(), check.Equals, "something went wrong listing objects: NotFound: Not Found\n"+
		"\tstatus code: 404, request id: , host id: ")

	files, err = ops.BucketObjects("sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"HIJKLMN", "OPQRSTU"})
}
