package storage

// Copyright (C) 2022 by RStudio, PBC

import (
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type ServerSuite struct {
	tempdir TempDirHelper
}

var _ = check.Suite(&ServerSuite{})

func (s *ServerSuite) SetUpSuite(c *check.C) {
	c.Assert(s.tempdir.SetUp(), check.IsNil)
}

func (s *ServerSuite) TearDownSuite(c *check.C) {
	c.Assert(s.tempdir.TearDown(), check.IsNil)
}

func (s *ServerSuite) TestGetS3Options(c *check.C) {
	// Test minimum configuration
	s3 := &ConfigS3{}
	o := getS3Options(s3)
	c.Check(o, check.DeepEquals, session.Options{
		Config: aws.Config{
			DisableSSL:       aws.Bool(false),
			S3ForcePathStyle: aws.Bool(false),
		},
		SharedConfigState: session.SharedConfigStateFromEnv,
	})

	// Test maximum configuration
	s3 = &ConfigS3{
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

func (s *ServerSuite) TestSetStorage(c *check.C) {
	cstore := &dummyStore{}
	wn := &dummyWaiterNotifier{}

	// Ok cases
	debugLogger := &TestLogger{}
	ps, err := getStorageServerAttempt(&Config{File: &ConfigFile{Location: "path"}}, "cache", "file", wn, wn, cstore, debugLogger)
	c.Assert(err, check.IsNil)
	c.Assert(ps, check.FitsTypeOf, &FileStorageServer{})
	ps, err = getStorageServerAttempt(&Config{
		S3: &ConfigS3{
			EnableSharedConfig: true,
			Region:             "testregion",
			Profile:            "testprofile",
			SkipValidation:     true,
		},
	}, "packages", "s3", wn, wn, cstore, debugLogger)
	c.Assert(err, check.IsNil)
	c.Assert(ps, check.FitsTypeOf, &S3StorageServer{})
	ps, err = getStorageServerAttempt(&Config{
		S3: &ConfigS3{
			Bucket:         "cranbucket",
			SkipValidation: true,
		},
	}, "cran", "s3", wn, wn, cstore, debugLogger)
	c.Assert(err, check.IsNil)
	c.Assert(ps, check.FitsTypeOf, &S3StorageServer{})
	ps, err = getStorageServerAttempt(&Config{
		S3: &ConfigS3{
			SkipValidation: true,
		},
	}, "metrics", "s3", wn, wn, cstore, debugLogger)
	c.Assert(err, check.IsNil)
	c.Assert(ps, check.FitsTypeOf, &S3StorageServer{})
	ps, err = getStorageServerAttempt(&Config{}, "launcher", "postgres", wn, wn, cstore, debugLogger)
	c.Assert(err, check.IsNil)
	c.Assert(ps, check.FitsTypeOf, &PgStorageServer{})

	// No configuration existing
	_, err = getStorageServerAttempt(&Config{}, "launcher", "s3", wn, wn, cstore, debugLogger)
	c.Assert(err, check.ErrorMatches, `Missing \[S3Storage "launcher"\] configuration section`)

	// No configuration
	_, err = getStorageServerAttempt(&Config{}, "launcher", "file", wn, wn, cstore, debugLogger)
	c.Assert(err, check.ErrorMatches, `Missing \[FileStorage "launcher"\] configuration section`)

	// Bad destination
	_, err = getStorageServerAttempt(&Config{}, "launcher", "nothing", wn, wn, cstore, debugLogger)
	c.Assert(err, check.ErrorMatches, "Invalid destination 'nothing' for 'launcher'")
}

func (s *ServerSuite) TestSetStorageS3Validate(c *check.C) {
	cstore := &dummyStore{}
	wn := &dummyWaiterNotifier{}

	debugLogger := &TestLogger{}

	_, err := getStorageServerAttempt(&Config{
		S3: &ConfigS3{
			Region: "testregion",
		},
	}, "packages", "s3", wn, wn, cstore, debugLogger)
	c.Assert(err, check.NotNil)

	matched, err := regexp.MatchString("Error validating S3 session for 'packages'.*", err.Error())
	c.Assert(err, check.IsNil)
	c.Check(matched, check.Equals, true)
}
