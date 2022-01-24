package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"io/ioutil"
	"os"
	"regexp"

	"gopkg.in/check.v1"
)

type UtilSuite struct{}

var _ = check.Suite(&UtilSuite{})

func (s *UtilSuite) TestInt64Min(c *check.C) {
	c.Check(MinInt64(-1, 42), check.Equals, int64(-1))
	c.Check(MinInt64(42, 1), check.Equals, int64(1))
}

func (s *UtilSuite) TestInt64Max(c *check.C) {
	c.Check(MaxInt64(-1, 42), check.Equals, int64(42))
	c.Check(MaxInt64(42, 1), check.Equals, int64(42))
}

func (s *UtilSuite) TestRandomString(c *check.C) {
	// TODO: Is it a problem that we use a-zA-Z when building random
	// strings? Some contexts treat characters in a case-insensitive way.
	validRE, err := regexp.Compile("^[a-zA-Z][a-zA-Z0-9]*$")
	c.Assert(err, check.IsNil)
	for _, n := range []int{1, 10, 100} {
		for i := 0; i < 1000; i++ {
			s := RandomString(n)
			comment := check.Commentf("Checking if %s looks like a well-formed random string", s)
			c.Check(len(s), check.Equals, n, comment)
			c.Check(validRE.MatchString(s), check.Equals, true, comment)
		}
	}
}

func (s *UtilSuite) TestNotEmptyJoin(c *check.C) {
	caseA := []string{"one", "two", "three"}
	caseB := []string{"", "one", "", "two", "", "three", ""}
	outputA := NotEmptyJoin(caseA, "/")
	c.Assert(outputA, check.Equals, "one/two/three")
	outputB := NotEmptyJoin(caseB, "/")
	c.Assert(outputB, check.Equals, outputA)
}

// TempDirHelper helps tests create and destroy temporary directories.
type TempDirHelper struct {
	prefix string
	dir    string
}

// SetUp creates a temporary directory
func (h *TempDirHelper) SetUp() error {
	var err error
	h.dir, err = ioutil.TempDir("", h.prefix)
	return err
}

// TearDown removes the configured directory
func (h *TempDirHelper) TearDown() error {
	var err error
	if h.dir != "" {
		err = os.RemoveAll(h.dir)
		h.dir = ""
	}
	return err
}

// Dir returns the path to the configured directory
func (h *TempDirHelper) Dir() string {
	return h.dir
}
