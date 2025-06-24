package test

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"
	"time"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type TempDirHelperSuite struct{}

var _ = check.Suite(&TempDirHelperSuite{})

// perform some common checks and return the path we were using.
func (s *TempDirHelperSuite) checkHelper(c *check.C, h TempDirHelper) string {
	// before setup, dir is not set.
	c.Check(h.Dir(), check.Equals, "")

	// after setup, we should have a directory that exists.
	err := h.SetUp()
	c.Assert(err, check.IsNil)
	dir := h.Dir()
	c.Check(dir, check.Not(check.Equals), "")

	// after teardown, we should no longer have a configured directory and
	// the previously configured one has been deleted.
	err = h.TearDown()
	c.Assert(err, check.IsNil)
	c.Check(h.Dir(), check.Equals, "")

	// double-deletion is not a problem
	err = h.TearDown()
	c.Assert(err, check.IsNil)

	return dir
}

func (s *TempDirHelperSuite) TestHelper(c *check.C) {
	h := NewTempDirHelper()

	s.checkHelper(c, h) // ignoring return
}

type CheckerSuite struct {
	now   time.Time
	later time.Time
}

var _ = check.Suite(&CheckerSuite{})

func (s *CheckerSuite) SetUpSuite(c *check.C) {
	s.now = time.Now()
	s.later = s.now.Add(time.Second)
}

func (s *CheckerSuite) TestTimeEqualsNonTimeFirstArg(c *check.C) {
	// non-time first argument is an error.
	result, errs := TimeEquals.Check([]interface{}{"not time", s.now}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "obtained is not a time.Time")
}

func (s *CheckerSuite) TestTimeEqualsNonTimeSecondArg(c *check.C) {
	// non-time second argument is an error.
	result, errs := TimeEquals.Check([]interface{}{s.now, "not time"}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "expected is not a time.Time")
}

func (s *CheckerSuite) TestTimeEqualsSameTime(c *check.C) {
	// same time is equals
	result, errs := TimeEquals.Check([]interface{}{s.now, s.now}, nil)
	c.Check(result, check.Equals, true)
	c.Check(errs, check.Equals, "")
}

func (s *CheckerSuite) TestTimeEqualsNowNotLater(c *check.C) {
	// now is not later
	result, errs := TimeEquals.Check([]interface{}{s.now, s.later}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "")
}

func (s *CheckerSuite) TestTimeEqualsLaterNotNow(c *check.C) {
	// later is not now
	result, errs := TimeEquals.Check([]interface{}{s.later, s.now}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "")
}
