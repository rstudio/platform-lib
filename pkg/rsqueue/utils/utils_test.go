package utils

// Copyright (C) 2025 By Posit Software, PBC

import (
	"errors"
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type CheckerSuite struct{}

var _ = check.Suite(&CheckerSuite{})

func (s *CheckerSuite) TestSliceEquivalentChecker(c *check.C) {
	// a is not a slices
	aNot := "test"
	b := []string{"test"}
	result, errs := SliceEquivalent.Check([]interface{}{aNot, b}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "unsupported type")

	// b is not a slice
	a := []string{"test"}
	bNot := "test"
	result, errs = SliceEquivalent.Check([]interface{}{a, bNot}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices are not the same type")

	// length is not equal
	a = []string{"test"}
	b = []string{"test", "two"}
	result, errs = SliceEquivalent.Check([]interface{}{a, b}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices are not the same length: 1 != 2")

	// missing element
	a = []string{"test", "another"}
	b = []string{"test", "two"}
	result, errs = SliceEquivalent.Check([]interface{}{a, b}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices do not match")

	// ok
	a = []string{"test", "another", "thing"}
	b = []string{"test", "thing", "another"}
	result, errs = SliceEquivalent.Check([]interface{}{a, b}, nil)
	c.Check(result, check.Equals, true)
	c.Check(errs, check.Equals, "")

	// missing element
	ai := []int{0, 2}
	bi := []int{0, 1}
	result, errs = SliceEquivalent.Check([]interface{}{ai, bi}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices do not match")

	// ok
	ai = []int{5, 4, 3}
	bi = []int{4, 3, 5}
	result, errs = SliceEquivalent.Check([]interface{}{ai, bi}, nil)
	c.Check(result, check.Equals, true)
	c.Check(errs, check.Equals, "")

	// missing element
	ai64 := []int64{0, 2}
	bi64 := []int64{0, 1}
	result, errs = SliceEquivalent.Check([]interface{}{ai64, bi64}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices do not match")

	// ok
	ai64 = []int64{5, 4, 3}
	bi64 = []int64{4, 3, 5}
	result, errs = SliceEquivalent.Check([]interface{}{ai64, bi64}, nil)
	c.Check(result, check.Equals, true)
	c.Check(errs, check.Equals, "")

	// missing element
	aui64 := []uint64{0, 2}
	bui64 := []uint64{0, 1}
	result, errs = SliceEquivalent.Check([]interface{}{aui64, bui64}, nil)
	c.Check(result, check.Equals, false)
	c.Check(errs, check.Equals, "slices do not match")

	// ok
	aui64 = []uint64{5, 4, 3}
	bui64 = []uint64{4, 3, 5}
	result, errs = SliceEquivalent.Check([]interface{}{aui64, bui64}, nil)
	c.Check(result, check.Equals, true)
	c.Check(errs, check.Equals, "")
}

func (s *CheckerSuite) TestIsSqliteLockError(c *check.C) {
	// nil
	c.Check(IsSqliteLockError(nil), check.Equals, false)

	// false
	someError := errors.New("some error")
	c.Check(IsSqliteLockError(someError), check.Equals, false)

	// true
	sqliteError := errors.New("database is locked")
	c.Check(IsSqliteLockError(sqliteError), check.Equals, true)

	// true
	sqliteError = errors.New("database schema is locked: main")
	c.Check(IsSqliteLockError(sqliteError), check.Equals, true)
}
