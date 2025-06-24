package agent

// Copyright (C) 2022 by Posit, PBC

import (
	"math"
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type ConcurrencySuite struct {
	defaults     map[int64]int64
	priorities   map[int64]int64
	priorityList []int64
}

var _ = check.Suite(&ConcurrencySuite{})

func (s *ConcurrencySuite) SetUpSuite(c *check.C) {
	s.priorityList = []int64{1, 3}
	s.priorities = map[int64]int64{
		1: 99,
	}
	s.defaults = map[int64]int64{
		1: 2,
		3: 2,
	}
}

func (s *ConcurrencySuite) TestInt64InSlice(c *check.C) {
	one := int64(1)
	two := int64(2)
	three := int64(3)
	c.Check(Int64InSlice(int64(1), []int64{one, two, three}), check.Equals, true)
	c.Check(Int64InSlice(int64(2), []int64{three, two, one}), check.Equals, true)
	c.Check(Int64InSlice(int64(3), []int64{two, three, one}), check.Equals, true)
	c.Check(Int64InSlice(int64(5), []int64{one, two, three}), check.Equals, false)
}

func (s *ConcurrencySuite) TestConcurrencies(c *check.C) {
	e, err := Concurrencies(s.defaults, s.priorities, s.priorityList)
	c.Check(err, check.IsNil)
	c.Check(e, check.DeepEquals, &ConcurrencyEnforcer{
		concurrencies: map[int64]int64{
			1: 99,
			3: 2,
		},
	})
}

func (s *ConcurrencySuite) TestVerifyOk(c *check.C) {
	e, err := Concurrencies(s.defaults, s.priorities, s.priorityList)
	c.Check(err, check.IsNil)
	err = e.Verify()
	c.Assert(err, check.IsNil)
}

func (s *ConcurrencySuite) TestVerifyFail(c *check.C) {
	priorities := map[int64]int64{
		1: 99,
		3: 100,
	}

	e, err := Concurrencies(s.defaults, priorities, s.priorityList)
	c.Check(err, check.IsNil)
	err = e.Verify()
	c.Check(err, check.ErrorMatches, "Higher priorities may not have lower concurrency "+
		"settings than lower priorities")
}

type checkCase struct {
	jobCount    int64
	ok          bool
	maxPriority uint64
}

func (s *ConcurrencySuite) TestCheck(c *check.C) {
	e, err := Concurrencies(s.defaults, s.priorities, s.priorityList)
	c.Check(err, check.IsNil)

	for _, testCase := range []checkCase{
		{
			jobCount:    0,
			ok:          true,
			maxPriority: math.MaxInt32,
		},
		{
			jobCount:    2,
			ok:          true,
			maxPriority: 1,
		},
		{
			jobCount:    100,
			ok:          false,
			maxPriority: 0,
		},
	} {
		ok, maxPriority := e.Check(testCase.jobCount)
		c.Check(ok, check.Equals, testCase.ok)
		c.Check(maxPriority, check.Equals, testCase.maxPriority)
	}
}
