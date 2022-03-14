package groups

// Copyright (C) 2022 by RStudio, PBC

import (
	"gopkg.in/check.v1"
)

type GroupQueueEndRunnerFactorySuite struct{}

var _ = check.Suite(&GroupQueueEndRunnerFactorySuite{})

func (s *GroupQueueEndRunnerFactorySuite) SetUpSuite(c *check.C) {
}

func (s *GroupQueueEndRunnerFactorySuite) TearDownSuite(c *check.C) {
}

func (s *GroupQueueEndRunnerFactorySuite) TestNewRunner(c *check.C) {
	r := NewGroupQueueEndRunnerFactory()
	c.Check(r, check.DeepEquals, &DefaultGroupQueueEndRunnerFactory{
		runners: make(map[uint8]GroupQueueEndRunner, 0),
	})
}

type fakeRunner struct {
	err error
}

func (f *fakeRunner) Run(work []byte) error {
	return f.err
}

func (s *GroupQueueEndRunnerFactorySuite) TestAddRunner(c *check.C) {
	r := NewGroupQueueEndRunnerFactory()
	runner := &fakeRunner{}
	r.AddRunner(uint8(1), runner)
	c.Check(r.runners, check.DeepEquals, map[uint8]GroupQueueEndRunner{
		uint8(1): runner,
	})
}

func (s *GroupQueueEndRunnerFactorySuite) TestGetRunner(c *check.C) {
	r := NewGroupQueueEndRunnerFactory()
	runner := &fakeRunner{}
	r.AddRunner(uint8(1), runner)

	// Get an existing runner
	got, err := r.GetRunner(uint8(1))
	c.Assert(err, check.IsNil)
	c.Check(got, check.DeepEquals, runner)

	// Get a runner that doesn't exist
	_, err = r.GetRunner(uint8(2))
	c.Assert(err, check.NotNil)
}
