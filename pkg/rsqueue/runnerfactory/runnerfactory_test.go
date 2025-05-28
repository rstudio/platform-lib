package runnerfactory

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"testing"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/utils"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type RunnerFactorySuite struct{}

var _ = check.Suite(&RunnerFactorySuite{})

func (s *RunnerFactorySuite) SetUpSuite(c *check.C) {
}

func (s *RunnerFactorySuite) TearDownSuite(c *check.C) {
}

type FakeRunnerOne struct {
	queue.BaseRunner
	ran bool
}

func (f *FakeRunnerOne) Run(ctx context.Context, work queue.RecursableWork) error {
	f.ran = true
	return nil
}

type FakeRunnerTwo struct {
	ran       bool
	stop      error
	accepting bool
}

func (f *FakeRunnerTwo) Run(ctx context.Context, work queue.RecursableWork) error {
	f.ran = true
	return nil
}

func (f *FakeRunnerTwo) Stop(timeout time.Duration) error {
	return f.stop
}

func (f *FakeRunnerTwo) Accepting() bool {
	return f.accepting
}

func (s *RunnerFactorySuite) TestNewRunner(c *check.C) {
	ctx := context.Background()
	types := &queue.DefaultQueueSupportedTypes{}
	r := NewRunnerFactory(RunnerFactoryConfig{SupportedTypes: types})
	c.Check(r, check.DeepEquals, &RunnerFactory{
		runners: make(map[uint64]queue.WorkRunner),
		types:   types,
	})

	// Add two runners
	fr1 := &FakeRunnerOne{}
	fr2 := &FakeRunnerTwo{}
	r.Add(0, fr1)
	r.Add(1, fr2)
	c.Check(r.runners, check.HasLen, 2)
	c.Check(fr1.ran, check.Equals, false)
	c.Check(fr2.ran, check.Equals, false)

	// Run something with the first runner
	err := r.Run(ctx, queue.RecursableWork{
		Work:     []byte{},
		WorkType: 0,
	})
	c.Assert(err, check.IsNil)
	c.Check(fr1.ran, check.Equals, true)
	c.Check(fr2.ran, check.Equals, false)

	// Run something with the second runner
	fr1.ran = false
	err = r.Run(ctx, queue.RecursableWork{
		Work:     []byte{},
		WorkType: 1,
	})
	c.Assert(err, check.IsNil)
	c.Check(fr1.ran, check.Equals, false)
	c.Check(fr2.ran, check.Equals, true)

	// Try to run something invalid
	err = r.Run(ctx, queue.RecursableWork{
		Work:     []byte{},
		WorkType: 2,
	})
	c.Assert(err, check.ErrorMatches, "invalid work type 2")
}

func (s *RunnerFactorySuite) TestRunnerConditional(c *check.C) {
	types := &queue.DefaultQueueSupportedTypes{}
	r := NewRunnerFactory(RunnerFactoryConfig{SupportedTypes: types})
	c.Check(r, check.DeepEquals, &RunnerFactory{
		runners: make(map[uint64]queue.WorkRunner),
		types:   types,
	})

	yes := func() bool {
		return true
	}
	no := func() bool {
		return false
	}

	// Add a runner
	fr1 := &FakeRunnerOne{}
	fr2 := &FakeRunnerTwo{}
	r.AddConditional(0, yes, fr1)
	r.AddConditional(1, no, fr2)
	c.Check(r.runners, check.HasLen, 2)
	c.Check(r.types.Enabled(), check.DeepEquals, []uint64{0})
}

func (s *RunnerFactorySuite) TestStop(c *check.C) {
	types := &queue.DefaultQueueSupportedTypes{}
	r := NewRunnerFactory(RunnerFactoryConfig{SupportedTypes: types})
	c.Check(r, check.DeepEquals, &RunnerFactory{
		runners: make(map[uint64]queue.WorkRunner),
		types:   types,
	})

	// Add two runners
	fr1 := &FakeRunnerOne{}
	fr2 := &FakeRunnerTwo{}
	r.Add(6, fr1)
	r.Add(1, fr2)
	c.Check(r.runners, check.HasLen, 2)

	c.Check(r.types.Enabled(), utils.SliceEquivalent, []uint64{6, 1})

	// No errors
	c.Assert(r.Stop(time.Millisecond*100), check.IsNil)
	c.Check(r.types.Enabled(), check.DeepEquals, []uint64{})

	// Error
	//fr2.stop = errors.New("stop error")
	//c.Assert(r.Stop(time.Millisecond*100), check.IsNil)
	//c.Check(r.types.Enabled(), check.DeepEquals, []uint64{})
}
