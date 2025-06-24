package queue

// Copyright (C) 2025 by Posit Software, PBC

import (
	"context"

	"gopkg.in/check.v1"
)

type WorkSuite struct{}

var _ = check.Suite(&WorkSuite{})

func (s *WorkSuite) TestNewRecursionContext(c *check.C) {
	recurse := func(run func()) {}
	ctx := context.Background()
	ctx = ContextWithRecursion(ctx, 4, recurse)
	val := ctx.Value(CtxRecurse)
	c.Assert(val, check.NotNil)
	r, ok := val.(*CtxRecurseData)
	c.Assert(ok, check.Equals, true)
	c.Assert(r, check.FitsTypeOf, &CtxRecurseData{})
	c.Assert(r.Recurse, check.NotNil)
	c.Assert(r.WorkType, check.Equals, uint64(4))
}

func (s *WorkSuite) TestNewRecursionAllowedContext(c *check.C) {
	ctx := context.Background()
	val := ctx.Value(CtxAllowsRecursion)
	c.Assert(val, check.IsNil)

	ctx = ContextWithExpectedRecursion(ctx)
	val = ctx.Value(CtxAllowsRecursion)
	c.Assert(val, check.NotNil)
	bVal, ok := val.(bool)
	c.Assert(ok, check.Equals, true)
	c.Assert(bVal, check.Equals, true)
}

func (s *WorkSuite) TestNewRecurser(c *check.C) {
	r := NewOptionalRecurser(OptionalRecurserConfig{FatalRecurseCheck: true})
	c.Check(r, check.DeepEquals, &OptionalRecurser{
		fatalRecurseCheck: true,
	})
}

type worker struct {
	done     bool
	recursed bool
}

func (w *worker) work() {
	w.done = true
}

func (w *worker) recurse(run func()) {
	run()
	w.recursed = true
}

func (s *WorkSuite) TestOptionallyRecurse(c *check.C) {
	w := &worker{}
	r := &OptionalRecurser{}
	ctx := context.Background()
	r.OptionallyRecurse(ctx, w.work)
	c.Assert(w.done, check.Equals, true)
	c.Assert(w.recursed, check.Equals, false)

	w = &worker{}
	ctx = ContextWithRecursion(ctx, 4, w.recurse)
	r.OptionallyRecurse(ctx, w.work)
	c.Assert(w.done, check.Equals, true)
	c.Assert(w.recursed, check.Equals, true)

	ctx = ContextWithRecursion(ctx, 5, w.recurse)
	r.OptionallyRecurse(ctx, w.work)
	c.Assert(w.done, check.Equals, true)
	c.Assert(w.recursed, check.Equals, true)
}
