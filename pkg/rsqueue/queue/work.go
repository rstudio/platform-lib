package queue

// Copyright (C) 2025 By Posit Software, PBC

import (
	"context"
	"fmt"
	"log"
	"time"
)

type Work interface {
	Type() uint64
}

type AddressableWork interface {
	Type() uint64
	Address() string
}

type WorkRunner interface {
	// Run the work
	Run(ctx context.Context, work RecursableWork) error

	// Stop the runner. For most implementations, this method should be
	// empty and return `nil`. If special logic is required to stop
	// a runner (for example, keeping a group runner running until all
	// the active groups are done), then it should go here.
	Stop(timeout time.Duration) error
}

// BaseRunner implements the WorkRunner's `Stop` function to avoid boilerplate
// since most runners don't need special stop logic
type BaseRunner struct{}

func (*BaseRunner) Stop(timeout time.Duration) error {
	return nil
}

type RecursableWork struct {
	Work     []byte
	WorkType uint64
	Context  context.Context
}

type CtxRecurseKey string

const (
	CtxRecurse         CtxRecurseKey = "context_recurse_data"
	CtxAllowsRecursion CtxRecurseKey = "context_recurse_allowed"
)

type RecurseFunc func(run func())

type CtxRecurseData struct {
	Recurse  RecurseFunc
	WorkType uint64
}

func ContextWithRecursion(ctx context.Context, workType uint64, recurseFunc RecurseFunc) context.Context {
	return context.WithValue(ctx, CtxRecurse, &CtxRecurseData{Recurse: recurseFunc, WorkType: workType})
}

func ContextWithExpectedRecursion(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, CtxAllowsRecursion, true)
}

type OptionalRecurser struct {
	fatalRecurseCheck bool
}

type OptionalRecurserConfig struct {
	FatalRecurseCheck bool
}

func NewOptionalRecurser(cfg OptionalRecurserConfig) *OptionalRecurser {
	return &OptionalRecurser{
		fatalRecurseCheck: cfg.FatalRecurseCheck,
	}
}

func (a *OptionalRecurser) OptionallyRecurse(ctx context.Context, run func()) {
	recurse := ctx.Value(CtxRecurse)
	if recurse != nil {
		if r, ok := recurse.(*CtxRecurseData); ok {

			// Are we expecting recursion?
			allowed := ctx.Value(CtxAllowsRecursion)
			if allowed == nil {
				msg := fmt.Sprintf("Work with type %d attempted recursion without being marked for recursion", r.WorkType)
				if a.fatalRecurseCheck {
					log.Fatalf(msg)
				} else {
					log.Printf(msg)
				}
			}

			r.Recurse(run)
			return
		}
	}

	// Catch all: handle work without wrapping
	run()
}
