package runnerfactory

// Copyright (C) 2025 By Posit Software, PBC

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

type RunnerFactory struct {
	runners map[uint64]queue.WorkRunner
	types   queue.QueueSupportedTypes
}

type RunnerFactoryConfig struct {
	SupportedTypes queue.QueueSupportedTypes
}

func NewRunnerFactory(cfg RunnerFactoryConfig) *RunnerFactory {
	return &RunnerFactory{
		runners: make(map[uint64]queue.WorkRunner),
		types:   cfg.SupportedTypes,
	}
}

func (r *RunnerFactory) Add(workType uint64, runner queue.WorkRunner) {
	r.runners[workType] = runner
	r.types.SetEnabled(workType, true)
}

func (r *RunnerFactory) AddConditional(workType uint64, enabled func() bool, runner queue.WorkRunner) {
	r.runners[workType] = runner
	r.types.SetEnabledConditional(workType, enabled)
}

// Run runs work if the work type is configured. Note that this doesn't check to
// see if the work type is enabled (in r.types).
func (r *RunnerFactory) Run(ctx context.Context, work queue.RecursableWork) error {

	runner, ok := r.runners[work.WorkType]
	if !ok {
		return fmt.Errorf("invalid work type %d", work.WorkType)
	}

	return runner.Run(ctx, work)
}

// Stop all the runners in the factory. After each runner is stopped,
// it is marked as disabled so that we won't attempt to grab future
// work for that runner from the queue.
func (r *RunnerFactory) Stop(timeout time.Duration) error {

	// Stop all runners
	wg := &sync.WaitGroup{}
	for key, runner := range r.runners {
		wg.Add(1)
		go func(key uint64, runner queue.WorkRunner) {
			defer wg.Done()
			err := runner.Stop(timeout)
			if err != nil {
				log.Printf("Error stopping runner for type %d: %s", key, err)
			}
			r.types.SetEnabled(key, false)
		}(key, runner)
	}
	wg.Wait()
	return nil
}
