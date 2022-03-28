package groups

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/pkg/rsqueue/types"
)

// Flags are submitted as part of the GroupQueueJob. When we handle work of the GroupQueueJob
// type, we use these flags to determine how to handle the job.
const (
	QueueGroupFlagStart  = "START"  // Job that starts a group of work.
	QueueGroupFlagEnd    = "END"    // Job that finalizes a completed group of work.
	QueueGroupFlagCancel = "CANCEL" // Job that cancels an in-progress group of work.
	QueueGroupFlagAbort  = "ABORT"  // Job that finalizes a cancelled group of work.
)

var ErrQueueGroupStopTimeout = errors.New("timeout stopping queue group runner")

type TypeMatcher interface {
	Field() string
	Register(workType uint64, dataType interface{})
	Type(workType uint64) (interface{}, error)
}

type GenericMatcher struct {
	field string
	types map[uint64]interface{}
}

func (m *GenericMatcher) Field() string {
	return m.field
}

func (m *GenericMatcher) Type(workType uint64) (interface{}, error) {
	return m.types[workType], nil
}

func (m *GenericMatcher) Register(workType uint64, dataType interface{}) {
	m.types[workType] = dataType
}

func NewMatcher(field string) *GenericMatcher {
	return &GenericMatcher{
		field: field,
		types: make(map[uint64]interface{}),
	}
}

type QueueGroupRunner struct {
	queue queue.Queue

	logger types.DebugLogger

	endRunnerFactory GroupQueueEndRunnerFactory

	provider GroupQueueProvider

	matcher TypeMatcher

	recurser *queue.OptionalRecurser

	// A WaitGroup whose delta indicates the number of QueueGroupRunner jobs in progress.
	// Used to safely stop the runner.
	wg *sync.WaitGroup
}

type QueueGroupRunnerConfig struct {
	Queue            queue.Queue
	Provider         GroupQueueProvider
	TypeMatcher      TypeMatcher
	EndRunnerFactory GroupQueueEndRunnerFactory
	Recurser         *queue.OptionalRecurser
	DebugLogger      types.DebugLogger
}

func NewQueueGroupRunner(cfg QueueGroupRunnerConfig) *QueueGroupRunner {
	return &QueueGroupRunner{
		queue:            cfg.Queue,
		provider:         cfg.Provider,
		matcher:          cfg.TypeMatcher,
		endRunnerFactory: cfg.EndRunnerFactory,
		recurser:         cfg.Recurser,
		wg:               &sync.WaitGroup{},
		logger:           cfg.DebugLogger,
	}
}

func (r *QueueGroupRunner) Stop(timeout time.Duration) error {
	t := time.NewTimer(timeout)
	defer t.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-t.C:
		return ErrQueueGroupStopTimeout
	}
}

func (r *QueueGroupRunner) unmarshal(work []byte) (GroupQueueJob, error) {
	var input GroupQueueJob

	// Unmarshal the payload to a raw message
	var tmp map[string]*json.RawMessage
	err := json.Unmarshal(work, &tmp)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling raw message: %s", err)
	}

	// Unmarshal request data type
	var dataType uint64
	if tmp[r.matcher.Field()] == nil {
		return nil, fmt.Errorf("message does not contain data type field %s", r.matcher.Field())
	}
	if err = json.Unmarshal(*tmp[r.matcher.Field()], &dataType); err != nil {
		return nil, fmt.Errorf("error unmarshalling message data type: %s", err)
	}
	t, err := r.matcher.Type(dataType)
	if err != nil {
		return nil, nil
	}
	if t == nil {
		return nil, fmt.Errorf("no matcher type found for %d", dataType)
	}

	// Get an object of the correct type
	input = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(GroupQueueJob)

	// Unmarshal the payload
	err = json.Unmarshal(work, input)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %s", err)
	}

	return input, nil
}

func (r *QueueGroupRunner) Run(work queue.RecursableWork) error {
	r.wg.Add(1)
	defer r.wg.Done()

	var job GroupQueueJob
	var err error
	job, err = r.unmarshal(work.Work)
	if err != nil {
		return err
	}

	// Run in the context of the queue agent's "recurse" function, so
	// we won't continually use an agent concurrency slot while waiting
	// for the queue group to complete.
	r.recurser.OptionallyRecurse(queue.ContextWithExpectedRecursion(work.Context), func() {
		origErr := r.run(job)
		if origErr != nil {
			r.logger.Debugf("QueueGroupRunner run failure: %s", origErr)
			// This will mark the queue group as `cancelled` and allow for re-runs of the same group later.
			// When this occurs, it means that the GroupRunner did not receive a `QueueGroupFlagCancel` job.Flag
			// so we also call the `Fail` method to ensure that any logic for recording failure runs.
			err = r.provider.Cancel(job)
			if err != nil {
				return
			}

			// Remove the queued work from the database to prevent jobs from restarting after the group runner finishes.
			err = r.provider.Clear(job)
			if err != nil {
				return
			}

			// Run any logic that records or handles failure.
			err = r.provider.Fail(job, origErr)
			if err == nil {
				err = origErr
			}
		}
	})

	return err
}

func (r *QueueGroupRunner) run(job GroupQueueJob) error {
	var err error

	switch job.Flag() {
	case QueueGroupFlagStart:
		err = r.provider.IsReady(job)
		if err != nil {
			r.logger.Debugf("Queue Group '%s' error waiting for queue group start: %s\n", job.Name(), err)
			return err
		}

		// Flag group as started
		err = r.provider.Begin(job)
		if err != nil {
			return err
		}

		// Wait for group to complete
		var cancelled bool
		cancelled, err = r.provider.IsComplete(job)

		// Completed with error
		if err != nil {
			r.logger.Debugf("Queue Group '%s' error: %s\n", job.Name(), err)
			return err
		}

		// Completed with cancellation
		if cancelled {
			r.logger.Debugf("Queue Group '%s' cancelled. Pushing QueueGroupFlagAbort work.\n", job.Name())
			return r.queue.Push(0, 0, job.AbortWork())
		}

		// Completed successfully
		r.logger.Debugf("Queue Group '%s' completed. Submitting QueueGroupFlagEnd work.\n", job.Name())
		return r.queue.Push(0, 0, job.EndWork())

	case QueueGroupFlagCancel:
		// First, cancel the queue group
		err = r.provider.Cancel(job)
		if err != nil {
			return err
		}

		// Next, remove all remaining work in the queue group
		return r.provider.Clear(job)

	case QueueGroupFlagEnd:

		r.logger.Debugf("Queue Group '%s' QueueGroupFlagEnd work received. Running end work", job.Name())
		var runner GroupQueueEndRunner
		runner, err = r.endRunnerFactory.GetRunner(job.EndWorkType())
		if err != nil {
			return err
		}

		return runner.Run(job.EndWorkJob())

	case QueueGroupFlagAbort:
		r.logger.Debugf("Queue Group '%s' QueueGroupFlagAbort work received\n", job.Name())
		err = r.provider.Abort(job)
		if err != nil {
			r.logger.Debugf("Error aborting queue group '%s': %s", job.Name(), err)
		}
		return err
	}

	r.logger.Debugf("Unexpected queue group job flag %s\n", job.Flag())
	return nil
}
