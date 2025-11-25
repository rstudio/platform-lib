package rselection

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/broadcaster"
	"gopkg.in/check.v1"
)

type TaskHandlerSuite struct {
}

var _ = check.Suite(&TaskHandlerSuite{})

// An example scheduled task
type TestTask struct {
	GenericTask
	ran *int
	n   chan bool
}

func (t *TestTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
	// Increments a counter and signals a channel when the task is run.
	// The counter lets us prove that the task ran the correct number of times.
	// The channel lets us block during tests to ensure that the task has time to run.
	*t.ran++
	t.n <- true
}

// An example persistent task
type TestTaskPersistent struct {
	GenericTask
	// Closed when the task exits
	done chan struct{}
	// When signaled, responds with a `true` signal to prove that the
	// task is still running.
	ok chan bool
}

func (t *TestTaskPersistent) Run(ctx context.Context, b broadcaster.Broadcaster) {
	defer close(t.done)
	select {
	case <-t.ok:
		t.ok <- true
	case <-ctx.Done():
	}
}

func (s *TaskHandlerSuite) TestGenericTaskHandler(c *check.C) {
	defer leaktest.Check(c)

	// This is a persistent task that will run until the task handler is stopped.
	persistentTaskDone := make(chan struct{})
	persistentTaskInProgress := make(chan bool)
	defer close(persistentTaskInProgress)
	persistentTask := &TestTaskPersistent{
		GenericTask: GenericTask{
			TaskName: "two",
			TaskType: TaskTypePersistent,
		},
		done: persistentTaskDone,       // Notified when persistent job is stopped
		ok:   persistentTaskInProgress, // Used to prove task is still running
	}

	// This is a scheduled task that will run when a `time.Time` value is sent
	// to the schedule's ticker channel.
	var ran int
	taskDone := make(chan bool)
	defer close(taskDone)
	scheduledTaskTick := make(chan time.Time)
	scheduledTask := &TestTask{
		GenericTask: GenericTask{
			TaskName: "one",
			TaskType: TaskTypeScheduled,
			TaskSchedule: &IntervalSchedule{
				Ticker: scheduledTaskTick,
			},
		},
		ran: &ran,     // Track run count
		n:   taskDone, // Notified when each task run is complete
	}

	// Create a task handler and start handling tasks
	handler := NewGenericTaskHandler(GenericTaskHandlerConfig{})
	handler.Register("one", scheduledTask)
	handler.Register("two", persistentTask)
	handler.Handle(nil)

	// Listen for verification requests. Normally, the leader does this, but in test
	// we'll just mock it.
	end := make(chan struct{})
	defer close(end)
	go func() {
		for {
			select {
			case ch := <-handler.Verify():
				ch <- true
			case <-end:
				return
			}
		}
	}()

	// Sent two ticks to run scheduled tasks, each time waiting
	// until the task runs.
	scheduledTaskTick <- time.Now()
	<-taskDone
	scheduledTaskTick <- time.Now()
	<-taskDone
	// Check that the scheduled task indeed ran the expected number of times.
	c.Assert(ran, check.Equals, 2)

	// Make sure the persistent task is still running
	persistentTaskInProgress <- true
	ok := <-persistentTaskInProgress
	c.Assert(ok, check.Equals, true)

	// Stop and wait for the persistent task to stop
	handler.Stop()
	<-persistentTaskDone
}
