package rselection_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"fmt"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rselection"
	"github.com/rstudio/platform-lib/v4/pkg/rsnotify/broadcaster"
)

// reportTask is a scheduled task that prints a line each time it runs.
type reportTask struct {
	rselection.GenericTask
	ran chan struct{}
}

func (t *reportTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
	fmt.Println("report ran")
	t.ran <- struct{}{}
}

// A GenericTaskHandler runs scheduled tasks when their schedule fires. Before
// each run it asks the leader to verify that the cluster is healthy. The
// leader reads these requests from Verify and answers true or false. A false
// answer skips that run.
func ExampleGenericTaskHandler() {
	tick := make(chan time.Time)
	task := &reportTask{
		GenericTask: rselection.GenericTask{
			TaskName:     "report",
			TaskType:     rselection.TaskTypeScheduled,
			TaskSchedule: &rselection.IntervalSchedule{Ticker: tick},
		},
		ran: make(chan struct{}),
	}

	handler := rselection.NewGenericTaskHandler(rselection.GenericTaskHandlerConfig{})
	handler.Register(task.Name(), task)
	// Handle passes its argument to each task's Run. A leader passes its
	// broadcaster here. This task does not use it, so nil is enough.
	handler.Handle(nil)
	defer handler.Stop()

	// Play the part of the leader. Approve the first verification request and
	// reject the second.
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		for _, ok := range []bool{true, false} {
			vCh := <-handler.Verify()
			vCh <- ok
		}
	}()

	tick <- time.Now()
	<-task.ran

	// The leader rejects this run, so the task does not run and prints nothing.
	tick <- time.Now()
	<-answered

	// Output:
	// report ran
}
