package localelection_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"fmt"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rselection"
	localelection "github.com/rstudio/platform-lib/v4/pkg/rselection/impls/local"
	"github.com/rstudio/platform-lib/v4/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v4/pkg/rsnotify/listeners/local"
)

// watchTask is a persistent task. It starts when the leader starts and runs
// until the task handler stops.
type watchTask struct {
	rselection.GenericTask
	started chan struct{}
	stopped chan struct{}
}

func (t *watchTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
	fmt.Println("persistent task started")
	close(t.started)
	<-ctx.Done()
	fmt.Println("persistent task stopped")
	close(t.stopped)
}

// sweepTask is a scheduled task. It runs each time its schedule fires.
type sweepTask struct {
	rselection.GenericTask
	ran chan struct{}
}

func (t *sweepTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
	fmt.Println("scheduled task ran")
	t.ran <- struct{}{}
}

// A single node is always the leader. The leader starts the registered tasks
// and stops them when its stop channel closes.
func ExampleLocalLeader() {
	// The leader passes a broadcaster to every task. Here it uses the
	// in-process listener, so no database is needed.
	provider := local.NewListenerProvider(local.ListenerProviderArgs{})
	l := provider.New("example")
	defer l.Stop()
	awbStop := make(chan bool)
	awb, err := broadcaster.NewNotificationBroadcaster(l, awbStop)
	if err != nil {
		panic(err)
	}
	defer func() { awbStop <- true }()

	// A schedule is a channel of times. A real schedule would use
	// time.NewTicker. This example sends the times by hand so that the output
	// does not depend on timing.
	tick := make(chan time.Time)

	watch := &watchTask{
		GenericTask: rselection.GenericTask{
			TaskName: "watch",
			TaskType: rselection.TaskTypePersistent,
		},
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	sweep := &sweepTask{
		GenericTask: rselection.GenericTask{
			TaskName:     "sweep",
			TaskType:     rselection.TaskTypeScheduled,
			TaskSchedule: &rselection.IntervalSchedule{Ticker: tick},
		},
		ran: make(chan struct{}),
	}

	// Register the tasks before the leader starts.
	handler := rselection.NewGenericTaskHandler(rselection.GenericTaskHandlerConfig{})
	handler.Register(watch.Name(), watch)
	handler.Register(sweep.Name(), sweep)

	stop := make(chan bool)
	leader := localelection.NewLocalLeader(localelection.LocalLeaderConfig{
		Broadcaster: awb,
		TaskHandler: handler,
		StopChannel: stop,
	})

	// Lead blocks until the stop channel fires.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := leader.Lead(); err != nil {
			panic(err)
		}
	}()

	<-watch.started
	tick <- time.Now()
	<-sweep.ran

	close(stop)
	<-done
	<-watch.stopped

	// Output:
	// persistent task started
	// scheduled task ran
	// persistent task stopped
}
