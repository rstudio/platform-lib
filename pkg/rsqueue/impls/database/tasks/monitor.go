package tasks

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

type DatabaseQueueMonitor interface {
	Run(ctx context.Context, b broadcaster.Broadcaster)
	Check(ctx context.Context, permitId uint64, created time.Time, maxAge time.Duration) bool
}

// DatabaseQueueMonitorTask a task that monitors queue permit extension notifications. This task
// also fulfills the `Check` interface method and is used by the `DatabaseQueueSweeperTask` to
// check for expired permits. This is a persistent task that runs until the provided context is
// canceled.
type DatabaseQueueMonitorTask struct {
	check    chan permitCheck
	sweepAge time.Duration
	cstore   queue.QueueStore
	started  time.Time

	queueName string

	// The notification type associated with permit extensions
	notifyTypePermitExtension uint8

	// Don't allow task to run concurrently.
	mutex sync.Mutex

	// Prevent calls to Check while not online
	enabled    bool
	mutexCheck sync.Mutex
}

type permitCheck struct {
	permitId uint64
	created  time.Time
	maxAge   time.Duration
	response chan bool
}

type DatabaseQueueMonitorTaskConfig struct {
	QueueName                 string
	SweepAge                  time.Duration
	QueueStore                queue.QueueStore
	NotifyTypePermitExtension uint8
}

func NewDatabaseQueueMonitorTask(cfg DatabaseQueueMonitorTaskConfig) *DatabaseQueueMonitorTask {
	return &DatabaseQueueMonitorTask{
		sweepAge: cfg.SweepAge,
		cstore:   cfg.QueueStore,

		notifyTypePermitExtension: cfg.NotifyTypePermitExtension,
		queueName:                 cfg.QueueName,
	}
}

func (t *DatabaseQueueMonitorTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	defer func() {
		t.enabled = false
		t.mutexCheck.Unlock()
	}()

	// Subscribe to queue permit extension notifications
	sub := b.Subscribe(t.notifyTypePermitExtension)
	defer b.Unsubscribe(sub)

	// Channel is notified when sweeping to determine which queue permits are current
	t.check = make(chan permitCheck)
	defer close(t.check)

	// Ticker for periodically cleaning up map
	sweepTicker := time.NewTicker(time.Minute)
	defer sweepTicker.Stop()

	// Map that records heartbeats
	permitMap := make(map[uint64]time.Time)

	// Refresh the queue permits on boot to avoid any race condition of not having received an extension notification
	t.refreshPermitMap(ctx, permitMap)

	// Record start time
	t.started = time.Now()

	// Drain the check channel
	drain := func() {
		defer t.mutexCheck.Lock()
		select {
		case in := <-t.check:
			in.response <- false
		default:
		}
	}

	// Enable
	t.mutexCheck.Lock()
	t.enabled = true
	t.mutexCheck.Unlock()

	for {
		select {
		case <-ctx.Done():
			// Ensure `Check` cannot be called while shutting down
			drain()
			return
		case n := <-sub:
			if cn, ok := n.(*queue.QueuePermitExtendNotification); ok {
				permitMap[cn.PermitID] = time.Now()
			}
		case in := <-t.check:
			var response bool
			if t, ok := permitMap[in.permitId]; ok {
				// The map records the permit. Check to see if we have an unexpired heartbeat.
				if t.After(time.Now().Add(-in.maxAge)) {
					response = true
				}
			}
			// Don't expire a permit that was created recently
			if !response {
				if in.created.After(time.Now().Add(-in.maxAge)) {
					response = true
				}
			}
			// Don't expire the permit if this monitor task was recently started
			if !response {
				if t.started.After(time.Now().Add(-in.maxAge)) {
					response = true
				}
			}
			in.response <- response
		case <-sweepTicker.C:
			t.sweep(time.Now(), permitMap)
		}
	}
}

func (t *DatabaseQueueMonitorTask) refreshPermitMap(ctx context.Context, permitMap map[uint64]time.Time) {
	permits, err := t.cstore.QueuePermits(ctx, t.queueName)
	if err != nil {
		slog.Debug(fmt.Sprintf("Error: DatabaseQueueMonitorTask failed to refresh queue permits map"))
		return
	}

	for _, permit := range permits {
		if _, ok := permitMap[uint64(permit.PermitId())]; !ok {
			permitMap[uint64(permit.PermitId())] = time.Now()
		}
	}
}

func (t *DatabaseQueueMonitorTask) Check(ctx context.Context, permitId uint64, created time.Time, maxAge time.Duration) bool {
	t.mutexCheck.Lock()
	defer t.mutexCheck.Unlock()

	if !t.enabled {
		return false
	}

	r := make(chan bool)
	defer close(r)
	check := permitCheck{
		permitId: permitId,
		created:  created,
		maxAge:   maxAge,
		response: r,
	}

	// Attempt to send, but abort if context is cancelled.
	select {
	case <-ctx.Done():
		return false
	case t.check <- check:
	}

	// Attempt to receive, but abort if context is cancelled.
	select {
	case <-ctx.Done():
		return false
	case result := <-r:
		return result
	}
}

func (t *DatabaseQueueMonitorTask) sweep(now time.Time, permitMap map[uint64]time.Time) {
	old := make([]uint64, 0)
	for key, val := range permitMap {
		if !val.After(now.Add(-t.sweepAge)) {
			old = append(old, key)
		}
	}
	for _, key := range old {
		delete(permitMap, key)
	}
}
