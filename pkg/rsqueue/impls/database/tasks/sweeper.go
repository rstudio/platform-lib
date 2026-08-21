package tasks

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rsqueue/queue"
)

type DatabaseQueueSweeper interface {
	Run(ctx context.Context)
}

// DatabaseQueueSweeperTask a task that checks existing queue permits and sweeps expired permits.
// Intended to be called by the task manager of your choice. This is a scheduled task that runs
// periodically when called by a scheduler.
type DatabaseQueueSweeperTask struct {
	store     queue.QueueStore
	queueName string
	monitor   DatabaseQueueMonitor

	// Sweep for items that have no heartbeat for this interval of time.
	sweepFor time.Duration
}

type DatabaseQueueSweeperTaskConfig struct {
	QueueName  string
	QueueStore queue.QueueStore
	SweepFor   time.Duration
	Monitor    DatabaseQueueMonitor
}

func NewDatabaseQueueSweeperTask(cfg DatabaseQueueSweeperTaskConfig) *DatabaseQueueSweeperTask {
	return &DatabaseQueueSweeperTask{
		queueName: cfg.QueueName,
		store:     cfg.QueueStore,
		monitor:   cfg.Monitor,
		sweepFor:  cfg.SweepFor,
	}
}

func (q *DatabaseQueueSweeperTask) Run(ctx context.Context) {
	var err error
	var tx queue.QueueStore

	tx, err = q.store.BeginTransactionQueue(ctx, "DatabaseQueueSweeperTask.Run")
	if err != nil {
		slog.Debug(fmt.Sprintf("Error sweeping for expired queue permits. Error getting permits: %s", err))
		return
	}
	defer tx.CompleteTransaction(&err)

	// Sweep for expired nodes
	permits, err := tx.QueuePermits(ctx, q.queueName)
	if err != nil {
		slog.Debug(fmt.Sprintf("Error sweeping for expired queue permits: %s", err))
		return
	}

	for _, permit := range permits {
		if !q.monitor.Check(ctx, uint64(permit.PermitId()), permit.PermitCreated(), q.sweepFor) {
			slog.Debug(fmt.Sprintf("Sweeping expired queue permit %d", permit.PermitId()))
			// Assign to the outer `err`, do not redeclare it. The deferred
			// CompleteTransaction above reads that variable to decide whether to commit
			// or roll back, so a `:=` here would leave it nil and commit the permits
			// deleted before the failure while abandoning the rest. That left the queue
			// in a state no caller asked for: a partially swept set of permits, reported
			// as a clean sweep.
			err = tx.QueuePermitDelete(ctx, permit.PermitId())
			if err != nil {
				slog.Debug(fmt.Sprintf("Error removing expired queue permit with id %d: %s", permit.PermitId(), err))
				return
			}
		}
	}
}
