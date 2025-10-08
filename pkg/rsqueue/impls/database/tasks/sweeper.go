package tasks

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database/dbqueuetypes"
)

type DatabaseQueueSweeper interface {
	Run(ctx context.Context)
}

// DatabaseQueueSweeperTask a task that checks existing queue permits and sweeps expired permits.
// Intended to be called by the task manager of your choice. This is a scheduled task that runs
// periodically when called by a scheduler.
type DatabaseQueueSweeperTask struct {
	store     dbqueuetypes.QueueStore
	queueName string
	monitor   DatabaseQueueMonitor

	// Sweep for items that have no heartbeat for this interval of time.
	sweepFor time.Duration
}

type DatabaseQueueSweeperTaskConfig struct {
	QueueName  string
	QueueStore dbqueuetypes.QueueStore
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
	var tx dbqueuetypes.QueueStore

	tx, err = q.store.BeginTransactionQueue("DatabaseQueueSweeperTask.Run")
	if err != nil {
		slog.Debug(fmt.Sprintf("Error sweeping for expired queue permits. Error getting permits: %s", err))
		return
	}
	defer tx.CompleteTransaction(&err)

	// Sweep for expired nodes
	permits, err := tx.QueuePermits(q.queueName)
	if err != nil {
		slog.Debug(fmt.Sprintf("Error sweeping for expired queue permits: %s", err))
		return
	}

	for _, permit := range permits {
		if !q.monitor.Check(ctx, uint64(permit.PermitId()), permit.PermitCreated(), q.sweepFor) {
			slog.Debug(fmt.Sprintf("Sweeping expired queue permit %d", permit.PermitId()))
			err := tx.QueuePermitDelete(permit.PermitId())
			if err != nil {
				slog.Debug(fmt.Sprintf("Error removing expired queue permit with id %d: %s", permit.PermitId(), err))
				return
			}
		}
	}
}
