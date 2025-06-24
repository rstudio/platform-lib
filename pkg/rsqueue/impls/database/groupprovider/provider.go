package groupprovider

// Copyright (C) 2022 by Posit, PBC

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/groups"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database/dbqueuetypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/utils"
)

type QueueGroupProvider struct {
	cstore dbqueuetypes.QueueGroupStore

	// Interval at which to poll for the group status. We poll periodically
	// to see if the queue group is complete.
	//
	// TODO: switch to notifications.
	pollInterval time.Duration
}

type QueueGroupProviderConfig struct {
	Store dbqueuetypes.QueueGroupStore
}

func NewQueueGroupProvider(cfg QueueGroupProviderConfig) *QueueGroupProvider {
	return &QueueGroupProvider{
		cstore:       cfg.Store,
		pollInterval: 2 * time.Second,
	}
}

func (p *QueueGroupProvider) IsComplete(job groups.GroupQueueJob) (cancelled bool, err error) {
	// Wait for group to complete
	groupDone, groupErr := p.poll(job)
	select {
	case err = <-groupErr:
		return
	case cancelled = <-groupDone:
		return
	}
}

func (p *QueueGroupProvider) Begin(job groups.GroupQueueJob) error {
	// Flag group as started
	return p.cstore.QueueGroupStart(job.GroupId())
}

func (p *QueueGroupProvider) Cancel(job groups.GroupQueueJob) error {
	// This will mark the queue group as `cancelled` and allow for re-runs of the same group later.
	// When this occurs, it means that the GroupRunner did not receive a `QueueGroupFlagCancel` job.Flag
	// so we also call the `Fail` method to ensure that any logic for recording failure runs.
	return p.cstore.QueueGroupCancel(job.GroupId())
}

func (p *QueueGroupProvider) Clear(job groups.GroupQueueJob) error {
	// Remove the queued work from the database to prevent jobs from restarting after the group runner finishes.
	return p.cstore.QueueGroupClear(job.GroupId())
}

// Poll polls the store
func (p *QueueGroupProvider) poll(job groups.GroupQueueJob) (done chan bool, errCh chan error) {
	done = make(chan bool)
	errCh = make(chan error)

	go func() {
		for {
			isDone, cancelled, err := p.cstore.QueueGroupComplete(job.GroupId())
			if utils.IsSqliteLockError(err) {
				slog.Debug(fmt.Sprintf("Queue Group Poll() lock error: %s. Waiting to retry.", err))
			} else if err != nil {
				errCh <- err
				return
			} else if isDone {
				done <- cancelled
				close(done)
				return
			}

			// Sleep for 2 seconds before polling again
			time.Sleep(p.pollInterval)
		}
	}()
	return
}
