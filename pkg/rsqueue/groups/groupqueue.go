package groups

// Copyright (C) 2025 By Posit Software, PBC

import (
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

/*
 * Introduces two interfaces, GroupQueueFactory and GroupQueue,
 * along with default implementations of each. Essentially, these
 * are helpers that help you insert data into a queue group and
 * then monitor the queue group for completion.
 *
 * This is useful for operations that need to complete a set of
 * queued work, where you are inserting a bunch of discreet work into
 * the queue, but you want to know when the entire set of work
 * is done.
 *
 * NOTE: Don't call `Start()` until you've inserted all the work
 * that will be handled by this queue group.
 *
 * Example:
 *
 * // Create a new group queue
 * group, err := groupQueue.NewGroup("name")
 * if err != nil {
 * 	 return err
 * }
 *
 * // Push some work into the group queue
 * group.Push(0, work)
 * group.Push(1, work)
 *
 * // After all work has been pushed into the queue, start
 * // monitoring it for completion
 * group.Start()
 */

// GroupQueue is the outer wrapper of the collection of other queues. It behaves
//
//	more or less like a DatabaseQueue. If you're looking for the raw queue underneath,
//	see the QueueGroup struct.
type GroupQueue interface {
	// Push pushes work into the base queue
	Push(priority uint64, work queue.Work) error

	// SetEndWork sets the work to run when the group ends
	SetEndWork(work interface{}, endWorkType uint8) error

	// Start starts monitoring the base queue for completion
	// of all queued work.
	Start() error

	// Group returns the group info
	Group() GroupQueueJob

	// BaseQueueName returns the base queue name
	BaseQueueName() string
}

// GroupQueueJob defines the lifecycle for a group of work in the queue.
type GroupQueueJob interface {
	Type() uint64
	GroupId() int64
	Name() string
	Flag() string
	EndWorkType() uint8
	EndWork() GroupQueueJob
	EndWorkJob() []byte
	AbortWork() GroupQueueJob
	CancelWork() GroupQueueJob
	SetEndWork(endWorkType uint8, work []byte)
}

// GroupQueueProvider links the group queue runner to a queue implementation
type GroupQueueProvider interface {
	// IsReady is called to determine when it is time to start a group of work.
	// It returns nil when it is time to start the work in a group.
	IsReady(job GroupQueueJob) error

	// IsComplete is called to determine all the work in a group has been
	// successfully completed.
	IsComplete(job GroupQueueJob) (cancelled bool, err error)

	// Begin is called when the group begins handling its work.
	Begin(job GroupQueueJob) error

	// Cancel is called when a group of work is cancelled.
	Cancel(job GroupQueueJob) error

	// Abort is called for finalization after handling a group cancellation.
	Abort(job GroupQueueJob) error

	// Clear is called to clear the work from a group. This is called upon
	// failure or cancellation.
	Clear(job GroupQueueJob) error

	// Fail is called when a group of work fails.
	Fail(job GroupQueueJob, err error) error
}
