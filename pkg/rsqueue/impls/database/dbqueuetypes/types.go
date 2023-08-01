package dbqueuetypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
)

type DebugLogger interface {
	Debugf(msg string, args ...interface{})
}

type QueuePermit interface {
	PermitId() permit.Permit
	PermitCreated() time.Time
}

type QueueGroupRecord interface {
	GroupId() int64
}

type TransactionCompleter interface {
	CompleteTransaction(err *error)
}

type DatabaseQueueChunkMatcher interface {
	Match(n listener.Notification, address string) bool
}

type QueueStore interface {
	TransactionCompleter
	BeginTransactionQueue(description string) (QueueStore, error)

	NotifyExtend(permit uint64) error

	// QueuePush pushes work into a queue. Pass a queue name, a priority (0 is the highest),
	// and some work.
	QueuePush(name string, groupId sql.NullInt64, priority, workType uint64, work interface{}, carrier []byte) error

	// QueuePushAddressed pushes work into a queue. Pass a queue name, a priority (0 is the highest),
	// a type, a unique address, and some work.
	QueuePushAddressed(name string, groupId sql.NullInt64, priority, workType uint64, address string, work interface{}, carrier []byte) error

	// QueuePop pops work from the queue. Pass a queue name and a maximum priority, along with
	// a []uint64 slice to indicate what types of work you're willing to grab.
	//
	// Returns a QueueWork pointer
	QueuePop(name string, maxPriority uint64, types []uint64) (*queue.QueueWork, error)

	// QueueDelete deletes claimed work from the queue. Called after the work is completed.
	QueueDelete(permitId permit.Permit) error

	// QueuePermits returns permits for a given queue
	QueuePermits(name string) ([]QueuePermit, error)

	// QueuePermitDelete clears a permit
	QueuePermitDelete(permitId permit.Permit) error

	// QueuePeek checks to see whether a queue has any outstanding work.
	// for a particular list of types.
	// Expects:
	//  * types - the work types
	// Returns:
	//  * results - the work
	//  * error - errors
	QueuePeek(types ...uint64) (results []queue.QueueWork, err error)

	// IsQueueAddressInProgress checks to see if an address is still in progress
	// Expects:
	//  * id - the queue item address
	// Returns:
	//  * bool - is the item still in progress?
	//  * error - errors
	IsQueueAddressInProgress(address string) (bool, error)
}

type QueueGroupStore interface {
	TransactionCompleter

	// QueueGroupStart marks a queue group as started. Work for this queue group
	// will not be retrieved by `QueuePop` until the group has
	// been marked as started
	QueueGroupStart(id int64) error

	// QueueGroupComplete checks to see if a queue group is complete/empty
	// Expects:
	//  * id - the queue group id
	// Returns:
	//  * bool - is the group complete/empty?
	//  * bool - was the group cancelled?
	//  * error - errors
	QueueGroupComplete(id int64) (bool, bool, error)

	// QueueGroupCancel cancels a queue group
	QueueGroupCancel(id int64) error

	// QueueGroupClear cancels/deletes the work in a queue group
	QueueGroupClear(id int64) error
}

// QueuePermitExtendNotification A notification that indicates a queue work permit extension
type QueuePermitExtendNotification struct {
	PermitID    uint64
	GuidVal     string
	MessageType uint8
}

func (n *QueuePermitExtendNotification) Type() uint8 {
	return n.MessageType
}

func (n *QueuePermitExtendNotification) Guid() string {
	return n.GuidVal
}

func NewQueuePermitExtendNotification(permitID uint64, notifyType uint8) *QueuePermitExtendNotification {
	return &QueuePermitExtendNotification{
		GuidVal:     uuid.New().String(),
		MessageType: notifyType,
		PermitID:    permitID,
	}
}
