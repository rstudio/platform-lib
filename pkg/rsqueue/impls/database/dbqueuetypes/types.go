package dbqueuetypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

type QueuePermit interface {
	PermitId() permit.Permit
	PermitCreated() time.Time
}

type QueueGroupRecord interface {
	GroupId() int64
}

type TransactionCompleter interface {
	CompleteTransaction(ctx context.Context, err *error)
}

type DatabaseQueueChunkMatcher interface {
	Match(n listener.Notification, address string) bool
}

type QueueStore interface {
	TransactionCompleter
	BeginTransactionQueue(ctx context.Context, description string) (QueueStore, error)

	NotifyExtend(ctx context.Context, permit uint64) error

	// QueuePush pushes work into a queue. Pass a queue name, a priority (0 is the highest),
	// and some work.
	QueuePush(ctx context.Context, name string, groupId sql.NullInt64, priority, workType uint64, work interface{}, carrier []byte) error

	// QueuePushAddressed pushes work into a queue. Pass a queue name, a priority (0 is the highest),
	// a type, a unique address, and some work.
	QueuePushAddressed(ctx context.Context, name string, groupId sql.NullInt64, priority, workType uint64, address string, work interface{}, carrier []byte) error

	// QueuePop pops work from the queue. Pass a queue name and a maximum priority, along with
	// a []uint64 slice to indicate what types of work you're willing to grab.
	//
	// Returns a QueueWork pointer
	QueuePop(ctx context.Context, name string, maxPriority uint64, types []uint64) (*queue.QueueWork, error)

	// QueueDelete deletes claimed work from the queue. Called after the work is completed.
	QueueDelete(ctx context.Context, permitId permit.Permit) error

	// QueuePermits returns permits for a given queue
	QueuePermits(ctx context.Context, name string) ([]QueuePermit, error)

	// QueuePermitDelete clears a permit
	QueuePermitDelete(ctx context.Context, permitId permit.Permit) error

	// QueuePeek checks to see whether a queue has any outstanding work.
	// for a particular list of types.
	// Expects:
	//  * types - the work types
	// Returns:
	//  * results - the work
	//  * error - errors
	QueuePeek(ctx context.Context, types ...uint64) (results []queue.QueueWork, err error)

	// IsQueueAddressComplete checks to see if an address is done/gone
	// Expects:
	//  * id - the queue item address
	// Returns:
	//  * bool - is the item done/gone?
	//  * error - errors
	IsQueueAddressComplete(ctx context.Context, address string) (bool, error)

	// IsQueueAddressInProgress checks to see if an address is still in progress
	// Expects:
	//  * id - the queue item address
	// Returns:
	//  * bool - is the item still in progress?
	//  * error - errors
	IsQueueAddressInProgress(ctx context.Context, address string) (bool, error)

	// QueueAddressedComplete saves or clears failure information for an addressed item
	QueueAddressedComplete(ctx context.Context, address string, failure error) error
}

type QueueGroupStore interface {
	TransactionCompleter

	// QueueGroupStart marks a queue group as started. Work for this queue group
	// will not be retrieved by `QueuePop` until the group has
	// been marked as started
	QueueGroupStart(ctx context.Context, id int64) error

	// QueueGroupComplete checks to see if a queue group is complete/empty
	// Expects:
	//  * id - the queue group id
	// Returns:
	//  * bool - is the group complete/empty?
	//  * bool - was the group cancelled?
	//  * error - errors
	QueueGroupComplete(ctx context.Context, id int64) (bool, bool, error)

	// QueueGroupCancel cancels a queue group
	QueueGroupCancel(ctx context.Context, id int64) error

	// QueueGroupClear cancels/deletes the work in a queue group
	QueueGroupClear(ctx context.Context, id int64) error
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
