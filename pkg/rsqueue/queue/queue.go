package queue

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"sync"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
)

var ErrDuplicateAddressedPush = errors.New("Duplicate address")

type Queue interface {
	// WithDbTx returns a queue that operates in the context of a database
	// transaction.
	WithDbTx(tx interface{}) Queue

	// Peek at work in the queue
	// * types the types of work to peek at
	Peek(filter func(work *QueueWork) (bool, error), types ...uint64) ([]QueueWork, error)

	// Push new work into the queue
	//  * priority the priority for this work. Lower number are higher priority
	//  * groupId the group id for this work. Use zero (0) if not grouped
	//  * work the work to push into the queue. Must be JSON-serializable
	Push(priority uint64, groupId int64, work Work) error

	// AddressedPush pushes uniquely addressed new work into the queue
	//  * priority the priority for this work. Lower number are higher priority
	//  * groupId the group id for this work. Use zero (0) if not grouped
	//  * address the address for this work. Must be unique. Address can be reused,
	//      but only one occurrence of any address may be in the queue at any time.
	//  * work the work to push into the queue. Must be JSON-serializable
	AddressedPush(priority uint64, groupId int64, address string, work Work) error

	// RecordFailure records a failure of addressed work in the queue
	//  * address the address of the work the failed
	//  * failure the error that occurred. Overwrites any previous error information
	//    from earlier runs of the same work. If `failure==nil`, the error is cleared.
	RecordFailure(address string, failure error) error

	// PollAddress polls addressed work in the queue, and an `errs` channels
	// to report when the work is done and/or an error has occurred. We pass
	// `nil` over the errs channel when the poll has completed without errors
	PollAddress(address string) (errs <-chan error)

	// IsAddressInQueue checks to see if work with the provided address is in the queue
	IsAddressInQueue(address string) (bool, error)

	// Get attempts to get a job from the queue. Blocks until a job is found and returned
	// Parameters:
	//  * maxPriority uint64 - get only jobs with priority <= this value.
	//  * maxPriorityChan chan uint64 - while blocking (waiting for a job) pass a new
	//     maxPriority value to this channel if the capacity changes. For example, we
	//     may be blocking (waiting for work) with a maximum priority of 2; when other
	//     in-progress work completes, we may suddenly have capacity for work with a
	//     maximum priority of 4. In this case, we'd pass a `4` on this channel to notify
	//     the store that we can change our polling query to allow a new maximum priority.
	//  * types QueueSupportedTypes - used to indicate what types of work we are willing
	//     to grab from the queue. During shutdown, we may be willing to grab certain
	//     types of work necessary to drain the queue while ignoring other types of work.
	//  * stop - notified when terminating
	// Returns:
	//  * *QueueWork - A pointer to a QueueWork struct
	//  * error - An error (if error occurs); nil if successful
	Get(maxPriority uint64, maxPriorityChan chan uint64, types QueueSupportedTypes, stop chan bool) (*QueueWork, error)

	// Extend (heartbeat) a queue permit while work is in progress.
	Extend(permit.Permit) error

	// Delete a queue permit and its associated work. This is typically called when
	// the work is complete.
	Delete(permit.Permit) error

	// Name returns the name of the queue.
	Name() string
}

type QueueSupportedTypes interface {
	Enabled() []uint64
	SetEnabled(typeId uint64, enabled bool)
	SetEnabledConditional(typeId uint64, enabled func() bool)
	DisableAll()
}

type Enabled struct {
	Always      bool
	Conditional func() bool
}

type DefaultQueueSupportedTypes struct {
	types map[uint64]Enabled
	mutex sync.RWMutex
}

func (d *DefaultQueueSupportedTypes) Enabled() []uint64 {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	results := make([]uint64, 0)
	for i, enabled := range d.types {
		if enabled.Always || (enabled.Conditional != nil && enabled.Conditional()) {
			results = append(results, i)
		}
	}
	return results
}

func (d *DefaultQueueSupportedTypes) SetEnabled(typeId uint64, enabled bool) {
	if d.types == nil {
		d.types = make(map[uint64]Enabled)
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.types[typeId] = Enabled{Always: enabled}
}

func (d *DefaultQueueSupportedTypes) SetEnabledConditional(typeId uint64, enabled func() bool) {
	if d.types == nil {
		d.types = make(map[uint64]Enabled)
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.types[typeId] = Enabled{Conditional: enabled}
}

func (d *DefaultQueueSupportedTypes) DisableAll() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	for i := range d.types {
		d.types[i] = Enabled{Always: false}
	}
}

type QueueWork struct {
	// A permit (uint64) for doing the work and heartbeating
	Permit permit.Permit

	// The work's address, if addressed. Blank if not addressed.
	Address string

	// The work type
	WorkType uint64

	// A byte array representing the work that can be unmarshaled to JSON
	Work []byte

	// A context for tracking recursive work
	Context context.Context

	// Byte array for persisting tracing data across the work lifecycle.
	Carrier []byte
}

func (w *QueueWork) Type() uint64 {
	return w.WorkType
}

// Used for queue work with an address. If your queue work has an address, and
// if the work's `Run` method returns a *QueueError pointer, then the queue
// `Agent` will record the message and code in the database. If the work's
// `Run` method returns a generic error, then the message will be recorded, but
// no code will be recorded. Used by `PollAddress` to return more detailed error
// information when polling for an address to complete. For example, see
// `/services/package/current.go`. This service checks for a non-zero error code,
// which most likely results in a 404 to the browser vs. a 500.
type QueueError struct {
	// HTTP error code to use if this error is returned by a service
	Code    int    `json:"code,omitempty"`
	Message string `json:"message"`
}

func (q *QueueError) Error() string {
	return q.Message
}
