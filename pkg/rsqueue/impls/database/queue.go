package database

// Copyright (C) 2022 by RStudio, PBC

import (
	"database/sql"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsqueue/agent"
	agenttypes "github.com/rstudio/platform-lib/pkg/rsqueue/agent/types"
	"github.com/rstudio/platform-lib/pkg/rsqueue/impls/database/dbqueuetypes"
	"github.com/rstudio/platform-lib/pkg/rsqueue/metrics"
	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	queuetypes "github.com/rstudio/platform-lib/pkg/rsqueue/types"
	"github.com/rstudio/platform-lib/pkg/rsqueue/utils"
)

type DatabaseQueue struct {
	debugLogger dbqueuetypes.DebugLogger

	carrierFactory metrics.CarrierFactory

	// The name of the queue. Each agent polls one named queue
	name string

	// A store
	store dbqueuetypes.QueueStore

	// Poll for addressed item completion at this interval
	addressPollInterval time.Duration

	// Used by the queue's internal broadcaster
	subscribe   chan broadcaster.Subscription
	unsubscribe chan (<-chan listener.Notification)

	// Define notifications to use
	leaderChannel          string
	notifyTypeWorkReady    uint8
	notifyTypeWorkComplete uint8
	notifyTypeChunk        uint8

	// Determines when a relevant chunk notification is received.
	chunkMatcher dbqueuetypes.DatabaseQueueChunkMatcher

	wrapper metrics.JobLifecycleWrapper
}

type DatabaseQueueConfig struct {
	QueueName              string
	NotifyTypeWorkReady    uint8
	NotifyTypeWorkComplete uint8
	NotifyTypeChunk        uint8
	ChunkMatcher           dbqueuetypes.DatabaseQueueChunkMatcher
	DebugLogger            dbqueuetypes.DebugLogger
	CarrierFactory         metrics.CarrierFactory
	QueueStore             dbqueuetypes.QueueStore
	QueueMsgsChan          <-chan listener.Notification
	WorkMsgsChan           <-chan listener.Notification
	ChunkMsgsChan          <-chan listener.Notification
	StopChan               chan bool
	JobLifecycleWrapper    metrics.JobLifecycleWrapper
}

func NewDatabaseQueue(cfg DatabaseQueueConfig) (queue.Queue, error) {
	rq := &DatabaseQueue{
		name:        cfg.QueueName,
		debugLogger: cfg.DebugLogger,
		store:       cfg.QueueStore,

		carrierFactory: cfg.CarrierFactory,

		notifyTypeWorkReady:    cfg.NotifyTypeWorkReady,
		notifyTypeWorkComplete: cfg.NotifyTypeWorkComplete,
		notifyTypeChunk:        cfg.NotifyTypeChunk,
		chunkMatcher:           cfg.ChunkMatcher,

		addressPollInterval: 5 * time.Second,

		subscribe:   make(chan broadcaster.Subscription),
		unsubscribe: make(chan (<-chan listener.Notification)),

		wrapper: cfg.JobLifecycleWrapper,
	}

	go rq.broadcast(cfg.StopChan, cfg.QueueMsgsChan, cfg.WorkMsgsChan, cfg.ChunkMsgsChan)

	return rq, nil
}

func (q *DatabaseQueue) WithDbTx(tx interface{}) queue.Queue {
	return &DatabaseQueue{
		name:                q.name,
		store:               tx.(dbqueuetypes.QueueStore),
		addressPollInterval: q.addressPollInterval,
		subscribe:           q.subscribe,
		unsubscribe:         q.unsubscribe,
	}
}

// The queue uses its own internal broadcaster. It's important that we always
// listen to queue notifications (from `q.msgs`) since the queue can deadlock
// if we're not listening. This broadcaster ALWAYS listens to these messages
// and discards them if the queue isn't waiting for a notification. The `Get`
// method subscribes and unsubscribes from these messages at will.
//
// This broadcaster is a simplified version of
// `github.com/rstudio/platform-lib/pkg/rsnotify/broadcaster`.
func (q *DatabaseQueue) broadcast(stop chan bool, queueMsgs, workMsgs, chunkMsgs <-chan listener.Notification) {
	defer close(stop)
	sinks := make([]broadcaster.Subscription, 0)
	for {
		select {
		case <-stop:
			q.stop(sinks)
			return
		case msg, more := <-queueMsgs:
			if more {
				sinks = notify(msg, q.notifyTypeWorkReady, sinks, 0)
			}
		case msg, more := <-workMsgs:
			if more {
				sinks = notify(msg, q.notifyTypeWorkComplete, sinks, 300*time.Millisecond)
			}
		case msg, more := <-chunkMsgs:
			if more {
				sinks = notify(msg, q.notifyTypeChunk, sinks, 300*time.Millisecond)
			}
		case sink := <-q.subscribe:
			sinks = append(sinks, sink)
		case sink := <-q.unsubscribe:
			for i, c := range sinks {
				if c.C == sink {
					sinks = append(sinks[:i], sinks[i+1:]...)
					close(c.C)
				}
			}
		}
	}
}

func notify(msg listener.Notification, dataType uint8, sinks []broadcaster.Subscription, timeout time.Duration) []broadcaster.Subscription {
	var needFilter bool
	for i, sink := range sinks {
		if sink.T == dataType {
			if sink.One != nil {
				// When the `SubscribeOne` method is used to register a listener, we
				// pass only one message over the channel. We filter via the `sink.One` method
				// which is defined by the call to `SubscribeOne`. If this method returns
				// `true`, then we broadcast the message to the subscribe and immediately
				// unsubscribe the subscriber.
				if sink.One(msg) {
					send(msg, sink.C, timeout)
					// Close the sink channel and mark the sink as used so it is
					// unsubscribed. Setting `needFilter = true` avoids multiple calls
					// to `Filter` by deferring the call to after all messages are
					// passed.
					close(sink.C)
					sinks[i].Used = true
					needFilter = true
				}
			} else {
				send(msg, sink.C, timeout)
			}
		}
	}
	// Remove used sinks, if needed. This removes any subscriptions that were created
	// via `SubscribeOne` that have just received their requested messages.
	if needFilter {
		sinks = broadcaster.Filter(sinks)
	}
	return sinks
}

func send(msg listener.Notification, ch chan listener.Notification, timeout time.Duration) {
	timeoutCh := make(<-chan time.Time)
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}
	select {
	case ch <- msg:
	case <-timeoutCh:
	}
}

// SubscribeOne returns a new output channel that will receive one and only one broadcast
// event when the provided Matcher returns `true`. When an event matches the Matcher,
// the event is passed over the output channel and the channel is immediately
// unsubscribed. You should still call `Unsubscribe` with the channel in case an event
// is never received.
func (q *DatabaseQueue) SubscribeOne(dataType uint8, matcher broadcaster.Matcher) <-chan listener.Notification {
	c := make(chan listener.Notification)

	q.subscribe <- broadcaster.Subscription{
		C:   c,
		T:   dataType,
		One: matcher,
	}

	return c
}

// Unsubscribe removes a channel from receiving broadcast events. That channel is
// closed as a consequence of unsubscribing.
func (q *DatabaseQueue) Unsubscribe(ch <-chan listener.Notification) {
	drainer := func() {
		for {
			_, more := <-ch
			if !more {
				return
			}
		}
	}
	go drainer()
	q.unsubscribe <- ch
}

// Stop the broadcaster safely.
func (q *DatabaseQueue) stop(sinks []broadcaster.Subscription) {
	for _, sink := range sinks {
		close(sink.C)
	}
}

func (q *DatabaseQueue) AddressedPush(priority uint64, groupId int64, address string, work queue.Work) error {
	group := sql.NullInt64{Int64: groupId, Valid: groupId > 0}
	c := q.carrierFactory.GetCarrier("addressed-queue-push", q.name, address, priority, work.Type(), groupId)
	err := q.store.QueuePushAddressed(q.name, group, priority, work.Type(), address, work, c)
	_ = q.wrapper.Enqueue(q.name, work, err)
	return err
}

func (q *DatabaseQueue) IsAddressInQueue(address string) (bool, error) {
	return q.store.IsQueueAddressInProgress(address)
}

func (q *DatabaseQueue) PollAddress(address string) <-chan error {
	errCh := make(chan error)

	go func() {
		for {
			queued, err := q.IsAddressInQueue(address)
			if err != nil {
				// Ignore lock errors
				if !utils.IsSqliteLockError(err) {
					errCh <- err
					close(errCh)
					return
				}
			} else if !queued {
				q.debugLogger.Debugf("Queue work with address %s completed", address)
				close(errCh)
				return
			}

			// Wait for a notification or an interval, then poll again
			done, err := func() (bool, error) {
				completedMsgs := q.SubscribeOne(q.notifyTypeWorkComplete, func(n listener.Notification) bool {
					if wn, ok := n.(*agenttypes.WorkCompleteNotification); ok {
						return wn.Address == address
					}
					return false
				})
				chunkMsgs := q.SubscribeOne(q.notifyTypeChunk, func(n listener.Notification) bool {
					return q.chunkMatcher.Match(n, address)
				})
				defer q.Unsubscribe(completedMsgs)
				defer q.Unsubscribe(chunkMsgs)

				tick := time.NewTicker(q.addressPollInterval)
				defer tick.Stop()
				for {
					select {
					case n := <-completedMsgs:
						q.debugLogger.Debugf("Queue was notified that work with address %s completed", address)
						if wn, ok := n.(*agenttypes.WorkCompleteNotification); ok {
							return true, wn.Error
						}
					case <-chunkMsgs:
						q.debugLogger.Debugf("Queue was notified that chunk with address %s is ready", address)
						return true, nil
					case <-tick.C:
						return false, nil
					}
				}
			}()
			// If we received a chunk or work complete notification, then we return immediately so the client can
			// continue.
			if done {
				if err != nil {
					errCh <- err
				}
				close(errCh)
				return
			}
		}
	}()
	return errCh
}

func (q *DatabaseQueue) Push(priority uint64, groupId int64, work queue.Work) error {
	group := sql.NullInt64{}
	if groupId > 0 {
		group = sql.NullInt64{Int64: groupId, Valid: true}
	}
	c := q.carrierFactory.GetCarrier("queue-push", q.name, "", priority, work.Type(), groupId)
	err := q.store.QueuePush(q.name, group, priority, work.Type(), work, c)
	_ = q.wrapper.Enqueue(q.name, work, err)
	return err
}

// Get attempts to get a job from the queue. Blocks until a job is found and returned
// Parameters:
//   - maxPriority uint64 - get only jobs with priority <= this value.
//   - maxPriorityChan chan uint64 - while blocking (waiting for a job) pass a new
//     maxPriority value to this channel if the capacity changes. For example, we
//     may be blocking (waiting for work) with a maximum priority of 2; when other
//     in-progress work completes, we may suddenly have capacity for work with a
//     maximum priority of 4. In this case, we'd pass a `4` on this channel to notify
//     the store that we can change our polling query to allow a new maximum priority.
//   - work - returns work via a pointer
//
// Returns:
//   - Permit - A permit (uint64) for doing the work
//   - Address - The work's address
//   - error - An error (if error occurs); nil if successful
func (q *DatabaseQueue) Get(maxPriority uint64, maxPriorityChan chan uint64, types queue.QueueSupportedTypes, stop chan bool) (*queue.QueueWork, error) {

	start := time.Now()
	q.debugLogger.Debugf("Queue Get() started")

	// First, try to get a job to avoid waiting for a tick
	// if jobs are waiting
	queueWork, err := q.store.QueuePop(q.name, maxPriority, types.Enabled())
	defer func(queueWork *queue.QueueWork) {
		if queueWork != nil {
			q.debugLogger.Debugf("Queue Get() for work type %d at address %s returned in %d ms", queueWork.WorkType, queueWork.Address, time.Now().Sub(start).Nanoseconds()/1000000)
		} else {
			q.debugLogger.Debugf("Queue Get() returned in %d ms", time.Now().Sub(start).Nanoseconds()/1000000)
		}
	}(queueWork)
	if err != sql.ErrNoRows {
		q.measureDequeue(queueWork, err)
		return queueWork, err
	}

	// If no jobs were waiting, then we loop and wait for a job.
	for {
		// The select is wrapped in a function so we can efficiently call `q.Unsubscribe`
		// immediately before attempting to pop from the queue.
		err = func() error {
			qAvail := q.SubscribeOne(q.notifyTypeWorkReady, func(n listener.Notification) bool {
				return n != nil
			})
			defer q.Unsubscribe(qAvail)
			select {
			case <-stop:
				return agent.ErrAgentStopped
			case priority := <-maxPriorityChan:
				if priority != maxPriority {
					q.debugLogger.Debugf("Priority changed via channel from %d to %d.\n", maxPriority, priority)
					maxPriority = priority
				}
			case n := <-qAvail:
				q.debugLogger.Debugf("Notification received: queue ready for processing: %s.", n.Guid())
			}
			return nil
		}()
		if err != nil {
			return queueWork, err
		}
		queueWork, err = q.store.QueuePop(q.name, maxPriority, types.Enabled())
		if err != sql.ErrNoRows {
			q.measureDequeue(queueWork, err)
			return queueWork, err
		}
	}
}

func (q *DatabaseQueue) Extend(permit permit.Permit) error {
	return q.store.NotifyExtend(uint64(permit))
}

func (q *DatabaseQueue) Delete(permit permit.Permit) error {
	return q.store.QueueDelete(permit)
}

func (q *DatabaseQueue) Name() string {
	return q.name
}

func (q *DatabaseQueue) Peek(filter func(work *queue.QueueWork) (bool, error), types ...uint64) ([]queue.QueueWork, error) {
	work, err := q.store.QueuePeek(types...)
	if err != nil {
		return nil, err
	}

	results := make([]queue.QueueWork, 0)
	for _, w := range work {
		var ok bool
		ok, err = filter(&w)
		if err != nil {
			return nil, err
		}
		if ok {
			results = append(results, w)
		}
	}

	return results, nil
}

func (q *DatabaseQueue) measureDequeue(queueWork *queue.QueueWork, err error) {
	if queueWork == nil {
		queueWork = &queue.QueueWork{WorkType: queuetypes.TYPE_NONE}
	}
	_ = q.wrapper.Dequeue(q.name, queueWork, err)
}
