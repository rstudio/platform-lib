package database

// Copyright (C) 2022 by Posit, PBC

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	agenttypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent/types"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database/dbqueuetypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/utils"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type QueueSuite struct {
	store *QueueTestStore
}

var _ = check.Suite(&QueueSuite{})

func (s *QueueSuite) SetUpSuite(c *check.C) {
	s.store = &QueueTestStore{}
}

func (s *QueueSuite) SetUpTest(c *check.C) {
	s.store.err = nil
	s.store.hasAddress = true
}

type fakeMetrics struct {
	count int
}

func (f *fakeMetrics) QueueNotificationMiss(name, address string) {
	f.count++
}

type QueueTestStore struct {
	llf            *local.ListenerProvider
	err            error
	polled         int
	poll           bool
	pollErr        error
	hasAddress     bool
	hasAddressErr  error
	enabled        []uint64
	permitsCalled  int
	permits        []dbqueuetypes.QueuePermit
	permitsErr     error
	permitsDeleted int
	permitDelete   error
	peek           []queue.QueueWork
	peekErr        error
}

func (s *QueueTestStore) BeginTransactionQueue(description string) (dbqueuetypes.QueueStore, error) {
	return s, nil
}

func (s *QueueTestStore) CompleteTransaction(err *error) {}

func (s *QueueTestStore) QueuePermits(name string) ([]dbqueuetypes.QueuePermit, error) {
	s.permitsCalled++
	return s.permits, s.permitsErr
}

func (s *QueueTestStore) QueuePermitDelete(permitID permit.Permit) error {
	if s.permitDelete == nil {
		s.permitsDeleted++
	}
	return s.permitDelete
}

func (s *QueueTestStore) QueuePeek(types ...uint64) (results []queue.QueueWork, err error) {
	return s.peek, s.peekErr
}

func (s *QueueTestStore) QueuePush(name string, groupId sql.NullInt64, priority, workType uint64, work interface{}, carrier []byte) error {
	return s.err
}

func (s *QueueTestStore) QueuePushAddressed(name string, groupId sql.NullInt64, priority, workType uint64, address string, work interface{}, carrier []byte) error {
	return s.err
}

func (s *QueueTestStore) QueuePop(name string, maxPriority uint64, types []uint64) (*queue.QueueWork, error) {
	// Simulate returning results on for maxPriority > 1
	if s.err == nil {
		s.enabled = types
	}
	if maxPriority > 1 && s.err == nil {
		return &queue.QueueWork{Permit: permit.Permit(34)}, nil
	} else if s.err == nil {
		return nil, sql.ErrNoRows
	}
	return nil, s.err
}

func (s *QueueTestStore) IsQueueAddressInProgress(address string) (bool, error) {
	return s.hasAddress, s.hasAddressErr
}

func (s *QueueTestStore) IsQueueAddressComplete(address string) (bool, error) {
	s.polled++
	return s.poll, s.pollErr
}

func (s *QueueTestStore) NotifyExtend(permit uint64) error {
	return s.err
}

func (s *QueueTestStore) QueueDelete(permit permit.Permit) error {
	return s.err
}

func (s *QueueTestStore) QueueAddressedComplete(address string, failure error) error {
	return s.err
}

func (s *QueueTestStore) GetLocalListenerProvider() *local.ListenerProvider {
	if s.llf != nil {
		return s.llf
	} else {
		return local.NewListenerProvider(local.ListenerProviderArgs{})
	}
}

type FakeWork struct{}

func (*FakeWork) Type() uint64 {
	return 0
}

type fakeChunkNotification struct {
	address string
}

func (f *fakeChunkNotification) Type() uint8 {
	return 0
}

func (f *fakeChunkNotification) Guid() string {
	return ""
}

type fakeMatcher struct{}

func (f *fakeMatcher) Match(n listener.Notification, address string) bool {
	if cn, ok := n.(*fakeChunkNotification); ok {
		return cn.address == address
	}
	return false
}

type fakeCarrierFactory struct{}

func (f *fakeCarrierFactory) GetCarrier(label, queueName, address string, priority, workType uint64, group int64) []byte {
	return []byte{}
}

type fakeWrapper struct{}

func (f *fakeWrapper) Start(ctx context.Context, work *queue.QueueWork) (context.Context, interface{}, error) {
	return ctx, nil, nil
}

func (f *fakeWrapper) Enqueue(queueName string, work queue.Work, err error) error {
	return nil
}

func (f *fakeWrapper) Dequeue(queueName string, work queue.Work, err error) error {
	return nil
}

func (f *fakeWrapper) Finish(data interface{}) {
}

func (s *QueueSuite) TestNewQueue(c *check.C) {
	msgs := make(chan listener.Notification)
	stop := make(chan bool)
	cf := &fakeCarrierFactory{}

	q, err := NewDatabaseQueue(DatabaseQueueConfig{
		QueueName:              "test",
		NotifyTypeWorkReady:    1,
		NotifyTypeWorkComplete: 2,
		NotifyTypeChunk:        3,
		ChunkMatcher:           &fakeMatcher{},
		CarrierFactory:         cf,
		QueueStore:             s.store,
		QueueMsgsChan:          msgs,
		WorkMsgsChan:           msgs,
		ChunkMsgsChan:          msgs,
		StopChan:               stop,
		JobLifecycleWrapper:    &fakeWrapper{},
	})
	c.Assert(err, check.IsNil)

	var typeQueue queue.Queue
	typeQueue = q
	_ = typeQueue
}

func (s *QueueSuite) TestRecord(c *check.C) {
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.RecordFailure("abc", errors.New("test"))
	c.Assert(err, check.IsNil)
	err = q.RecordFailure("abc", nil)
	c.Assert(err, check.IsNil)
}

func (s *QueueSuite) TestRecordErrs(c *check.C) {
	s.store.err = errors.New("kaboom!")
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.RecordFailure("abc", errors.New("test"))
	c.Assert(err, check.NotNil)
}

func (s *QueueSuite) TestPush(c *check.C) {
	q := &DatabaseQueue{
		store:          s.store,
		carrierFactory: &fakeCarrierFactory{},
		wrapper:        &fakeWrapper{},
	}
	err := q.Push(9, 0, &FakeWork{})
	c.Assert(err, check.IsNil)
}

func (s *QueueSuite) TestPeek(c *check.C) {
	peek := []queue.QueueWork{
		{
			Address:  "abc",
			WorkType: 5,
		},
		{
			Address:  "def",
			WorkType: 5,
		},
	}
	cstore := &QueueTestStore{
		peek:    peek,
		peekErr: errors.New("peek error"),
	}
	q := &DatabaseQueue{
		store:   cstore,
		wrapper: &fakeWrapper{},
	}

	// Error in store
	_, err := q.Peek(func(work *queue.QueueWork) (bool, error) { return true, nil }, 5)
	c.Assert(err, check.ErrorMatches, "peek error")

	// Error in filter
	cstore.peekErr = nil
	_, err = q.Peek(func(work *queue.QueueWork) (bool, error) {
		return false, errors.New("filter error")
	}, 5)
	c.Assert(err, check.ErrorMatches, "filter error")

	// Ok with filter
	results, err := q.Peek(func(work *queue.QueueWork) (bool, error) {
		return work.Address == "abc", nil
	}, 5)
	c.Assert(err, check.IsNil)
	c.Assert(results, check.DeepEquals, []queue.QueueWork{
		{
			Address:  "abc",
			WorkType: 5,
		},
	})
}

type FakeJob struct {
	// The job's runtime in ms
	Runtime int    `json:"runtime"`
	Tag     uint64 `json:"tag"`
}

func (*FakeJob) Type() uint64 {
	return 0
}

func (s *QueueSuite) TestGetAvailable(c *check.C) {
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	enabled := &queue.DefaultQueueSupportedTypes{}
	enabled.SetEnabled(3, true)
	enabled.SetEnabled(4, true)
	stop := make(chan bool)
	queueWork, err := q.Get(2, make(chan uint64), enabled, stop)
	c.Assert(err, check.IsNil)
	c.Check(queueWork.Permit, check.Equals, permit.Permit(34))
	c.Check(s.store.enabled, utils.SliceEquivalent, []uint64{3, 4})
}

func (s *QueueSuite) TestGetWait(c *check.C) {
	q := &DatabaseQueue{
		store:       s.store,
		subscribe:   make(chan broadcaster.Subscription),
		unsubscribe: make(chan (<-chan listener.Notification)),
		wrapper:     &fakeWrapper{},

		notifyTypeWorkReady: 9,
	}
	maxPriorityChan := make(chan uint64)
	changePriority := make(chan bool)
	priorityChanged := false

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	// Allows us to change the priority at a later time
	changeComplete := make(chan struct{})
	go func() {
		<-changePriority
		maxPriorityChan <- uint64(2)
		close(changeComplete)
	}()

	// Blocks on `q.Get` until the maxPriority is bumped
	// to at least 2.
	done := make(chan bool)
	go func() {
		enabled := &queue.DefaultQueueSupportedTypes{}
		stop := make(chan bool)
		queueWork, err := q.Get(1, maxPriorityChan, enabled, stop)
		c.Check(priorityChanged, check.Equals, true)
		c.Assert(err, check.IsNil)
		c.Check(queueWork.Permit, check.Equals, permit.Permit(34))
		done <- true
	}()

	// Signal that work is ready (but priority constraints are not yet met
	queueMsgs <- &listener.GenericNotification{
		NotifyGuid: uuid.New().String(),
		NotifyType: 9,
	}

	// Signal to change priority
	priorityChanged = true
	changePriority <- true
	//
	// Wait for completion
	<-changeComplete

	// Block until done, with a 5 second timeout
	var timeout error
	go func() {
		time.Sleep(5 * time.Second)
		timeout = errors.New("test timeout")
		done <- true
	}()
	<-done
	c.Assert(timeout, check.IsNil)
}

func (s *QueueSuite) TestExtend(c *check.C) {
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.Extend(permit.Permit(9))
	c.Assert(err, check.IsNil)
}

func (s *QueueSuite) TestDelete(c *check.C) {
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.Delete(permit.Permit(9))
	c.Assert(err, check.IsNil)
}

func (s *QueueSuite) TestPushErrs(c *check.C) {
	s.store.err = errors.New("push error")
	q := &DatabaseQueue{
		store:          s.store,
		carrierFactory: &fakeCarrierFactory{},
		wrapper:        &fakeWrapper{},
	}
	err := q.Push(9, 0, &FakeWork{})
	c.Assert(err, check.ErrorMatches, "push error")
}

func (s *QueueSuite) TestGetErrs(c *check.C) {
	s.store.err = errors.New("Kaboom!")
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	enabled := &queue.DefaultQueueSupportedTypes{}
	stop := make(chan bool)
	queueWork, err := q.Get(0, make(chan uint64), enabled, stop)
	c.Assert(err, check.NotNil)
	c.Check(queueWork, check.IsNil)
}

func (s *QueueSuite) TestExtendErrs(c *check.C) {
	s.store.err = errors.New("Kaboom!")
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.Extend(permit.Permit(9))
	c.Assert(err, check.NotNil)
}

func (s *QueueSuite) TestDeleteErrs(c *check.C) {
	s.store.err = errors.New("Kaboom!")
	q := &DatabaseQueue{
		store:   s.store,
		wrapper: &fakeWrapper{},
	}
	err := q.Delete(permit.Permit(9))
	c.Assert(err, check.NotNil)
}

func (s *QueueSuite) TestPollErr(c *check.C) {
	defer leaktest.Check(c)

	s.store.pollErr = errors.New("horrible error")
	q := &DatabaseQueue{
		store:               s.store,
		addressPollInterval: time.Millisecond * 5,
		subscribe:           make(chan broadcaster.Subscription),
		unsubscribe:         make(chan (<-chan listener.Notification)),
		wrapper:             &fakeWrapper{},
	}

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	errCh := q.PollAddress("something")

	err := <-errCh
	c.Check(err, check.ErrorMatches, "horrible error")

	<-errCh
	c.Assert(s.store.polled, check.Equals, 1)
}

func (s *QueueSuite) TestPollLockErr(c *check.C) {
	defer leaktest.Check(c)

	s.store.pollErr = errors.New("database is locked")
	q := &DatabaseQueue{
		store:               s.store,
		addressPollInterval: time.Millisecond * 5,
		subscribe:           make(chan broadcaster.Subscription),
		unsubscribe:         make(chan (<-chan listener.Notification)),
		wrapper:             &fakeWrapper{},
	}

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	errCh := q.PollAddress("something")

	go func() {
		time.Sleep(time.Millisecond * 30)
		s.store.pollErr = nil
		s.store.poll = true
	}()

	err := <-errCh
	c.Assert(err, check.IsNil)
	c.Assert(s.store.polled > 1, check.Equals, true)
}

func (s *QueueSuite) TestPollTickOk(c *check.C) {
	defer leaktest.Check(c)

	s.store.poll = false
	fm := &fakeMetrics{}
	q := &DatabaseQueue{
		store:               s.store,
		addressPollInterval: time.Millisecond * 50,
		subscribe:           make(chan broadcaster.Subscription),
		unsubscribe:         make(chan (<-chan listener.Notification)),
		wrapper:             &fakeWrapper{},
		metrics:             fm,
	}

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	errCh := q.PollAddress("something")

	go func() {
		time.Sleep(time.Millisecond * 30)
		s.store.poll = true
	}()

	<-errCh
	c.Assert(s.store.polled > 1, check.Equals, true)
	c.Assert(fm.count, check.Equals, 1)
}

func (s *QueueSuite) TestPollNotifyOk(c *check.C) {
	defer leaktest.Check(c)

	cstore := &QueueTestStore{}
	q := &DatabaseQueue{
		store:               cstore,
		addressPollInterval: time.Hour,
		subscribe:           make(chan broadcaster.Subscription),
		unsubscribe:         make(chan (<-chan listener.Notification)),
		wrapper:             &fakeWrapper{},

		notifyTypeWorkReady:    9,
		notifyTypeWorkComplete: 10,
		notifyTypeChunk:        11,
		chunkMatcher:           &fakeMatcher{},
	}

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	errCh := q.PollAddress("something")

	time.Sleep(10 * time.Millisecond)

	// This should not initiate a poll, since the address doesn't match
	workMsgs <- agenttypes.NewWorkCompleteNotification("nothing", 10)
	// This should initiate a poll
	workMsgs <- agenttypes.NewWorkCompleteNotification("something", 10)

	// Now the work is done
	// Sleep a bit to prevent a race when setting store.poll = true
	time.Sleep(100 * time.Millisecond)
	cstore.poll = true
	workMsgs <- agenttypes.NewWorkCompleteNotification("something", 10)

	<-errCh
	c.Assert(cstore.polled, check.Equals, 3)
}

func (s *QueueSuite) TestPollNotifyChunkOk(c *check.C) {
	defer leaktest.Check(c)

	cstore := &QueueTestStore{}
	q := &DatabaseQueue{
		store:               cstore,
		addressPollInterval: time.Hour,
		subscribe:           make(chan broadcaster.Subscription),
		unsubscribe:         make(chan (<-chan listener.Notification)),
		wrapper:             &fakeWrapper{},
		chunkMatcher:        &fakeMatcher{},
	}

	queueMsgs := make(chan listener.Notification)
	workMsgs := make(chan listener.Notification)
	chunkMsgs := make(chan listener.Notification)
	defer close(queueMsgs)
	defer close(workMsgs)
	defer close(chunkMsgs)

	stopper := make(chan bool)
	defer func() { stopper <- true }()
	go q.broadcast(stopper, queueMsgs, workMsgs, chunkMsgs)

	errCh := q.PollAddress("something")

	time.Sleep(10 * time.Millisecond)

	// This should not initiate a poll, since the address doesn't match
	chunkMsgs <- &fakeChunkNotification{address: "nothing"}

	// A chunk notification with a matching address should result in
	// closing errCh and returning since at least one chunk is now available
	// and any consumers can start reading.
	chunkMsgs <- &fakeChunkNotification{address: "something"}

	<-errCh
	c.Assert(cstore.polled, check.Equals, 1)
}

func (s *QueueSuite) TestHasAddressError(c *check.C) {
	cstore := &QueueTestStore{
		hasAddressErr: errors.New("check address error"),
	}
	q := &DatabaseQueue{
		store:   cstore,
		wrapper: &fakeWrapper{},
	}
	_, err := q.IsAddressInQueue("something")
	c.Assert(err, check.ErrorMatches, "check address error")
}

func (s *QueueSuite) TestHasAddressOk(c *check.C) {
	cstore := &QueueTestStore{
		hasAddress: true,
	}
	q := &DatabaseQueue{
		store:   cstore,
		wrapper: &fakeWrapper{},
	}
	has, err := q.IsAddressInQueue("something")
	c.Assert(err, check.IsNil)
	c.Assert(has, check.Equals, true)
}
