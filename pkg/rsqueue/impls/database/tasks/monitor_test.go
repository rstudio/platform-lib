package tasks

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type fakePermit struct {
	permitId permit.Permit
}

func (f *fakePermit) PermitId() permit.Permit {
	return f.permitId
}

func (*fakePermit) PermitCreated() time.Time {
	return time.Time{}
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
	permits        []queue.QueuePermit
	permitsErr     error
	permitsDeleted int
	permitDelete   error
	peek           []queue.QueueWork
	peekErr        error
}

func (s *QueueTestStore) BeginTransactionQueue(ctx context.Context, description string) (queue.QueueStore, error) {
	return s, nil
}

func (s *QueueTestStore) CompleteTransaction(ctx context.Context, err *error) {}

func (s *QueueTestStore) QueuePermits(ctx context.Context, name string) ([]queue.QueuePermit, error) {
	s.permitsCalled++
	return s.permits, s.permitsErr
}

func (s *QueueTestStore) QueuePermitDelete(ctx context.Context, permitID permit.Permit) error {
	if s.permitDelete == nil {
		s.permitsDeleted++
	}
	return s.permitDelete
}

func (s *QueueTestStore) QueuePeek(ctx context.Context, types ...uint64) (results []queue.QueueWork, err error) {
	return s.peek, s.peekErr
}

func (s *QueueTestStore) QueuePush(ctx context.Context, name string, groupId sql.NullInt64, priority, workType uint64, work interface{}, carrier []byte) error {
	return s.err
}

func (s *QueueTestStore) QueuePushAddressed(ctx context.Context, name string, groupId sql.NullInt64, priority, workType uint64, address string, work interface{}, carrier []byte) error {
	return s.err
}

func (s *QueueTestStore) QueuePop(ctx context.Context, name string, maxPriority uint64, types []uint64) (*queue.QueueWork, error) {
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

func (s *QueueTestStore) IsQueueAddressInProgress(ctx context.Context, address string) (bool, error) {
	return s.hasAddress, s.hasAddressErr
}

func (s *QueueTestStore) IsQueueAddressComplete(ctx context.Context, address string) (bool, error) {
	s.polled++
	return s.poll, s.pollErr
}

func (s *QueueTestStore) NotifyExtend(ctx context.Context, permit uint64) error {
	return s.err
}

func (s *QueueTestStore) QueueDelete(ctx context.Context, permit permit.Permit) error {
	return s.err
}

func (s *QueueTestStore) QueueAddressedComplete(ctx context.Context, address string, failure error) error {
	return s.err
}

func (s *QueueTestStore) GetLocalListenerProvider() *local.ListenerProvider {
	if s.llf != nil {
		return s.llf
	} else {
		return local.NewListenerProvider(local.ListenerProviderArgs{})
	}
}

type fakeBroadcaster struct {
	work   <-chan listener.Notification
	chunks <-chan listener.Notification
}

func (f *fakeBroadcaster) IP() string {
	return ""
}

func (f *fakeBroadcaster) Subscribe(dataType uint8) <-chan listener.Notification {
	if dataType == uint8(11) {
		return f.chunks
	}
	return f.work
}

func (f *fakeBroadcaster) SubscribeOne(dataType uint8, matcher broadcaster.Matcher) <-chan listener.Notification {
	return f.work
}

func (f *fakeBroadcaster) Unsubscribe(ch <-chan listener.Notification) {
}

type fakeMonitor struct {
	result    bool
	resultMap map[uint64]bool
}

func (f *fakeMonitor) Run(ctx context.Context, b broadcaster.Broadcaster) {
}

func (f *fakeMonitor) Check(ctx context.Context, permitId uint64, created time.Time, maxAge time.Duration) bool {
	if f.resultMap != nil {
		return f.resultMap[permitId]
	}
	return f.result
}

type QueuePermitMonitorSuite struct{}

var _ = check.Suite(&QueuePermitMonitorSuite{})

func (s *QueuePermitMonitorSuite) TestNew(c *check.C) {
	cstore := &QueueTestStore{}
	m := NewDatabaseQueueMonitorTask(DatabaseQueueMonitorTaskConfig{
		QueueName:                 "test",
		SweepAge:                  time.Minute,
		QueueStore:                cstore,
		NotifyTypePermitExtension: 8,
	})
	c.Assert(m, check.DeepEquals, &DatabaseQueueMonitorTask{
		sweepAge:                  time.Minute,
		cstore:                    cstore,
		notifyTypePermitExtension: 8,
		queueName:                 "test",
	})
}

func (s *QueuePermitMonitorSuite) TestRun(c *check.C) {
	defer leaktest.Check(c)
	ctx := context.Background()

	ch := make(chan listener.Notification)
	nb := &fakeBroadcaster{
		work: ch,
	}
	m := &DatabaseQueueMonitorTask{
		sweepAge: time.Minute,
		cstore:   &QueueTestStore{},

		notifyTypePermitExtension: 8,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx, nb)

	// Send a notification
	ch <- queue.NewQueuePermitExtendNotification(123, 8)

	// Set the monitor start time
	m.started = time.Now().Add(-time.Hour)

	// Check the map
	b := m.Check(ctx, 123, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, true)

	// Check for a value that does not exist in the map
	b = m.Check(ctx, 456, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, false)

	// Check for a value that does not exist in the map, but was created recently
	b = m.Check(ctx, 456, time.Now(), time.Minute)
	c.Assert(b, check.Equals, true)

	// Send a notification
	ch <- queue.NewQueuePermitExtendNotification(456, 8)
	b = m.Check(ctx, 456, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, true)

	// Check for a value that does not exist in the map, but right after the
	// service started. This will return true since the service has not been
	// running for as long as the 1 minute maxAge parameter.
	m.started = time.Now().Add(-time.Second * 30)
	b = m.Check(ctx, 789, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, true)
}

func (s *QueuePermitMonitorSuite) TestRunDrain(c *check.C) {
	defer leaktest.Check(c)

	ch := make(chan listener.Notification)
	nb := &fakeBroadcaster{
		work: ch,
	}
	m := &DatabaseQueueMonitorTask{
		sweepAge: time.Minute,
		cstore:   &QueueTestStore{},
	}

	// Run, but cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.Run(ctx, nb)

	// Checks all return false since the service is stopped
	b := m.Check(ctx, 123, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, false)
	b = m.Check(ctx, 456, time.Time{}, time.Minute)
	c.Assert(b, check.Equals, false)
}

func (s *QueuePermitMonitorSuite) TestCheck(c *check.C) {
	checkCh := make(chan permitCheck)
	defer close(checkCh)
	m := &DatabaseQueueMonitorTask{
		check:   checkCh,
		enabled: true,
		cstore:  &QueueTestStore{},
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case n := <-checkCh:
				var r bool
				if n.permitId == 123 {
					r = true
				}
				n.response <- r
			}
		}
	}()

	r := m.Check(context.Background(), 123, time.Time{}, time.Minute)
	c.Assert(r, check.Equals, true)
	r = m.Check(context.Background(), 456, time.Time{}, time.Minute)
	c.Assert(r, check.Equals, false)
}
func (s *QueuePermitMonitorSuite) TestSweep(c *check.C) {
	m := &DatabaseQueueMonitorTask{
		sweepAge: time.Minute,
	}

	// Time one is at minute 15
	t1 := time.Date(2020, 1, 1, 14, 15, 0, 0, time.UTC)
	// Time two is at minute 13 (2 minutes older)
	t2 := time.Date(2020, 1, 1, 14, 13, 0, 0, time.UTC)

	// Build an initial map with the two above times.
	permitMap := map[uint64]time.Time{
		123: t1,
		456: t2,
	}

	// Now is at minute 15, 30 seconds later than time 1.
	now := time.Date(2020, 1, 1, 14, 15, 30, 0, time.UTC)

	// Sweep should remove time t2 since it is older than one minute.
	m.sweep(now, permitMap)
	c.Assert(permitMap, check.DeepEquals, map[uint64]time.Time{
		123: t1,
	})
}

func (s *QueuePermitMonitorSuite) TestRefreshPermitMap(c *check.C) {
	ctx := context.Background()
	m := &DatabaseQueueMonitorTask{
		cstore: &QueueTestStore{},
	}
	fakePermitMap := map[uint64]time.Time{}
	m.refreshPermitMap(ctx, fakePermitMap)
	c.Check(len(fakePermitMap), check.Equals, 0)

	// Map should acquire missing permits from the store
	fakeStore := &QueueTestStore{
		permits: []queue.QueuePermit{
			&fakePermit{permitId: 1},
		},
	}

	m.cstore = fakeStore
	m.refreshPermitMap(ctx, fakePermitMap)
	c.Check(len(fakePermitMap), check.Equals, 1)

	// Transient failure should not disrupt permitMap
	fakeStore = &QueueTestStore{
		permitsErr: errors.New("Fake QueuePermits error"),
	}

	m.cstore = fakeStore
	m.refreshPermitMap(ctx, fakePermitMap)
	c.Check(len(fakePermitMap), check.Equals, 1)
}
