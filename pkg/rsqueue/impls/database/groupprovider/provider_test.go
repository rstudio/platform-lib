package groupprovider

// Copyright (C) 2022 by Posit, PBC

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/groups"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type DatabaseProviderSuite struct{}

var _ = check.Suite(&DatabaseProviderSuite{})

type fakeProviderStore struct {
	result        bool
	polled        chan bool
	errs          error
	count         int
	cleared       bool
	clearErr      error
	cancel        bool
	cancelled     bool
	cancelErr     error
	started       bool
	groupStartErr error

	mutex sync.Mutex
}

func (s *fakeProviderStore) QueueGroupComplete(groupId int64) (bool, bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.count++
	if !s.result && s.polled != nil {
		s.polled <- true
		// Set chan to nil so we don't accidentally race
		s.polled = nil
	}
	return s.result, s.cancel, s.errs
}

func (s *fakeProviderStore) QueueGroupClear(groupId int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.cleared = true
	return s.clearErr
}

func (s *fakeProviderStore) QueueGroupCancel(groupId int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.cancelled = true
	return s.cancelErr
}

func (s *fakeProviderStore) QueueGroupStart(id int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.started = true
	return s.groupStartErr
}

func (s *fakeProviderStore) CompleteTransaction(err *error) {
}

type fakeGroup struct {
}

func (q *fakeGroup) Type() uint64 {
	return 1
}

func (q *fakeGroup) EndWorkType() uint8 {
	return 0
}

func (q *fakeGroup) GroupId() int64 {
	return 22
}

func (q *fakeGroup) Name() string {
	return "fakeName"
}

func (q *fakeGroup) Flag() string {
	return ""
}

func (q *fakeGroup) EndWork() groups.GroupQueueJob {
	return &fakeGroup{}
}

func (q *fakeGroup) SetEndWork(endWorkType uint8, work []byte) {
}

func (q *fakeGroup) EndWorkJob() []byte {
	return []byte{}
}

func (q *fakeGroup) AbortWork() groups.GroupQueueJob {
	return &fakeGroup{}
}

func (q *fakeGroup) CancelWork() groups.GroupQueueJob {
	return &fakeGroup{}
}

func (s *DatabaseProviderSuite) TestNewRunner(c *check.C) {
	cstore := &fakeProviderStore{}
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	c.Assert(p, check.DeepEquals, &QueueGroupProvider{
		cstore:       cstore,
		pollInterval: 2 * time.Second,
	})
}

func (s *DatabaseProviderSuite) TestPollOk(c *check.C) {
	cstore := &fakeProviderStore{
		result: true,
	}
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	group := &fakeGroup{}
	done, _ := p.poll(group)
	cancelled := <-done
	c.Check(cancelled, check.Equals, false)
}

func (s *DatabaseProviderSuite) TestPollCancelled(c *check.C) {
	cstore := &fakeProviderStore{
		result: true, // group work is done
		cancel: true, // cancel work
	}
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	group := &fakeGroup{}
	done, _ := p.poll(group)
	cancelled := <-done
	c.Check(cancelled, check.Equals, true)
}

func (s *DatabaseProviderSuite) TestPollFast(c *check.C) {
	cstore := &fakeProviderStore{
		result: false, // group work is not done at first
	}
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	group := &fakeGroup{}
	// Poll fast
	p.pollInterval = 1 * time.Millisecond

	var groupDone bool

	// After 300ms, group work is done
	go func() {
		<-time.After(5 * time.Millisecond)

		cstore.mutex.Lock()
		defer cstore.mutex.Unlock()
		groupDone = true
		cstore.result = true
	}()

	// Start polling. Should not complete until done
	done, _ := p.poll(group)
	<-done

	c.Check(groupDone, check.Equals, true)
}

func (s *DatabaseProviderSuite) TestPollErr(c *check.C) {
	cstore := &fakeProviderStore{
		errs: errors.New("db error"), // db errs
	}
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	group := &fakeGroup{}
	_, errCh := p.poll(group)
	err := <-errCh
	c.Assert(err, check.ErrorMatches, "db error")
}

func (s *DatabaseProviderSuite) TestPollLockError(c *check.C) {
	cstore := &fakeProviderStore{
		errs: errors.New("database is locked"),
	}
	allDone := make(chan struct{})
	p := NewQueueGroupProvider(QueueGroupProviderConfig{Store: cstore})
	group := &fakeGroup{}
	go func() {
		p.pollInterval = time.Millisecond * 1
		done, _ := p.poll(group)
		<-done
		close(allDone)
	}()
	go func() {
		time.Sleep(time.Millisecond * 2)

		cstore.mutex.Lock()
		defer cstore.mutex.Unlock()
		cstore.errs = nil
		cstore.result = true
	}()
	<-allDone

	// Should have erred at least once
	c.Assert(cstore.count > 0, check.Equals, true)
}
