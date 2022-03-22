package database

// Copyright (C) 2022 by RStudio, PBC

import (
	"errors"

	"github.com/rstudio/platform-lib/pkg/rsqueue/groups"
	"github.com/rstudio/platform-lib/pkg/rsqueue/impls/database/dbqueuetypes"
	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"gopkg.in/check.v1"
)

type GroupQueueSuite struct{}

var _ = check.Suite(&GroupQueueSuite{})

func (s *GroupQueueSuite) SetUpSuite(c *check.C) {
}

func (s *GroupQueueSuite) TearDownSuite(c *check.C) {
}

type fakeGroup struct {
	flag        string
	endWork     []byte
	endWorkType uint8
}

func (q *fakeGroup) Type() uint64 {
	return 1
}

func (q *fakeGroup) EndWorkType() uint8 {
	return q.endWorkType
}

func (q *fakeGroup) GroupId() int64 {
	return 22
}

func (q *fakeGroup) Name() string {
	return "fakeName"
}

func (q *fakeGroup) Flag() string {
	return q.flag
}

func (q *fakeGroup) EndWork() groups.GroupQueueJob {
	return &fakeGroup{
		flag: groups.QueueGroupFlagEnd,
	}
}

func (q *fakeGroup) SetEndWork(endWorkType uint8, work []byte) {
	q.endWork = work
	q.endWorkType = endWorkType
}

func (q *fakeGroup) EndWorkJob() []byte {
	return q.endWork
}

func (q *fakeGroup) AbortWork() groups.GroupQueueJob {
	return &fakeGroup{
		flag: groups.QueueGroupFlagAbort,
	}
}

func (q *fakeGroup) CancelWork() groups.GroupQueueJob {
	return &fakeGroup{
		flag: groups.QueueGroupFlagCancel,
	}
}

type fakeQueue struct {
	result    error
	queue     []queue.Work
	extended  int
	extendErr error
	pollErrs  chan error
	record    error
}

func (f *fakeQueue) Push(priority uint64, groupId int64, work queue.Work) error {
	f.queue = append(f.queue, work)
	return f.result
}
func (f *fakeQueue) WithDbTx(tx interface{}) queue.Queue {
	return f
}
func (f *fakeQueue) Peek(filter func(work *queue.QueueWork) (bool, error), types ...uint64) ([]queue.QueueWork, error) {
	return nil, nil
}
func (f *fakeQueue) AddressedPush(priority uint64, groupId int64, address string, work queue.Work) error {
	return nil
}
func (f *fakeQueue) IsAddressInQueue(address string) (bool, error) {
	return false, nil
}
func (f *fakeQueue) PollAddress(address string) (errs <-chan error) {
	return f.pollErrs
}
func (f *fakeQueue) RecordFailure(address string, failure error) error {
	return f.record
}
func (f *fakeQueue) Get(maxPriority uint64, maxPriorityChan chan uint64, types queue.QueueSupportedTypes, stop chan bool) (*queue.QueueWork, error) {
	return nil, errors.New("n/i")
}

func (f *fakeQueue) Extend(permit.Permit) error {
	f.extended += 1
	return f.extendErr
}

func (f *fakeQueue) Delete(permit.Permit) error {
	return errors.New("n/i")
}

func (f *fakeQueue) Name() string {
	return "base queue"
}

type fakeStore struct {
	returns   dbqueuetypes.QueueGroupRecord
	errs      error
	begin     error
	complete  error
	exists    bool
	existsErr error
}

func (s *fakeStore) BeginTransactionQueue(description string) (dbqueuetypes.QueueGroupStore, error) {
	return s, s.begin
}

func (s *fakeStore) CompleteTransaction(err *error) {
	if s.complete != nil && *err == nil {
		*err = s.complete
	}
}

func (s *fakeStore) QueueGroupStart(id int64) error {
	return nil
}

func (s *fakeStore) QueueGroupComplete(id int64) (bool, bool, error) {
	return false, false, nil
}

func (s *fakeStore) QueueGroupCancel(id int64) error {
	return nil
}

func (s *fakeStore) QueueGroupClear(id int64) error {
	return nil
}

func (s *GroupQueueSuite) TestNewFactory(c *check.C) {
	baseQueue := &fakeQueue{}
	groupQueue := &fakeQueue{}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	c.Check(gf, check.DeepEquals, &DefaultQueueGroupFactory{
		baseQueue:  baseQueue,
		groupQueue: groupQueue,
	})
}

func (s *GroupQueueSuite) TestGetGroup(c *check.C) {
	baseQueue := &fakeQueue{}
	groupQueue := &fakeQueue{}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gq := gf.GetGroup(group)
	c.Assert(gq.Group(), check.DeepEquals, group)
	gq.(*DefaultGroupQueue).group = nil
	c.Check(gq, check.DeepEquals, &DefaultGroupQueue{
		BaseQueue:  baseQueue,
		GroupQueue: groupQueue,
	})
	c.Check(gq.BaseQueueName(), check.Equals, "base queue")
}

func (s *GroupQueueSuite) TestNewGroup(c *check.C) {
	baseQueue := &fakeQueue{}
	groupQueue := &fakeQueue{}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)
	c.Check(gq.Group(), check.DeepEquals, group)
	gq.(*DefaultGroupQueue).group = nil
	c.Check(gq, check.DeepEquals, &DefaultGroupQueue{
		BaseQueue:  baseQueue,
		GroupQueue: groupQueue,
	})
}

type work struct {
	name string
}

func (work) Type() uint64 {
	return 0
}

func (s *GroupQueueSuite) TestPush(c *check.C) {
	baseQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	groupQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)

	err = gq.Push(0, work{name: "testwork"})
	c.Assert(err, check.IsNil)
	err = gq.Push(0, work{name: "testwork2"})
	c.Assert(err, check.IsNil)

	c.Assert(baseQueue.queue, check.HasLen, 2)
	c.Assert(groupQueue.queue, check.HasLen, 0)
}

func (s *GroupQueueSuite) TestPushErrs(c *check.C) {
	baseQueue := &fakeQueue{
		queue:  make([]queue.Work, 0),
		result: errors.New("push failed"),
	}
	groupQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)

	err = gq.Push(0, work{name: "testwork"})
	c.Assert(err, check.ErrorMatches, "push failed")
}

func (s *GroupQueueSuite) TestStart(c *check.C) {
	baseQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	groupQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)

	err = gq.Start()
	c.Assert(err, check.IsNil)

	c.Assert(baseQueue.queue, check.HasLen, 0)
	c.Assert(groupQueue.queue, check.HasLen, 1)
}

func (s *GroupQueueSuite) TestStartErrs(c *check.C) {
	baseQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	groupQueue := &fakeQueue{
		queue:  make([]queue.Work, 0),
		result: errors.New("start push failed"),
	}
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)

	err = gq.Start()
	c.Assert(err, check.ErrorMatches, "start push failed")
}

func (s *GroupQueueSuite) TestSetEndWork(c *check.C) {
	baseQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	groupQueue := &fakeQueue{
		queue: make([]queue.Work, 0),
	}
	group := &fakeGroup{
		flag: groups.QueueGroupFlagStart,
	}
	gf := NewQueueGroupFactory(QueueGroupFactoryConfig{BaseQueue: baseQueue, GroupQueue: groupQueue})
	gq, err := gf.NewGroup(group)
	c.Assert(err, check.IsNil)

	fe := &fakeEndWork{
		Name: "test",
	}

	err = gq.SetEndWork(fe, 3)
	c.Assert(err, check.IsNil)
	c.Assert(gq.Group().EndWorkJob(), check.DeepEquals, []byte(`{"name":"test"}`))
	c.Assert(gq.Group().EndWorkType(), check.Equals, uint8(3))
}

type fakeEndWork struct {
	Name string `json:"name"`
}
