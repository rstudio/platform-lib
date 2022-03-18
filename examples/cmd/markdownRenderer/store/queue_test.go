package store

// Copyright (C) 2022 by RStudio, PBC

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"gopkg.in/check.v1"
)

const (
	TypeTest       uint64 = 5
	TypeTest2      uint64 = 6
	TypeGroupEvent uint64 = 7
	TypeGroupWork  uint64 = 8
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type QueueSqliteSuite struct {
	store Store
	tmp   string
}

type fakeDebugLogger struct{}

func (*fakeDebugLogger) Debugf(msg string, args ...interface{}) {}
func (*fakeDebugLogger) Enabled() bool                          { return false }

var _ = check.Suite(&QueueSqliteSuite{})

func (s *QueueSqliteSuite) SetUpTest(c *check.C) {
	tmp, err := ioutil.TempFile("", "")
	c.Assert(err, check.IsNil)
	s.tmp = tmp.Name()

	llf := local.NewListenerProviderWithLogger(&fakeDebugLogger{})
	s.store = Open(s.tmp, llf, rslog.NewDebugLogger(0))
}

func (s *QueueSqliteSuite) TearDownTest(c *check.C) {
	os.Remove(s.tmp)
}

type FakeJob struct {
	Tag string `json:"tag"`
}

var nullInt64 = sql.NullInt64{}

func testPush(store Store, c *check.C) {
	err := store.QueuePush("test", nullInt64, 8, TypeTest, &FakeJob{Tag: "8-1"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePush("test", nullInt64, 8, TypeTest, &FakeJob{Tag: "8-2"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePush("test", nullInt64, 7, TypeTest, &FakeJob{Tag: "7-1"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePush("test", nullInt64, 4, TypeTest, &FakeJob{Tag: "4-1"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePush("test", nullInt64, 0, TypeTest, &FakeJob{Tag: "0-1"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePush("test", nullInt64, 0, TypeTest, &FakeJob{Tag: "0-2"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePushAddressed("test", nullInt64, 7, TypeTest, "abc", &FakeJob{Tag: "7-2"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePushAddressed("test", nullInt64, 5, TypeTest, "def", &FakeJob{Tag: "5-1"}, nil)
	c.Assert(err, check.IsNil)
	err = store.QueuePushAddressed("test", nullInt64, 5, TypeTest, "def", &FakeJob{Tag: "5-1"}, nil)
	c.Assert(err, check.DeepEquals, queue.ErrDuplicateAddressedPush)
	err = store.QueuePush("test", nullInt64, 8, TypeTest2, &FakeJob{Tag: "8-3"}, nil)
	c.Assert(err, check.IsNil)
}

func testPop(store Store, firstRun bool, delete bool, c *check.C) (returnPermit permit.Permit, address string) {
	popOne := func(max uint64, expected interface{}, expectedType uint64) (permit.Permit, string) {
		job := FakeJob{}
		queueWork, err := store.QueuePop("test", max, []uint64{expectedType})
		c.Assert(err, check.IsNil)
		c.Check(queueWork.Permit, check.Not(check.Equals), uint64(0))
		c.Check(queueWork.WorkType, check.Equals, expectedType)
		err = json.Unmarshal(queueWork.Work, &job)
		c.Assert(err, check.IsNil)
		c.Check(job, check.DeepEquals, expected)
		if delete {
			err := store.QueueDelete(queueWork.Permit)
			c.Assert(err, check.IsNil)
		}
		return queueWork.Permit, queueWork.Address
	}

	// Should get first, highest priority job
	// On the second run, we don't check this since the first job's
	// heartbeat should be extended and it should be invisible
	if firstRun {
		returnPermit, _ = popOne(11, FakeJob{Tag: "0-1"}, TypeTest)
	}

	// Should get next highest priority job
	popOne(2, FakeJob{Tag: "0-2"}, TypeTest)

	// Should get nothing
	queueWork, err := store.QueuePop("test", 1, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.NotNil)
	c.Check(queueWork, check.IsNil)

	// Should get next highest priority job
	popOne(8, FakeJob{Tag: "4-1"}, TypeTest)
	popOne(8, FakeJob{Tag: "5-1"}, TypeTest)
	popOne(8, FakeJob{Tag: "7-1"}, TypeTest)
	popOne(8, FakeJob{Tag: "7-2"}, TypeTest)
	popOne(8, FakeJob{Tag: "8-1"}, TypeTest)
	popOne(8, FakeJob{Tag: "8-2"}, TypeTest)
	popOne(8, FakeJob{Tag: "8-3"}, TypeTest2)

	// Should get nothing
	queueWork, err = store.QueuePop("test", 1, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.NotNil)
	c.Check(queueWork, check.IsNil)

	return
}

func (s *QueueSqliteSuite) TestQueue(c *check.C) {
	// Push 9 items into the queue
	testPush(s.store, c)

	// Test popping the items.
	pt, _ := testPop(s.store, true, false, c)

	// Enumerate permits
	permits, err := s.store.QueuePermits("test")
	c.Assert(err, check.IsNil)
	c.Assert(permits, check.HasLen, 9)

	// Delete the permits
	for _, p := range permits {
		if p.PermitId() != pt {
			err = s.store.QueuePermitDelete(p.PermitId())
			c.Assert(err, check.IsNil)
		}
	}

	permits, err = s.store.QueuePermits("test")
	c.Assert(err, check.IsNil)
	c.Assert(permits, check.HasLen, 1)

	// Pop everything again. Everything we popped before should be
	// popped again, excepting `permit`.
	testPop(s.store, false, true, c)

	// Delete last job
	err = s.store.QueueDelete(pt)
	c.Assert(err, check.IsNil)

	// Nothing should remain
	queueWork, err := s.store.QueuePop("test", 999, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.NotNil)
	c.Check(queueWork, check.IsNil)

	// Ensure an empty slice doesn't cause an expected error
	queueWork, err = s.store.QueuePop("test", 999, []uint64{})
	c.Assert(err, check.DeepEquals, sql.ErrNoRows)
}

func (s *QueueSqliteSuite) TestAddressedPush(c *check.C) {
	err := s.store.QueuePushAddressed("test", nullInt64, 0, TypeTest, "abc", &FakeJob{Tag: "7-2"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePushAddressed("test", nullInt64, 0, TypeTest, "def", &FakeJob{Tag: "5-1"}, nil)
	c.Assert(err, check.IsNil)

	// Duplicate errs
	err = s.store.QueuePushAddressed("test", nullInt64, 0, TypeTest, "def", &FakeJob{Tag: "5-1"}, nil)
	c.Assert(err, check.DeepEquals, queue.ErrDuplicateAddressedPush)

	// Addresses should not be completed
	done, err := s.store.IsQueueAddressComplete("abc")
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)
	done, err = s.store.IsQueueAddressComplete("def")
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Attempt to pop a type that doesn't exist in the queue
	queueWork, err := s.store.QueuePop("test", 0, []uint64{888})
	c.Assert(err, check.DeepEquals, sql.ErrNoRows)

	// Pop one item
	job := FakeJob{}
	queueWork, err = s.store.QueuePop("test", 0, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest)
	c.Check(queueWork.Address, check.Equals, "abc")
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "7-2"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// First address should be completed
	done, err = s.store.IsQueueAddressComplete("abc")
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)
	done, err = s.store.IsQueueAddressComplete("def")
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Pop second item
	queueWork, err = s.store.QueuePop("test", 0, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest)
	c.Check(queueWork.Address, check.Equals, "def")
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "5-1"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Second address should be completed
	done, err = s.store.IsQueueAddressComplete("def")
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)

	// Record error for second address
	err = s.store.QueueAddressedComplete("def", errors.New("some error"))
	c.Assert(err, check.IsNil)

	// Second address should be completed, but with error
	done, err = s.store.IsQueueAddressComplete("def")
	c.Assert(err, check.ErrorMatches, "some error")
	c.Check(done, check.Equals, true)

	// Cannot check for empty address
	_, err = s.store.IsQueueAddressComplete("   ")
	c.Check(err, check.ErrorMatches, "no address provided for IsQueueAddressComplete")
}

func (s *QueueSqliteSuite) TestQueueGroups(c *check.C) {
	// Push some items without a group
	err := s.store.QueuePush("test", nullInt64, 1, TypeTest, &FakeJob{Tag: "0-1"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePush("test", nullInt64, 1, TypeTest, &FakeJob{Tag: "0-2"}, nil)
	c.Assert(err, check.IsNil)

	// Now, create a queue group
	//     the name is coded with the first word of the operation (always sync from what I can tell)
	//     the middle doesn't matter, but the last number is the source ID.
	group, err := s.store.QueueNewGroup("Sync-test")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	c.Check(g.Name, check.Equals, "Sync-test")
	c.Check(g.Cancelled, check.Equals, false)

	// Creating the same named group should fail; names are unique, but we should get the progressGuid of the existing item back.
	_, err = s.store.QueueNewGroup("Sync-test")
	c.Assert(err, check.ErrorMatches, "UNIQUE constraint failed.+")

	// Mark the queue group as started
	err = s.store.QueueGroupStart(g.GroupId())
	c.Assert(err, check.IsNil)

	// Push some items with our new group. Give them a zero
	// priority so we can guarantee they'll be popped first
	var groupInt64 = sql.NullInt64{Int64: int64(g.ID), Valid: true}
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-3"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest2, &FakeJob{Tag: "0-4"}, nil)
	c.Assert(err, check.IsNil)

	// Queue group should not be done. It just started
	done, _, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Peek for the scheduled work
	peek, err := s.store.QueuePeek(TypeTest)
	c.Assert(err, check.IsNil)
	c.Assert(peek, check.DeepEquals, []queue.QueueWork{
		{
			Address:  "",
			WorkType: TypeTest,
			Work:     []byte("{\"tag\":\"0-1\"}"),
		},
		{
			Address:  "",
			WorkType: TypeTest,
			Work:     []byte("{\"tag\":\"0-2\"}"),
		},
		{
			Address:  "",
			WorkType: TypeTest,
			Work:     []byte("{\"tag\":\"0-3\"}"),
		},
	})

	// Pop one item
	job := FakeJob{}
	queueWork, err := s.store.QueuePop("test", 0, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest)
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "0-3"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Queue group should still not be done.
	done, _, err = s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Pop another item
	queueWork, err = s.store.QueuePop("test", 0, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest2)
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "0-4"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Queue group should now be done
	done, cancelled, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)
	c.Check(cancelled, check.Equals, false)

	// To avoid import cycle, create a type to simulate the queue
	// work we place in the queue.
	type qgWork struct {
		Name        string `json:"name"`
		GroupId     int64  `json:"group_id"`
		MacroGuid   string `json:"macro_guid"`
		SourceId    uint64 `json:"source_id"`
		Flag        string `json:"flag"`
		EndWorkType uint8  `json:"end_work_type"`
		EndWork     []byte `json:"end_work"`
		StatsID     uint64 `json:"stats_id"`
	}
	groupWork := qgWork{
		Name:        "test-group",
		GroupId:     int64(g.ID), // Group ID must match group
		MacroGuid:   "abc123",
		SourceId:    3,
		Flag:        "START",
		EndWorkType: 25,
		StatsID:     23,
	}

	// Push a queue group job into the queue. This will prevent QueueGroupExists from returning
	// false since it also evaluates work for the group.
	err = s.store.QueuePush("test", sql.NullInt64{}, 0, TypeGroupEvent, &groupWork, nil)
	c.Assert(err, check.IsNil)

	// Peek for the group event
	peek, err = s.store.QueuePeek(TypeGroupEvent)
	c.Assert(err, check.IsNil)
	c.Assert(peek, check.DeepEquals, []queue.QueueWork{
		{
			Address:  "",
			WorkType: TypeGroupEvent,
			Work:     []byte(`{"name":"test-group","group_id":1,"macro_guid":"abc123","source_id":3,"flag":"START","end_work_type":25,"end_work":null,"stats_id":23}`),
		},
	})

	// Remove the work for the queue group
	queueWork, err = s.store.QueuePop("test", 0, []uint64{TypeGroupEvent})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeGroupEvent)
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Peek for the group event
	peek, err = s.store.QueuePeek(TypeGroupEvent)
	c.Assert(err, check.IsNil)
	c.Assert(peek, check.DeepEquals, []queue.QueueWork{})

	// Push a sync prep work item
	type mockPrepWork struct {
		StatId   uint64 `json:"stat_id"`
		Guid     string `json:"guid"`
		SourceId uint64 `json:"source_id"`
	}
	prep := &mockPrepWork{
		Guid:     "abc",
		StatId:   123,
		SourceId: 3,
	}
	err = s.store.QueuePush("test", sql.NullInt64{}, 0, TypeGroupWork, &prep, nil)
	c.Assert(err, check.IsNil)

	// Remove the work for the queue group
	queueWork, err = s.store.QueuePop("test", 0, []uint64{TypeGroupWork})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeGroupWork)
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)
}

func (s *QueueSqliteSuite) TestQueueGroupStart(c *check.C) {
	// Create a queue group
	group, err := s.store.QueueNewGroup("test-group")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	c.Check(g.Name, check.Equals, "test-group")
	c.Check(g.Started, check.Equals, false)

	// Start
	err = s.store.QueueGroupStart(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Get group
	var started bool
	err = s.store.(*store).db.Raw(`SELECT started FROM queue_group WHERE id = ?`, g.ID).Scan(&started).Error
	c.Assert(err, check.IsNil)
	c.Check(started, check.Equals, true)
}

func (s *QueueSqliteSuite) TestQueueUpdateGroup(c *check.C) {
	// Create a queue group
	group, err := s.store.QueueNewGroup("test-group")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	g.Started = true
	g.Cancelled = true

	_, err = QueueUpdateGroup(s.store.(*store).db, g)
	c.Assert(err, check.IsNil)

	// Get group
	var started bool
	err = s.store.(*store).db.Raw(`SELECT started FROM queue_group WHERE id = ?`, g.ID).Scan(&started).Error
	c.Assert(err, check.IsNil)
	c.Check(started, check.Equals, true)

	var cancelled bool
	err = s.store.(*store).db.Raw(`SELECT cancelled FROM queue_group WHERE id = ?`, g.ID).Scan(&cancelled).Error
	c.Assert(err, check.IsNil)
	c.Check(cancelled, check.Equals, true)
}

func (s *QueueSqliteSuite) TestQueueGroupPopStarted(c *check.C) {

	// Create a queue group
	group, err := s.store.QueueNewGroup("test-group")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	c.Check(g.Name, check.Equals, "test-group")

	// Push some items with our new group.
	var groupInt64 = sql.NullInt64{Int64: int64(g.ID), Valid: true}
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-3"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest2, &FakeJob{Tag: "0-4"}, nil)
	c.Assert(err, check.IsNil)

	// Push one items without a group
	err = s.store.QueuePush("test", nullInt64, 1, TypeTest, &FakeJob{Tag: "0-1"}, nil)
	c.Assert(err, check.IsNil)

	// Pop one item. Should get the item without a queue group, even
	// though it has a lower priority
	job := FakeJob{}
	queueWork, err := s.store.QueuePop("test", 100, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest)
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Assert(queueWork.Permit, check.Equals, permit.Permit(1))
	c.Check(job, check.DeepEquals, FakeJob{Tag: "0-1"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Attempt to pop another item. Nothing should be returned since the group
	// hasn't been started
	_, err = s.store.QueuePop("test", 100, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.DeepEquals, sql.ErrNoRows)

	// Mark the queue group as started
	err = s.store.QueueGroupStart(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Pop one item
	job = FakeJob{}
	queueWork, err = s.store.QueuePop("test", 100, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest)
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "0-3"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Pop another item
	queueWork, err = s.store.QueuePop("test", 100, []uint64{TypeTest, TypeTest2})
	c.Assert(err, check.IsNil)
	c.Check(queueWork.WorkType, check.Equals, TypeTest2)
	err = json.Unmarshal(queueWork.Work, &job)
	c.Assert(err, check.IsNil)
	c.Check(job, check.DeepEquals, FakeJob{Tag: "0-4"})
	err = s.store.QueueDelete(queueWork.Permit)
	c.Assert(err, check.IsNil)

	// Queue group should now be done
	done, cancelled, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)
	c.Check(cancelled, check.Equals, false)
}

func (s *QueueSqliteSuite) TestQueueGroupClear(c *check.C) {
	// Create a queue group
	group, err := s.store.QueueNewGroup("test-group")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	c.Check(g.Name, check.Equals, "test-group")

	// Push some items with our new group.
	var groupInt64 = sql.NullInt64{Int64: int64(g.ID), Valid: true}
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-3"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-4"}, nil)
	c.Assert(err, check.IsNil)

	// Mark the queue group as started
	err = s.store.QueueGroupStart(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Queue group should not be done. It just started
	done, _, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Clear the group queue
	err = s.store.QueueGroupClear(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Queue group should now be done
	done, cancelled, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)
	c.Check(cancelled, check.Equals, false)
}

func (s *QueueSqliteSuite) TestQueueGroupCancel(c *check.C) {
	// Create a queue group
	group, err := s.store.QueueNewGroup("test-group")
	c.Assert(err, check.IsNil)
	c.Assert(group, check.FitsTypeOf, &QueueGroup{})
	g := group.(*QueueGroup)
	c.Check(g.Name, check.Equals, "test-group")

	// Push some items with our new group. Give them a zero
	// priority so we can guarantee they'll be popped first
	var groupInt64 = sql.NullInt64{Int64: int64(g.ID), Valid: true}
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-3"}, nil)
	c.Assert(err, check.IsNil)
	err = s.store.QueuePush("test", groupInt64, 0, TypeTest, &FakeJob{Tag: "0-4"}, nil)
	c.Assert(err, check.IsNil)

	// Mark the queue group as started
	err = s.store.QueueGroupStart(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Queue group should not be done. It just started
	done, _, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, false)

	// Cancel the queue group
	err = s.store.QueueGroupCancel(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Clear the group queue
	err = s.store.QueueGroupClear(int64(g.ID))
	c.Assert(err, check.IsNil)

	// Queue group should now be done
	done, cancelled, err := s.store.QueueGroupComplete(int64(g.ID))
	c.Assert(err, check.IsNil)
	c.Check(done, check.Equals, true)
	c.Check(cancelled, check.Equals, true)
}

func (s *QueueSqliteSuite) TestAddressFailure(c *check.C) {
	// Fail
	address := "abcdefg"
	// Pass a generic error
	err := s.store.QueueAddressedComplete(address, errors.New("first error"))
	c.Assert(err, check.IsNil)
	err = s.store.(*store).QueueAddressedCheck(address)
	c.Check(err, check.ErrorMatches, "first error")

	// Should be castable to queue.QueueError pointer
	queueError, ok := err.(*queue.QueueError)
	c.Assert(ok, check.Equals, true)
	c.Check(queueError, check.DeepEquals, &queue.QueueError{
		Code:    0, // Not set on generic error
		Message: "first error",
	})

	// Fail again, but pass a typed error
	err = s.store.QueueAddressedComplete(address, &queue.QueueError{Code: 404, Message: "second error"})
	c.Assert(err, check.IsNil)
	err = s.store.(*store).QueueAddressedCheck(address)
	c.Check(err, check.ErrorMatches, "second error")

	// Should be castable to queue.QueueError pointer
	queueError, ok = err.(*queue.QueueError)
	c.Assert(ok, check.Equals, true)
	c.Check(queueError, check.DeepEquals, &queue.QueueError{
		Code:    404,
		Message: "second error",
	})

	// Don't fail
	err = s.store.QueueAddressedComplete(address, nil)
	c.Assert(err, check.IsNil)
	c.Check(s.store.(*store).QueueAddressedCheck(address), check.IsNil)
}

func (s *QueueSqliteSuite) TestIsQueueAddressInProgress(c *check.C) {
	// Is a non-existing address in the queue?
	found, err := s.store.IsQueueAddressInProgress("def")
	c.Assert(err, check.IsNil)
	c.Assert(found, check.Equals, false)

	// Add the address to the queue
	err = s.store.QueuePushAddressed("test", nullInt64, 0, TypeTest, "def", &FakeJob{Tag: "5-1"}, nil)
	c.Assert(err, check.IsNil)

	// Now it should be found
	found, err = s.store.IsQueueAddressInProgress("def")
	c.Assert(err, check.IsNil)
	c.Assert(found, check.Equals, true)

	// Complete the work
	err = s.store.QueueAddressedComplete("def", nil)
	c.Assert(err, check.IsNil)
	found, err = s.store.IsQueueAddressInProgress("def")

	// Now it should not be found
	c.Assert(err, check.IsNil)
	c.Assert(found, check.Equals, true)
}
