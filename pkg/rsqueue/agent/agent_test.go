package agent

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	agenttypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent/types"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/metrics"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/utils"
	"gopkg.in/check.v1"
)

type AgentSuite struct {
	cEnforcer    *ConcurrencyEnforcer
	defaults     map[int64]int64
	priorities   map[int64]int64
	priorityList []int64
}

var _ = check.Suite(&AgentSuite{})

func (s *AgentSuite) SetUpSuite(c *check.C) {
	s.priorityList = []int64{1, 3}
	s.priorities = map[int64]int64{
		1: 99,
	}
	s.defaults = map[int64]int64{
		1: 2,
		3: 2,
	}

	var err error
	s.cEnforcer, err = Concurrencies(s.defaults, s.priorities, s.priorityList)
	c.Assert(err, check.IsNil)
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

// Inherits queue.DefaultQueueSupportedTypes, which is what we really want
// to test. Adds a channel so we can reliably know when `DisableAll` is called
// to avoid test races and avoid requiring wasting test time with long timeouts.
type fakeSupportedTypes struct {
	*queue.DefaultQueueSupportedTypes
	disabled chan struct{}
}

func (f *fakeSupportedTypes) DisableAll() {
	if f.disabled != nil {
		defer close(f.disabled)
	}
	f.DefaultQueueSupportedTypes.DisableAll()
}

type FakeWork struct {
	Tag string
}

func (FakeWork) Type() uint64 {
	return 0
}

type FakeRunner struct {
	queue.BaseRunner
	err    error
	finish map[string]chan bool
	mutex  *sync.Mutex
}

func (r *FakeRunner) Run(ctx context.Context, work queue.RecursableWork) error {
	w := FakeWork{}
	err := json.Unmarshal(work.Work, &w)
	if err != nil {
		return errors.New("Invalid work")
	}
	r.mutex.Lock()
	ch := r.finish[w.Tag]
	r.mutex.Unlock()
	<-ch
	return r.err
}

type FakeQueue struct {
	deleted  int
	pollErrs chan error
	extended map[permit.Permit]int
	extend   error
	mutex    sync.Mutex
	record   error
	errs     []error
}

func (*FakeQueue) Push(priority uint64, groupId int64, work queue.Work) error {
	return nil
}
func (f *FakeQueue) WithDbTx(tx interface{}) queue.Queue {
	return f
}
func (*FakeQueue) Peek(filter func(work *queue.QueueWork) (bool, error), types ...uint64) ([]queue.QueueWork, error) {
	return nil, nil
}
func (*FakeQueue) AddressedPush(priority uint64, groupId int64, address string, work queue.Work) error {
	return nil
}
func (*FakeQueue) IsAddressInQueue(address string) (bool, error) {
	return false, nil
}
func (f *FakeQueue) PollAddress(address string) (errs <-chan error) {
	return f.pollErrs
}
func (f *FakeQueue) RecordFailure(address string, failure error) error {
	if f.record == nil {
		f.errs = append(f.errs, failure)
	}
	return f.record
}
func (*FakeQueue) Get(maxPriority uint64, maxPriorityChan chan uint64, types queue.QueueSupportedTypes, stop chan bool) (*queue.QueueWork, error) {
	return nil, nil
}
func (f *FakeQueue) Extend(p permit.Permit) error {
	if f.extend == nil {
		f.mutex.Lock()
		defer f.mutex.Unlock()
		f.extended[p]++
	}
	return f.extend
}
func (f *FakeQueue) Delete(permit.Permit) error {
	f.deleted += 1
	return nil
}
func (*FakeQueue) Name() string {
	return ""
}

func agentCfg(runner queue.WorkRunner, queue queue.Queue, cEnforcer *ConcurrencyEnforcer, supportedTypes queue.QueueSupportedTypes, msgs <-chan listener.Notification, notifyTypeWorkComplete uint8, wrapper metrics.JobLifecycleWrapper) AgentConfig {
	return AgentConfig{
		WorkRunner:             runner,
		Queue:                  queue,
		ConcurrencyEnforcer:    cEnforcer,
		SupportedTypes:         supportedTypes,
		NotificationsChan:      msgs,
		NotifyTypeWorkComplete: notifyTypeWorkComplete,
		JobLifecycleWrapper:    wrapper,
	}
}

func (s *AgentSuite) TestNewAgent(c *check.C) {
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, &FakeQueue{}, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	var testAgent *DefaultAgent
	testAgent = a
	_ = testAgent
}

func (s *AgentSuite) TestWaitImmediate(c *check.C) {
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, &FakeQueue{}, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	done := make(chan int64)
	result := a.Wait(50, done)
	// If there are already 50 jobs running, we can only take a new job
	// if it's priority is 9 or greater
	c.Check(result, check.Equals, uint64(1))
}

func (s *AgentSuite) TestWaitBlocked(c *check.C) {
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, &FakeQueue{}, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	jobDone := make(chan int64)
	completed := false
	done := make(chan bool)

	go func() {
		result := a.Wait(99, jobDone)
		c.Check(result, check.Equals, uint64(1))
		completed = true
		done <- true
	}()

	// Send some messages across the queue while blocked and make sure they're discarded
	msgs <- agenttypes.NewWorkCompleteNotification("somewhere1", 10)
	msgs <- agenttypes.NewWorkCompleteNotification("somewhere2", 10)
	msgs <- agenttypes.NewWorkCompleteNotification("somewhere3", 10)

	c.Check(completed, check.Equals, false)
	jobDone <- 98

	// Wait
	<-done
	c.Check(completed, check.Equals, true)
}

func (s *AgentSuite) TestRunJobOk(c *check.C) {
	q := &FakeQueue{
		extended: make(map[permit.Permit]int),
		errs:     make([]error, 0),
	}
	finish := map[string]chan bool{
		"one":   make(chan bool),
		"two":   make(chan bool),
		"three": make(chan bool),
	}
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{finish: finish, mutex: &sync.Mutex{}}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.extend = time.Millisecond

	// Assume we start with three jobs
	a.runningJobs = 3
	c.Check(a.runningJobs, check.Equals, int64(3))

	// Run three jobs in separate goroutines.
	// The jobs run for 10, 20, and 30 milliseconds, respectively,
	// in an attempt to force them to complete in the order we expect.
	jobDone := make(chan int64)
	maxPriorityChan := make(chan uint64)

	b1, err := json.Marshal(&FakeWork{Tag: "one"})
	c.Assert(err, check.IsNil)
	b2, err := json.Marshal(&FakeWork{Tag: "two"})
	c.Assert(err, check.IsNil)
	b3, err := json.Marshal(&FakeWork{Tag: "three"})
	c.Assert(err, check.IsNil)

	// Only two of the jobs have addresses, so we should receive two notifications
	wg := &sync.WaitGroup{}
	wg.Add(2)
	n := func(n listener.Notification) {
		wg.Done()
	}

	ctx1 := context.Background()
	ctx2 := context.Background()
	ctx3 := context.Background()

	a.running.Add(1)
	a.runningWork[permit.Permit(1234)] = ctx1
	go a.runJob(ctx1, &queue.QueueWork{WorkType: 0, Work: b1, Address: "", Permit: permit.Permit(1234)}, jobDone, maxPriorityChan, n)
	a.running.Add(1)
	a.runningWork[permit.Permit(4567)] = ctx2
	go a.runJob(ctx2, &queue.QueueWork{WorkType: 0, Work: b2, Address: "def", Permit: permit.Permit(4567)}, jobDone, maxPriorityChan, n)
	a.running.Add(1)
	a.runningWork[permit.Permit(8901)] = ctx3
	go a.runJob(ctx3, &queue.QueueWork{WorkType: 0, Work: b3, Address: "ghi", Permit: permit.Permit(8901)}, jobDone, maxPriorityChan, n)

	// Three running jobs
	c.Check(a.runningJobs, check.Equals, int64(3))
	c.Check(a.runningWork, check.HasLen, 3)

	// Since we had capacity for more running jobs, we should
	// end up sending two values across the maxPriorityChan
	maxPriorityDone := make(chan struct{})
	receivedPriority := make([]uint64, 0)
	go func() {
		defer close(maxPriorityDone)
		for i := 0; i < 3; i++ {
			p := <-maxPriorityChan
			receivedPriority = append(receivedPriority, p)
		}
	}()

	// The jobDone channel should have been sent the job counts when each
	// job completed
	go func() { time.Sleep(time.Millisecond * 3); finish["one"] <- true }()
	ct := <-jobDone
	c.Check(ct, check.Equals, int64(2))
	go func() { time.Sleep(time.Millisecond * 3); finish["two"] <- true }()
	ct = <-jobDone
	c.Check(ct, check.Equals, int64(1))
	go func() { time.Sleep(time.Millisecond * 3); finish["three"] <- true }()
	ct = <-jobDone
	c.Check(ct, check.Equals, int64(0))

	c.Check(a.runningJobs, check.Equals, int64(0))
	c.Check(a.runningWork, check.HasLen, 0)

	// Ensure we received the max priority notifications
	<-maxPriorityDone
	c.Check(receivedPriority[0], check.Equals, uint64(1))
	c.Check(receivedPriority[1], check.Equals, MAX_CONCURRENCY)
	c.Check(receivedPriority[2], check.Equals, MAX_CONCURRENCY)

	// Jobs b2 and b3 should have been marked as successful since they were addressed
	c.Check(q.errs, check.HasLen, 2)
	c.Check(q.errs[0], check.IsNil)
	c.Check(q.errs[1], check.IsNil)

	// Jobs should have been deleted
	c.Check(q.deleted, check.Equals, 3)

	// Queue permits should have been extended
	c.Check(len(q.extended) > 0, check.Equals, true)

	// Two jobs should notify that they are complete, since two have addresses
	wg.Wait()

	// The running jobs waitgroup should be done
	a.running.Wait()
}

func (s *AgentSuite) TestRunJobFails(c *check.C) {
	q := &FakeQueue{
		extended: make(map[permit.Permit]int),
		errs:     make([]error, 0),
	}
	finish := map[string]chan bool{
		"one": make(chan bool),
	}
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{
		err:    errors.New("work failed"),
		finish: finish,
		mutex:  &sync.Mutex{},
	}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.extend = 10 * time.Millisecond

	// Assume we start with three jobs
	a.runningJobs = 1

	// Run jobs in separate goroutine
	jobDone := make(chan int64)
	maxPriorityChan := make(chan uint64)

	w1 := FakeWork{Tag: "one"}

	b1, err := json.Marshal(&w1)
	c.Assert(err, check.IsNil)

	n := func(n listener.Notification) {}

	maxPriorityDone := make(chan struct{})
	go func() {
		defer close(maxPriorityDone)
		<-maxPriorityChan
	}()

	a.running.Add(1)
	go a.runJob(context.Background(), &queue.QueueWork{WorkType: 0, Work: b1, Address: "abc", Permit: permit.Permit(1234)}, jobDone, maxPriorityChan, n)

	// One running jobs
	c.Check(a.runningJobs, check.Equals, int64(1))

	// The jobDone channel should have been sent the job counts when each
	// job completed
	go func() { time.Sleep(time.Millisecond * 3); finish["one"] <- true }()
	ct := <-jobDone
	c.Check(ct, check.Equals, int64(0))

	// Wait on the max priority channel to be notified
	<-maxPriorityDone

	// Jobs should have been marked as failed since it was addressed
	c.Check(q.errs, check.HasLen, 1)
	c.Check(q.errs[0], check.ErrorMatches, "work failed")

	// Jobs should have been deleted
	c.Check(q.deleted, check.Equals, 1)

	// The running jobs waitgroup should be done
	a.running.Wait()
}

func (s *AgentSuite) TestRunJobExtendError(c *check.C) {
	q := &FakeQueue{
		extended: make(map[permit.Permit]int),
		extend:   errors.New("extend error"),
	}
	finish := map[string]chan bool{
		"one": make(chan bool),
	}
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{finish: finish, mutex: &sync.Mutex{}}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.extend = 10 * time.Millisecond
	a.runningJobs = 1

	jobDone := make(chan int64)
	maxPriorityChan := make(chan uint64)

	w1 := FakeWork{Tag: "one"}

	b1, err := json.Marshal(&w1)
	c.Assert(err, check.IsNil)

	n := func(n listener.Notification) {}

	maxPriorityDone := make(chan struct{})
	go func() {
		defer close(maxPriorityDone)
		<-maxPriorityChan
	}()

	a.running.Add(1)
	go a.runJob(context.Background(), &queue.QueueWork{WorkType: 0, Work: b1, Address: "", Permit: permit.Permit(1234)}, jobDone, maxPriorityChan, n)

	// The jobDone channel should have been sent the job counts when each
	// job completed
	go func() { time.Sleep(time.Millisecond * 3); finish["one"] <- true }()
	ct := <-jobDone
	c.Check(ct, check.Equals, int64(0))

	// Wait on the max priority channel to be notified
	<-maxPriorityDone

	// Jobs should have been deleted
	c.Check(q.deleted, check.Equals, 1)

	// Extension failed, but there should be no other errors since we do a
	// best-effort to extend queue permits.
	c.Check(q.extended, check.HasLen, 0)

	// The running jobs waitgroup should be done
	a.running.Wait()
}

func (s *AgentSuite) TestAgentStop(c *check.C) {
	defer leaktest.Check(c)

	q := &FakeQueue{}
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.recursing.Add(1)
	a.running.Add(1)

	// Insert two jobs; one marked for recursion.
	ctx1 := context.Background()
	ctx2 := queue.ContextWithExpectedRecursion(ctx1)
	a.runningWork[permit.Permit(1234)] = ctx1
	a.runningWork[permit.Permit(2345)] = ctx2

	stopped := make(chan struct{})
	var err error
	go func() {
		err = a.Stop(time.Minute * 1)
		close(stopped)
	}()

	go func() {
		<-a.stop
		close(a.stop)
	}()

	a.recursing.Done()
	a.running.Done()

	// Remove the job that has been marked for recursion
	a.mutex.Lock()
	delete(a.runningWork, permit.Permit(2345))
	a.mutex.Unlock()

	// Wait for stop
	<-stopped

	c.Assert(err, check.IsNil)
}

func (s *AgentSuite) TestAgentStopTimeoutRecurse(c *check.C) {
	defer leaktest.Check(c)

	q := &FakeQueue{}
	supportedTypes := &queue.DefaultQueueSupportedTypes{}
	supportedTypes.SetEnabled(2, true)
	supportedTypes.SetEnabled(3, true)
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.recursing.Add(1)

	stopped := make(chan struct{})
	var err error
	go func() {
		err = a.Stop(time.Millisecond * 10)
		close(stopped)
	}()

	// Wait for stop
	<-stopped

	// Should have time out, and since we were waiting on recursion, the supported
	// types list should still be populated.
	c.Assert(err, check.DeepEquals, ErrAgentStopTimeout)
	c.Assert(supportedTypes.Enabled(), utils.SliceEquivalent, []uint64{2, 3})
}

func (s *AgentSuite) TestAgentStopTimeoutRun(c *check.C) {
	defer leaktest.Check(c)

	q := &FakeQueue{}
	disabled := make(chan struct{})
	supportedTypes := &fakeSupportedTypes{
		DefaultQueueSupportedTypes: &queue.DefaultQueueSupportedTypes{},
		disabled:                   disabled,
	}
	supportedTypes.SetEnabled(2, true)
	supportedTypes.SetEnabled(3, true)
	msgs := make(chan listener.Notification)
	a := NewAgent(agentCfg(&FakeRunner{}, q, s.cEnforcer, supportedTypes, msgs, 10, &fakeWrapper{}))
	a.running.Add(1)

	stopped := make(chan struct{})
	var err error
	go func() {
		err = a.Stop(time.Millisecond * 10)
		// Wait for all types to be disabled
		<-disabled
		close(stopped)
	}()

	// Wait for stop
	<-stopped

	// Should have time out, but since we were not waiting on recursion, the supported
	// types list should be empty
	c.Assert(err, check.DeepEquals, ErrAgentStopTimeout)
	c.Assert(supportedTypes.Enabled(), check.DeepEquals, []uint64{})
}

func (s *AgentSuite) TestCheckForJobsWithRecursion(c *check.C) {
	a := &DefaultAgent{
		runningWork: make(map[permit.Permit]context.Context),
	}

	// Insert three jobs; two marked for recursion, one not.
	ctx1 := context.Background()
	ctx2 := queue.ContextWithExpectedRecursion(ctx1)
	ctx3 := queue.ContextWithExpectedRecursion(context.Background())
	a.runningWork[permit.Permit(1234)] = ctx1
	a.runningWork[permit.Permit(2345)] = ctx2
	a.runningWork[permit.Permit(3456)] = ctx3

	// Wait for the recursing jobs to leave the map
	done := make(chan struct{})
	var result bool
	go func() {
		defer close(done)
		result = a.waitForJobsWithRecursion(time.Second * 2)
	}()

	// Remove the two jobs that have been marked for recursion
	a.mutex.Lock()
	delete(a.runningWork, permit.Permit(2345))
	delete(a.runningWork, permit.Permit(3456))
	a.mutex.Unlock()

	<-done
	c.Check(result, check.Equals, true)
}

func (s *AgentSuite) TestCheckForJobsWithRecursionTimeout(c *check.C) {
	a := &DefaultAgent{
		runningWork: make(map[permit.Permit]context.Context),
	}
	ctx1 := context.Background()
	ctx2 := queue.ContextWithExpectedRecursion(ctx1)
	a.runningWork[permit.Permit(1234)] = ctx1
	a.runningWork[permit.Permit(2345)] = ctx2

	done := make(chan struct{})
	var result bool
	go func() {
		defer close(done)
		result = a.waitForJobsWithRecursion(time.Millisecond)
	}()

	<-done
	c.Check(result, check.Equals, false)
}

func (s *AgentSuite) TestRecurseFn(c *check.C) {
	defer leaktest.Check(c)

	a := &DefaultAgent{
		runningJobs: 2,
	}
	jobDone := make(chan int64)
	recurse := a.getRecurseFn(jobDone)
	workDone := make(chan bool)
	work := func() {
		defer close(workDone)
		<-workDone
	}

	// Start the work
	go recurse(work)

	// Job done channel should be notified
	j := <-jobDone
	c.Assert(j, check.Equals, int64(1))

	// Running job count should be reduced
	c.Assert(a.runningJobs, check.Equals, int64(1))

	// End work
	workDone <- true
	<-workDone

	// Job count should be incremented to original count again
	c.Assert(a.runningJobs, check.Equals, int64(2))

	// Start the work again
	jobDone = make(chan int64)
	recurse = a.getRecurseFn(jobDone)
	workDone = make(chan bool)
	work = func() {
		defer close(workDone)
		<-workDone
	}
	go recurse(work)

	// Don't receive on the jobDone channel. The work should still
	// succeed since we don't block.

	// End work
	workDone <- true
	<-workDone

	// Job count should be incremented to original count again
	c.Assert(a.runningJobs, check.Equals, int64(2))
}
