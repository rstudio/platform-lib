package groups

// Copyright (C) 2022 by RStudio, PBC

import (
	"errors"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/pkg/rsqueue/types"
	"gopkg.in/check.v1"
)

type QueueGroupRunnerSuite struct{}

var _ = check.Suite(&QueueGroupRunnerSuite{})

func (s *QueueGroupRunnerSuite) SetUpSuite(c *check.C) {
}

func (s *QueueGroupRunnerSuite) TearDownSuite(c *check.C) {
}

type fakeRunnerProvider struct {
	// Used by IsReady
	ready    chan bool
	readyErr error

	// Used by IsComplete
	complete       chan bool
	completeResult bool
	completeErr    error

	// Used by method matching name
	beginErr  error
	cancelErr error
	abortErr  error
	clearErr  error

	// Used by Fail
	failRecordErr error
	failErr       error
}

func (s *fakeRunnerProvider) IsReady(job GroupQueueJob) error {
	if s.ready != nil {
		defer close(s.ready)
		<-s.ready
	}
	return s.readyErr
}

func (s *fakeRunnerProvider) IsComplete(job GroupQueueJob) (cancelled bool, err error) {
	if s.complete != nil {
		defer close(s.complete)
		<-s.complete
	}
	return s.completeResult, s.completeErr
}

func (s *fakeRunnerProvider) Begin(job GroupQueueJob) error {
	return s.beginErr
}

func (s *fakeRunnerProvider) Cancel(job GroupQueueJob) error {
	return s.cancelErr
}

func (s *fakeRunnerProvider) Abort(job GroupQueueJob) error {
	return s.abortErr
}

func (s *fakeRunnerProvider) Clear(job GroupQueueJob) error {
	return s.clearErr
}

func (s *fakeRunnerProvider) Fail(job GroupQueueJob, err error) error {
	s.failRecordErr = err
	return s.failErr
}

type fakeFactory struct {
	get    GroupQueueEndRunner
	getErr error
}

func (f *fakeFactory) AddRunner(t uint8, runner GroupQueueEndRunner) {
}

func (f *fakeFactory) GetRunner(t uint8) (GroupQueueEndRunner, error) {
	return f.get, f.getErr
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

type fakeGroup struct {
	TypeVal     uint64 `json:"type"`
	Value       string `json:"value"`
	FlagVal     string `json:"flag"`
	endWork     []byte
	endWorkType uint8
}

func (q *fakeGroup) Type() uint64 {
	return q.TypeVal
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
	return q.FlagVal
}

func (q *fakeGroup) EndWork() GroupQueueJob {
	return &fakeGroup{
		FlagVal: QueueGroupFlagEnd,
	}
}

func (q *fakeGroup) SetEndWork(endWorkType uint8, work []byte) {
	q.endWork = work
	q.endWorkType = endWorkType
}

func (q *fakeGroup) EndWorkJob() []byte {
	return q.endWork
}

func (q *fakeGroup) AbortWork() GroupQueueJob {
	return &fakeGroup{
		FlagVal: QueueGroupFlagAbort,
	}
}

func (q *fakeGroup) CancelWork() GroupQueueJob {
	return &fakeGroup{
		FlagVal: QueueGroupFlagCancel,
	}
}

type fakeLogger struct{}

func (*fakeLogger) Debugf(message string, args ...interface{}) {}

func (*fakeLogger) Enabled() bool { return false }

func runnerCfg(q queue.Queue, provider GroupQueueProvider, matcher TypeMatcher, endRunnerFactory GroupQueueEndRunnerFactory, recurser *queue.OptionalRecurser, logger types.DebugLogger) QueueGroupRunnerConfig {
	return QueueGroupRunnerConfig{
		Queue:            q,
		Provider:         provider,
		TypeMatcher:      matcher,
		EndRunnerFactory: endRunnerFactory,
		Recurser:         recurser,
		DebugLogger:      logger,
	}
}

func (s *QueueGroupRunnerSuite) TestNewRunner(c *check.C) {
	q := &fakeQueue{}
	provider := &fakeRunnerProvider{}
	factory := &fakeFactory{}
	matcher := NewMatcher("type")
	rec := queue.NewOptionalRecurser(queue.OptionalRecurserConfig{FatalRecurseCheck: false})
	lgr := &fakeLogger{}
	r := NewQueueGroupRunner(runnerCfg(q, provider, matcher, factory, rec, lgr))
	c.Check(r, check.DeepEquals, &QueueGroupRunner{
		queue:            q,
		provider:         provider,
		matcher:          matcher,
		endRunnerFactory: factory,
		recurser:         rec,
		wg:               &sync.WaitGroup{},
		logger:           lgr,
	})
}

func (s *QueueGroupRunnerSuite) TestUnmarshal(c *check.C) {
	q := &fakeQueue{}
	provider := &fakeRunnerProvider{}
	factory := &fakeFactory{}
	matcher := NewMatcher("type")
	matcher.Register(3, &fakeGroup{})
	rec := queue.NewOptionalRecurser(queue.OptionalRecurserConfig{FatalRecurseCheck: false})
	lgr := &fakeLogger{}
	r := NewQueueGroupRunner(runnerCfg(q, provider, matcher, factory, rec, lgr))

	// Ok case
	work := []byte(`{"type":3,"value":"a test"}`)
	gqj, err := r.unmarshal(work)
	c.Assert(err, check.IsNil)
	c.Assert(gqj, check.DeepEquals, &fakeGroup{
		TypeVal: 3,
		Value:   "a test",
	})

	// Raw unmarshal error
	work = []byte(`{!`)
	_, err = r.unmarshal(work)
	c.Assert(err, check.ErrorMatches, "error unmarshalling raw message.+")

	// No data type field
	work = []byte(`{"type-missing":3,"value":"a test"}`)
	_, err = r.unmarshal(work)
	c.Assert(err, check.ErrorMatches, "message does not contain data type field type")

	// Error unmarshalling message data type
	work = []byte(`{"type":{"name":"myName"},"value":"a test"}`)
	_, err = r.unmarshal(work)
	c.Assert(err, check.ErrorMatches, "error unmarshalling message data type.+")

	// No matcher for type
	work = []byte(`{"type":222,"value":"a test"}`)
	_, err = r.unmarshal(work)
	c.Assert(err, check.ErrorMatches, "no matcher type found for 222")

	// Job unmarshal error
	work = []byte(`{"type":3,"value":{"json":"object"}}`)
	_, err = r.unmarshal(work)
	c.Assert(err, check.ErrorMatches, "error unmarshalling JSON:.+")
}

func (s *QueueGroupRunnerSuite) TestRun(c *check.C) {
	q := &fakeQueue{}
	p := &fakeRunnerProvider{}
	factory := &fakeFactory{}
	matcher := NewMatcher("type")
	matcher.Register(3, &fakeGroup{})
	r := NewQueueGroupRunner(runnerCfg(q, p, matcher, factory, &queue.OptionalRecurser{}, &fakeLogger{}))

	// Unmarshal errs
	work := queue.RecursableWork{
		Work: []byte(`{!`),
	}
	err := r.Run(work)
	c.Assert(err, check.ErrorMatches, "error unmarshalling .*")

	// Run failure results in cancel, clear, fail
	p.completeErr = errors.New("complete error")
	work.Work = []byte(`{"type":3,"value":"something","flag":"START"}`)
	err = r.Run(work)
	c.Assert(err, check.ErrorMatches, "complete error")

	// Run failure with cancel error
	p.cancelErr = errors.New("cancel error")
	err = r.Run(work)
	c.Assert(err, check.ErrorMatches, "cancel error")

	// Run failure with clear error
	p.cancelErr = nil
	p.clearErr = errors.New("clear error")
	err = r.Run(work)
	c.Assert(err, check.ErrorMatches, "clear error")

	// Run failure with fail error
	p.clearErr = nil
	p.failErr = errors.New("fail error")
	err = r.Run(work)
	c.Assert(err, check.ErrorMatches, "fail error")
	c.Assert(p.failRecordErr, check.ErrorMatches, "complete error")

	// Success
	p.completeErr = nil
	err = r.Run(work)
	c.Assert(err, check.IsNil)
}

func (s *QueueGroupRunnerSuite) TestRunInternal(c *check.C) {
	q := &fakeQueue{}
	p := &fakeRunnerProvider{}
	factory := &fakeFactory{}
	matcher := NewMatcher("type")
	r := NewQueueGroupRunner(runnerCfg(q, p, matcher, factory, &queue.OptionalRecurser{}, &fakeLogger{}))

	// Error on provider ready check
	job := &fakeGroup{FlagVal: QueueGroupFlagStart}
	p.readyErr = errors.New("ready error")
	err := r.run(job)
	c.Assert(err, check.ErrorMatches, "ready error")

	// Error on provider begin
	p.readyErr = nil
	p.beginErr = errors.New("begin error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "begin error")

	// Error on provider completion check
	p.beginErr = nil
	p.completeErr = errors.New("complete error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "complete error")

	// Error on push
	p.completeErr = nil
	q.result = errors.New("push error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "push error")

	// Error on cancelled push
	p.completeResult = true
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "push error")

	// Error on provider cancel
	q.result = nil
	job.FlagVal = QueueGroupFlagCancel
	p.cancelErr = errors.New("cancel error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "cancel error")

	// Error on provider clear after cancel
	p.cancelErr = nil
	p.clearErr = errors.New("clear error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "clear error")

	// Error on retrieving end runner
	p.clearErr = nil
	job.FlagVal = QueueGroupFlagEnd
	factory.getErr = errors.New("get runner error")
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "get runner error")

	// Error on running end work
	factory.getErr = nil
	factory.get = &fakeRunner{err: errors.New("runner run error")}
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "runner run error")

	// Error on provider abort
	factory.get = &fakeRunner{}
	p.abortErr = errors.New("abort error")
	job.FlagVal = QueueGroupFlagAbort
	err = r.run(job)
	c.Assert(err, check.ErrorMatches, "abort error")

	// Success
	job.FlagVal = QueueGroupFlagStart
	p.abortErr = nil
	err = r.run(job)
	c.Assert(err, check.IsNil)
	job.FlagVal = QueueGroupFlagCancel
	err = r.run(job)
	c.Assert(err, check.IsNil)
	job.FlagVal = QueueGroupFlagAbort
	err = r.run(job)
	c.Assert(err, check.IsNil)
	job.FlagVal = QueueGroupFlagEnd
	err = r.run(job)
	c.Assert(err, check.IsNil)
}

func (s *QueueGroupRunnerSuite) TestStop(c *check.C) {
	r := &QueueGroupRunner{
		wg: &sync.WaitGroup{},
	}

	// Nothing waiting
	err := r.Stop(time.Millisecond * 100)
	c.Assert(err, check.IsNil)

	// Timeout
	r.wg.Add(1)
	err = r.Stop(time.Millisecond * 1)
	c.Assert(err, check.DeepEquals, ErrQueueGroupStopTimeout)

	// Wait
	go func() {
		defer r.wg.Done()
		time.Sleep(time.Millisecond * 10)
	}()
	err = r.Stop(time.Millisecond * 500)
	c.Assert(err, check.IsNil)
}
