package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rscache/test"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

var errDup = errors.New("dup")

func fileCfg(q Queue, dup DuplicateMatcher, server rsstorage.StorageServer, recurser OptionalRecurser, timeout time.Duration) FileCacheConfig {
	return FileCacheConfig{
		Queue:            q,
		DuplicateMatcher: dup,
		StorageServer:    server,
		Recurser:         recurser,
		Timeout:          timeout,
	}
}

type fakeDupMatcher struct{}

func (f *fakeDupMatcher) IsDuplicate(err error) bool {
	return err == errDup
}

type addParams struct {
	Item    QueueWork
	Address string
}

type fakeQueue struct {
	AddParams []addParams
	PushError error
	PollErrs  chan error
	// Notified when the first AddressedPush is received
	Received chan bool
	Lock     sync.Mutex
}

func (q *fakeQueue) AddressedPush(priority uint64, groupId int64, address string, work QueueWork) error {
	q.AddParams = append(q.AddParams, addParams{work, address})
	q.Lock.Lock()
	defer q.Lock.Unlock()
	if q.Received != nil {
		q.Received <- true
		q.Received = nil
	}
	return q.PushError
}

func (q *fakeQueue) PollAddress(address string) (errs <-chan error) {
	return q.PollErrs
}

type fakeRecurser struct{}

func (a *fakeRecurser) OptionallyRecurse(ctx context.Context, run func()) {
	run()
}

type FileCacheSuite struct {
	tempdirhelper test.TempDirHelper
}

var _ = check.Suite(&FileCacheSuite{})

func (s *FileCacheSuite) SetUpSuite(c *check.C) {
	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *FileCacheSuite) TearDownSuite(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}

type worker struct {
	recursed bool
}

func (w *worker) recurse(run func()) {
	run()
	w.recursed = true
}

func (s *FileCacheSuite) TestNew(c *check.C) {
	q := &fakeQueue{}
	server := &rsstorage.DummyStorageServer{}
	r := &fakeRecurser{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, r, time.Second*30))
	c.Check(st, check.DeepEquals, &fileCache{
		queue:            q,
		duplicateMatcher: dup,
		server:           server,
		recurser:         r,
		timeout:          time.Second * 30,
		retry:            time.Millisecond * 200,
	})
}

func (s *FileCacheSuite) TestCheckMissing(c *check.C) {
	server := &rsstorage.DummyStorageServer{
		GetOk: false,
	}
	st := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	ok, err := st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, false)
}

func (s *FileCacheSuite) TestCheckAlreadyCached(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
	}
	st := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	ok, err := st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
}

func (s *FileCacheSuite) TestCheckAlreadyCachedChunked(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: true,
		},
	}
	st := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	ok, err := st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
}

func (s *FileCacheSuite) TestCheckAlreadyCachedChunkedButIncomplete(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: false,
		},
	}
	st := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	ok, err := st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, false)
}

func (s *FileCacheSuite) TestHeadAlreadyCached(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// Ensure we don't push
		PushError: errors.New("push error"),
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	sz, ts, err := st.Head(context.Background(), spec)
	c.Assert(err, check.IsNil)
	// Nothing should have gone to the queue
	c.Assert(q.AddParams, check.HasLen, 0)
	c.Assert(sz, check.Equals, int64(64))
	c.Assert(ts, test.TimeEquals, now)
}

func (s *FileCacheSuite) TestHeadAlreadyCachedChunked(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// Ensure we don't push
		PushError: errors.New("push error"),
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: true,
		},
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	sz, ts, err := st.Head(context.Background(), spec)
	c.Assert(err, check.IsNil)
	// Nothing should have gone to the queue
	c.Assert(q.AddParams, check.HasLen, 0)
	c.Assert(sz, check.Equals, int64(64))
	c.Assert(ts, test.TimeEquals, now)
}

func (s *FileCacheSuite) TestHeadAlreadyCachedChunkedButIncomplete(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// This will be thrown since we push the work to the queue when a
		// chunked asset is incomplete.
		PushError: errors.New("push error"),
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: false,
		},
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, _, err = st.Head(context.Background(), spec)
	c.Assert(err, check.ErrorMatches, "push error")
}

func (s *FileCacheSuite) TestHeadResolvePushError(c *check.C) {
	q := &fakeQueue{
		PushError: errors.New("push error"),
	}
	server := &rsstorage.DummyStorageServer{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, _, err := st.Head(context.Background(), spec)
	c.Assert(err, check.ErrorMatches, "push error")
}

func (s *FileCacheSuite) TestHeadResolvePollError(c *check.C) {
	errCh := make(chan error)
	q := &fakeQueue{
		PollErrs: errCh,
	}
	go func() {
		errCh <- errors.New("poll error")
	}()

	server := &rsstorage.DummyStorageServer{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, _, err := st.Head(context.Background(), spec)
	c.Assert(err, check.ErrorMatches, "poll error")
}

func (s *FileCacheSuite) TestHeadResolveStillMissing(c *check.C) {
	errCh := make(chan error)
	q := &fakeQueue{
		PollErrs: errCh,
	}
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	server := &rsstorage.DummyStorageServer{
		GetReader: f,
	}
	st := &fileCache{
		queue:            q,
		server:           server,
		timeout:          time.Millisecond * 100,
		retry:            time.Millisecond * 10,
		recurser:         &fakeRecurser{},
		duplicateMatcher: &fakeDupMatcher{},
	}
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		_, _, err := st.Head(context.Background(), spec)
		c.Assert(err, check.ErrorMatches, "error: FileCache reported address 'two' complete, but item was not found in cache")
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		close(errCh)
	}()

	<-testDone

	// We should have timed out eventually, but after several attempts
	c.Check(server.GetAttempts > 5, check.Equals, true)
}

func (s *FileCacheSuite) TestHeadResolveSuccess(c *check.C) {
	defer leaktest.Check(c)

	errCh := make(chan error)

	q := &fakeQueue{
		PollErrs: errCh,
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetSize:    6,
		GetModTime: now,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		sz, ts, err := st.Head(context.Background(), spec)
		c.Assert(err, check.IsNil)
		c.Assert(sz, check.Equals, int64(6))
		c.Assert(ts, test.TimeEquals, now)
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		server.GetCheckLock.Lock()
		server.GetOk = true
		server.GetCheckLock.Unlock()
		close(errCh)
	}()

	<-testDone
}

func (s *FileCacheSuite) TestHeadResolveOkDuplicateResolver(c *check.C) {
	defer leaktest.Check(c)

	errCh := make(chan error)
	q := &fakeQueue{
		PushError: errDup,
		PollErrs:  errCh,
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetSize:    6,
		GetModTime: now,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		sz, ts, err := st.Head(context.Background(), spec)
		c.Assert(err, check.IsNil)
		c.Assert(sz, check.Equals, int64(6))
		c.Assert(ts, test.TimeEquals, now)
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		server.GetCheckLock.Lock()
		server.GetOk = true
		server.GetCheckLock.Unlock()
		close(errCh)
	}()

	<-testDone
}

func (s *FileCacheSuite) TestGetAlreadyCached(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// Ensure we don't push
		PushError: errors.New("push error"),
	}
	server := &rsstorage.DummyStorageServer{
		GetOk:     true,
		GetReader: f,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	reader, err := st.Get(context.Background(), spec).AsReader()
	c.Assert(err, check.IsNil)
	// Nothing should have gone to the queue
	c.Assert(q.AddParams, check.HasLen, 0)
	defer reader.Close()
	c.Check(reader, check.Equals, f)
}

func (s *FileCacheSuite) TestGetAlreadyCachedChunked(c *check.C) {
	defer leaktest.Check(c)

	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// Ensure we don't push
		PushError: errors.New("push error"),
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: true,
		},
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	reader, err := st.Get(context.Background(), spec).AsReader()
	c.Assert(err, check.IsNil)
	// Nothing should have gone to the queue
	c.Assert(q.AddParams, check.HasLen, 0)
	defer reader.Close()
	c.Check(reader, check.Equals, f)
}

func (s *FileCacheSuite) TestGetAlreadyCachedChunkedButIncomplete(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	q := &fakeQueue{
		// This will be thrown since we push the work to the queue when a
		// chunked asset is incomplete.
		PushError: errors.New("push error"),
	}
	now := time.Now()
	server := &rsstorage.DummyStorageServer{
		GetOk:      true,
		GetSize:    64,
		GetModTime: now,
		GetReader:  f,
		GetChunked: &types.ChunksInfo{
			Complete: false,
		},
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err = st.Get(context.Background(), spec).AsReader()
	c.Assert(err, check.ErrorMatches, "push error")
}

func (s *FileCacheSuite) TestGetResolvePushError(c *check.C) {
	q := &fakeQueue{
		PushError: errors.New("push error"),
	}
	server := &rsstorage.DummyStorageServer{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err := st.Get(context.Background(), spec).AsReader()
	c.Assert(err, check.ErrorMatches, "push error")
}

func (s *FileCacheSuite) TestGetResolvePollError(c *check.C) {
	errCh := make(chan error)
	q := &fakeQueue{
		PollErrs: errCh,
	}
	go func() {
		errCh <- errors.New("poll error")
	}()

	server := &rsstorage.DummyStorageServer{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err := st.Get(context.Background(), spec).AsReader()
	c.Assert(err, check.ErrorMatches, "poll error")
}

func (s *FileCacheSuite) TestGetResolveStillMissing(c *check.C) {
	errCh := make(chan error)
	q := &fakeQueue{
		PollErrs: errCh,
	}
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	server := &rsstorage.DummyStorageServer{
		GetReader: f,
	}
	st := &fileCache{
		queue:            q,
		server:           server,
		timeout:          time.Millisecond * 100,
		retry:            time.Millisecond * 10,
		recurser:         &fakeRecurser{},
		duplicateMatcher: &fakeDupMatcher{},
	}
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		_, err := st.Get(context.Background(), spec).AsReader()
		c.Assert(err, check.ErrorMatches, "error: FileCache reported address 'two' complete, but item was not found in cache")
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		close(errCh)
	}()

	<-testDone

	// We should have timed out eventually, but after several attempts
	c.Check(server.GetAttempts > 5, check.Equals, true)
}

func (s *FileCacheSuite) TestGetResolveSuccess(c *check.C) {
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)

	errCh := make(chan error)

	q := &fakeQueue{
		PollErrs: errCh,
	}
	server := &rsstorage.DummyStorageServer{
		GetReader: f,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		reader, err := st.Get(context.Background(), spec).AsReader()
		c.Assert(err, check.IsNil)
		defer reader.Close()
		c.Assert(reader, check.Equals, f)
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		server.GetCheckLock.Lock()
		server.GetOk = true
		server.GetCheckLock.Unlock()
		close(errCh)
	}()

	<-testDone
}

func (s *FileCacheSuite) TestGetResolveOkDuplicateResolver(c *check.C) {
	errCh := make(chan error)
	q := &fakeQueue{
		PushError: errDup,
		PollErrs:  errCh,
	}
	f, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "test"))
	c.Assert(err, check.IsNil)
	server := &rsstorage.DummyStorageServer{
		GetReader: f,
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	testDone := make(chan struct{})
	go func() {
		reader, err := st.Get(context.Background(), spec).AsReader()
		c.Assert(err, check.IsNil)
		defer reader.Close()
		c.Assert(reader, check.Equals, f)
		close(testDone)
	}()

	go func() {
		time.Sleep(time.Millisecond * 100)
		server.GetCheckLock.Lock()
		server.GetOk = true
		server.GetCheckLock.Unlock()
		close(errCh)
	}()

	<-testDone
}

func (s *FileCacheSuite) TestUncacheError(c *check.C) {
	q := &fakeQueue{}
	server := &rsstorage.DummyStorageServer{
		RemoveErr: errors.New("remove error"),
	}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	err := st.Uncache(spec)
	c.Check(err, check.ErrorMatches, "remove error")
}

func (s *FileCacheSuite) TestUncacheOk(c *check.C) {
	q := &fakeQueue{}
	server := &rsstorage.DummyStorageServer{}
	dup := &fakeDupMatcher{}
	st := NewFileCache(fileCfg(q, dup, server, &fakeRecurser{}, time.Second*30))
	spec := ResolverSpec{
		Work: &FakeWork{
			address: "two",
		},
	}

	err := st.Uncache(spec)
	c.Assert(err, check.IsNil)
}

type fakeGet struct {
	result bool
	notify chan bool
	lock   sync.Mutex
}

func (f *fakeGet) get() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	result := f.result
	if f.notify != nil {
		f.notify <- true
	}
	return result
}

func (s *FileCacheSuite) TestRetryingGetInitialSuccess(c *check.C) {
	st := &fileCache{
		recurser:         &fakeRecurser{},
		duplicateMatcher: &fakeDupMatcher{},
	}
	fakeGet := &fakeGet{result: true}
	result := st.retryingGet("", "", fakeGet.get)
	c.Check(result, check.Equals, true)
}

func (s *FileCacheSuite) TestRetryingGetTimeout(c *check.C) {

	st := &fileCache{
		timeout:          10 * time.Millisecond,
		retry:            50 * time.Millisecond,
		server:           &rsstorage.DummyStorageServer{},
		recurser:         &fakeRecurser{},
		duplicateMatcher: &fakeDupMatcher{},
	}
	fakeGet := &fakeGet{result: false}
	result := st.retryingGet("", "", fakeGet.get)
	c.Check(result, check.Equals, false)
}

func (s *FileCacheSuite) TestRetryingGetRetryOk(c *check.C) {
	defer leaktest.Check(c)

	server := &rsstorage.DummyStorageServer{}
	st := &fileCache{
		timeout:          500 * time.Millisecond,
		retry:            1 * time.Millisecond,
		server:           server,
		recurser:         &fakeRecurser{},
		duplicateMatcher: &fakeDupMatcher{},
	}
	notify := make(chan bool)
	fakeGet := &fakeGet{
		result: false,
		notify: notify,
	}
	go func() {
		// Wait for 2 notifications (first try)
		<-notify
		<-notify
		// Wait for 2 notifications (second try)
		<-notify
		<-notify
		fakeGet.lock.Lock()
		fakeGet.result = true
		fakeGet.lock.Unlock()
		<-notify
		return
	}()
	result := st.retryingGet("", "", fakeGet.get)
	c.Check(result, check.Equals, true)

	// We should have flushed the NFS cache exactly twice
	c.Check(server.Flushed, check.Equals, 2)
}
