package rscache

// Copyright (C) 2026 by Posit Software, PBC

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

type RangeCacheSuite struct{}

var _ = check.Suite(&RangeCacheSuite{})

var rangeCacheModTime = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

// rangeStorage holds one item that appears only once the queue has run.
type rangeStorage struct {
	rsstorage.DummyStorageServer
	filled     bool
	data       []byte
	gets       int
	rangeCalls []struct {
		offset, length int64
		filledAtCall   bool
	}
}

func (s *rangeStorage) Check(ctx context.Context, dir, address string) (bool, *types.ChunksInfo, int64, time.Time, error) {
	if !s.filled {
		return false, nil, 0, time.Time{}, nil
	}
	return true, nil, int64(len(s.data)), rangeCacheModTime, nil
}

func (s *rangeStorage) Get(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	s.gets++
	return nil, nil, 0, time.Time{}, false, errors.New("Get must not be called by GetRange")
}

func (s *rangeStorage) GetRange(ctx context.Context, dir, address string, offset, length int64) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	s.rangeCalls = append(s.rangeCalls, struct {
		offset, length int64
		filledAtCall   bool
	}{offset, length, s.filled})
	if !s.filled {
		return nil, nil, 0, time.Time{}, false, nil
	}
	n, err := rsstorage.ClampRange(offset, length, int64(len(s.data)))
	if err != nil {
		return nil, nil, 0, time.Time{}, false, err
	}
	return io.NopCloser(bytes.NewReader(s.data[offset : offset+n])), nil, int64(len(s.data)), rangeCacheModTime, true, nil
}

// fillingQueue fills the storage when work is pushed, the way a runner would.
type fillingQueue struct {
	storage *rangeStorage
	pushes  int
}

func (q *fillingQueue) AddressedPush(ctx context.Context, priority uint64, groupId int64, address string, work QueueWork) error {
	q.pushes++
	q.storage.filled = true
	return nil
}

func (q *fillingQueue) PollAddress(ctx context.Context, address string) (errs <-chan error) {
	ch := make(chan error, 1)
	ch <- nil
	return ch
}

func newRangeCache(storage rsstorage.StorageServer, q Queue) RangeFileCache {
	fc := NewFileCache(fileCfg(q, &fakeDupMatcher{}, storage, &fakeRecurser{}, time.Second*5))
	return fc.(RangeFileCache)
}

func (s *RangeCacheSuite) TestHit(c *check.C) {
	storage := &rangeStorage{filled: true, data: []byte("0123456789")}
	q := &fillingQueue{storage: storage}
	cache := newRangeCache(storage, q)
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	r, size, mod, err := cache.GetRange(context.Background(), spec, 3, 4)
	c.Assert(err, check.IsNil)
	got, _ := io.ReadAll(r)
	c.Check(r.Close(), check.IsNil)
	c.Check(string(got), check.Equals, "3456")
	c.Check(size, check.Equals, int64(10))
	c.Check(mod.Equal(rangeCacheModTime), check.Equals, true)

	// The decision: ranged read from storage; no queue work, no full Get.
	c.Check(q.pushes, check.Equals, 0)
	c.Check(storage.gets, check.Equals, 0)
	c.Assert(storage.rangeCalls, check.HasLen, 1)
	c.Check(storage.rangeCalls[0].offset, check.Equals, int64(3))
	c.Check(storage.rangeCalls[0].length, check.Equals, int64(4))
}

func (s *RangeCacheSuite) TestMissFillsCacheThenRangesStorage(c *check.C) {
	storage := &rangeStorage{filled: false, data: []byte("0123456789")}
	q := &fillingQueue{storage: storage}
	cache := newRangeCache(storage, q)
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	r, size, _, err := cache.GetRange(context.Background(), spec, 7, 100)
	c.Assert(err, check.IsNil)
	got, _ := io.ReadAll(r)
	c.Check(r.Close(), check.IsNil)
	c.Check(string(got), check.Equals, "789")
	c.Check(size, check.Equals, int64(10))

	// The cache was filled from the origin (one push), and the one ranged read
	// happened afterwards, against storage. The origin was never ranged.
	c.Check(q.pushes, check.Equals, 1)
	c.Assert(storage.rangeCalls, check.HasLen, 1)
	c.Check(storage.rangeCalls[0].filledAtCall, check.Equals, true)
	c.Check(storage.gets, check.Equals, 0)
}

func (s *RangeCacheSuite) TestUnsupportedStorageFallsBack(c *check.C) {
	// DummyStorageServer has no GetRange.
	storage := &rsstorage.DummyStorageServer{}
	q := &fakeQueue{}
	cache := newRangeCache(storage, q)
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	_, _, _, err := cache.GetRange(context.Background(), spec, 0, 1)
	c.Check(errors.Is(err, rsstorage.ErrRangeNotSupported), check.Equals, true)
	// It did not queue work: the caller's fallback Get does that.
	c.Check(q.AddParams, check.HasLen, 0)
}

func (s *RangeCacheSuite) TestInvalidRangeDoesNotFillCache(c *check.C) {
	storage := &rangeStorage{filled: false, data: []byte("0123456789")}
	q := &fillingQueue{storage: storage}
	cache := newRangeCache(storage, q)
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	for _, rng := range [][2]int64{{-1, 5}, {0, 0}, {0, -3}} {
		_, _, _, err := cache.GetRange(context.Background(), spec, rng[0], rng[1])
		c.Check(errors.Is(err, rsstorage.ErrInvalidRange), check.Equals, true)
	}
	c.Check(q.pushes, check.Equals, 0)
}

func (s *RangeCacheSuite) TestNotSatisfiablePassesThrough(c *check.C) {
	storage := &rangeStorage{filled: true, data: []byte("0123456789")}
	cache := newRangeCache(storage, &fillingQueue{storage: storage})
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	_, _, _, err := cache.GetRange(context.Background(), spec, 10, 1)
	var rerr *rsstorage.RangeNotSatisfiableError
	c.Assert(errors.As(err, &rerr), check.Equals, true)
	c.Check(rerr.Size, check.Equals, int64(10))
}

func newMemoryBackedRange(storage rsstorage.StorageServer, q Queue) CacheProvider {
	fc := NewFileCache(fileCfg(q, &fakeDupMatcher{}, storage, &fakeRecurser{}, time.Second*5))
	return NewMemoryBackedFileCache(MemoryBackedFileCacheConfig{FileCache: fc})
}

func (s *RangeCacheSuite) TestMemoryBackedRanges(c *check.C) {
	storage := &rangeStorage{filled: true, data: []byte("0123456789")}
	cache := newMemoryBackedRange(storage, &fillingQueue{storage: storage})
	rc, ok := cache.(RangeFileCache)
	c.Assert(ok, check.Equals, true)
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	r, size, mod, err := rc.GetRange(context.Background(), spec, 3, 4)
	c.Assert(err, check.IsNil)
	got, _ := io.ReadAll(r)
	c.Check(r.Close(), check.IsNil)
	c.Check(string(got), check.Equals, "3456")
	c.Check(size, check.Equals, int64(10))
	c.Check(mod.Equal(rangeCacheModTime), check.Equals, true)
}

// noRangeCache is a FileCache with no GetRange.
type noRangeCache struct{ FileCache }

func (s *RangeCacheSuite) TestMemoryBackedInnerCannotRange(c *check.C) {
	cache := NewMemoryBackedFileCache(MemoryBackedFileCacheConfig{FileCache: noRangeCache{}})
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	_, _, _, err := cache.GetRange(context.Background(), spec, 0, 1)
	c.Check(errors.Is(err, rsstorage.ErrRangeNotSupported), check.Equals, true)
}

func (s *RangeCacheSuite) TestMemoryBackedNotSatisfiable(c *check.C) {
	storage := &rangeStorage{filled: true, data: []byte("0123456789")}
	cache := newMemoryBackedRange(storage, &fillingQueue{storage: storage})
	spec := ResolverSpec{Work: &FakeWork{address: "addr"}}

	_, _, _, err := cache.(RangeFileCache).GetRange(context.Background(), spec, 10, 1)
	var rerr *rsstorage.RangeNotSatisfiableError
	c.Assert(errors.As(err, &rerr), check.Equals, true)
	c.Check(rerr.Size, check.Equals, int64(10))
}
