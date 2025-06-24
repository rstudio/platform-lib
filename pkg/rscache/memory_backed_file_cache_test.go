package rscache

// Copyright (C) 2025 by Posit Software, PBC

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/v2/pkg/rscache/test"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"gopkg.in/check.v1"
)

type MemoryBackedFileCacheSuite struct {
	tempdirhelper test.TempDirHelper
}

var _ = check.Suite(&MemoryBackedFileCacheSuite{})

type FakeReadCloser struct {
	io.Reader
}

func (*FakeReadCloser) Close() error {
	return nil
}

func (s *MemoryBackedFileCacheSuite) SetUpSuite(c *check.C) {
	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *MemoryBackedFileCacheSuite) TearDownSuite(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}

func memCfg(fc FileCache, mc MemoryCache, maxMemoryPerObject int64) MemoryBackedFileCacheConfig {
	return MemoryBackedFileCacheConfig{
		FileCache:          fc,
		MemoryCache:        mc,
		MaxMemoryPerObject: maxMemoryPerObject,
	}
}

func (s *MemoryBackedFileCacheSuite) TestGetInMemory(c *check.C) {
	defer leaktest.Check(c)

	m := NewFakeMemoryCache(true)
	st := &MemoryBackedFileCache{
		fc: nil,
		mc: m,
	}

	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "one",
		},
	}

	obj, err := st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryErrs(c *check.C) {
	defer leaktest.Check(c)

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})
	c.Assert(err, check.IsNil)
	err = m.Put("two", &CacheReturn{Err: errors.New("cache error")})
	c.Assert(err, check.IsNil)

	st := &DummyMemoryBackedFileCache{
		MC: m,
	}

	spec := ResolverSpec{
		CacheInMemory: false,
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err = st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.ErrorMatches, "cache error")
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryErrsDecoding(c *check.C) {
	defer leaktest.Check(c)

	b := &FakeReadCloser{Reader: bytes.NewBufferString("dummy")}

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	st := &DummyMemoryBackedFileCache{
		RcFunc: func(spec ResolverSpec) (io.ReadCloser, int64) { return b, 3 },
		MC:     m,
	}

	spec := ResolverSpec{
		CacheInMemory: false,
		Retries:       1,
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err = st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.ErrorMatches, "EOF")
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryErrsDecodingRetryFails(c *check.C) {
	defer leaktest.Check(c)

	r := func(spec ResolverSpec) (io.ReadCloser, int64) {
		b := bytes.NewBufferString("dummy")
		return &FakeReadCloser{Reader: b}, 5
	}

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	st := &DummyMemoryBackedFileCache{
		RcFunc: r,
		MC:     m,
	}

	spec := ResolverSpec{
		CacheInMemory: false,
		Retries:       0,
		Work: &FakeWork{
			address: "two",
		},
	}
	_, err = st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.ErrorMatches, "unexpected EOF")
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryErrsDecodingRetrySucceeds(c *check.C) {
	defer leaktest.Check(c)

	var attempts int

	r := func(spec ResolverSpec) (io.ReadCloser, int64) {
		attempts++
		if spec.Retries == 0 {
			// pass bad data on the first attempt
			b := bytes.NewBufferString("dummy")
			return &FakeReadCloser{Reader: b}, 5
		} else {
			// pass good data on the second attempt
			bytes := bytes.NewBuffer([]byte{})
			enc := gob.NewEncoder(bytes)
			enc.Encode(&testItem{"one"})
			return &FakeReadCloser{Reader: bytes}, int64(len(bytes.String()))
		}
	}

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	st := &DummyMemoryBackedFileCache{
		RcFunc: r,
		MC:     m,
	}

	spec := ResolverSpec{
		CacheInMemory: false,
		Retries:       0,
		Work: &FakeWork{
			address: "two",
		},
	}
	obj, err := st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.IsNil)
	c.Check(attempts, check.Equals, 2)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
}

func (s *MemoryBackedFileCacheSuite) TestCheckInMemory(c *check.C) {
	ctx := context.Background()
	defer leaktest.Check(c)

	server := &rsstorage.DummyStorageServer{
		GetOk: false,
	}

	m := NewFakeMemoryCache(true)
	fc := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000))

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "one",
		},
	}

	ok, err := st.Check(ctx, spec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, false)

	err = m.Put("one", &CacheReturn{Value: &testItem{"one"}, ReturnedFrom: "space"})
	c.Assert(err, check.IsNil)

	cacheValue := m.Get("one")
	obj, err := cacheValue.AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "memory")

	ok, err = st.Check(ctx, spec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)

	mbfcCacheValue := st.GetObject(context.Background(), spec, &testItem{})
	obj, err = mbfcCacheValue.AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
	c.Check(mbfcCacheValue.ReturnedFrom, check.Equals, "memory")
}
