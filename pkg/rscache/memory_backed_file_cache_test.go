package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/file"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rscache/test"
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

func memCfg(fc FileCache, mc MemoryCache, maxMemoryPerObject int64, lgr DebugLogger) MemoryBackedFileCacheConfig {
	return MemoryBackedFileCacheConfig{
		FileCache:          fc,
		MemoryCache:        mc,
		MaxMemoryPerObject: maxMemoryPerObject,
		DebugLogger:        lgr,
	}
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryPutsInMemory(c *check.C) {
	defer leaktest.Check(c)

	ctx := context.Background()

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)

	// Add an item to the cache on disk
	fPath := filepath.Join(s.tempdirhelper.Dir(), "two")
	f, err := os.Create(fPath)
	_, err = f.WriteString("two")
	c.Assert(err, check.IsNil)
	f.Close()

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "two",
		},
	}
	buf := new(bytes.Buffer)

	// First pass loads the content from the FileStorageServer and loads it into RAM.
	obj, err := st.Get(ctx, spec).AsReader()
	c.Assert(err, check.IsNil)
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.DeepEquals, "two")

	// Now cached in memory
	obj, err = st.Get(ctx, spec).AsReader()
	c.Check(err, check.IsNil)
	buf.Reset()
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.Equals, "two")

	// Change item cached on disk
	fPath = filepath.Join(s.tempdirhelper.Dir(), "two")
	f, err = os.Create(fPath)
	c.Assert(err, check.IsNil)
	_, err = f.WriteString("two-updated")
	c.Assert(err, check.IsNil)
	f.Close()

	// sleep a bit to allow the new data to enter the cache. It shouldn't reload the file if the memory cache hasn't
	//expired, so here no change should happen.
	pollingGet("doesnt-matter", make([]byte, 0), m)

	// Ensure we get the old item from memory via object cache. The new file will get picked up only after the memory
	//     cache expires.
	obj, err = st.Get(ctx, spec).AsReader()
	c.Check(err, check.IsNil)
	buf.Reset()
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.Equals, "two")
}

//    ----------------------------------------------------

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

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemory(c *check.C) {
	defer leaktest.Check(c)

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	fPath := filepath.Join(s.tempdirhelper.Dir(), "TestGetNotInMemory")
	f, err := os.Create(fPath)
	c.Assert(err, check.IsNil)

	enc := gob.NewEncoder(f)
	err = enc.Encode(&testItem{"TestGetNotInMemory"})
	c.Assert(err, check.IsNil)

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: false,
		Work: &FakeWork{
			address: "TestGetNotInMemory",
		},
	}
	cacheValue := st.GetObject(context.Background(), spec, &testItem{})
	c.Assert(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.Value, check.DeepEquals, &testItem{"TestGetNotInMemory"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")

	// Still not in memory
	cacheValue = st.GetObject(context.Background(), spec, &testItem{})
	c.Assert(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")
}

func (s *MemoryBackedFileCacheSuite) TestGetNotInMemoryGzipped(c *check.C) {
	defer leaktest.Check(c)

	fPath := filepath.Join(s.tempdirhelper.Dir(), "TestGetNotInMemoryGzipped")
	f, err := os.Create(fPath)
	c.Assert(err, check.IsNil)

	gzw := gzip.NewWriter(f)
	enc := gob.NewEncoder(gzw)
	err = enc.Encode(&testItem{"TestGetNotInMemoryGzipped"})
	c.Assert(err, check.IsNil)
	err = gzw.Flush()
	c.Assert(err, check.IsNil)
	gzw.Close()
	f.Close()

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	m := NewFakeMemoryCache(true)
	err = m.Put("one", &CacheReturn{Value: &testItem{"one"}})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: false,
		Work: &FakeWork{
			address: "TestGetNotInMemoryGzipped",
		},
		Gzip: true,
	}
	cacheValue := st.GetObject(context.Background(), spec, &testItem{})
	obj, err := cacheValue.AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj, check.DeepEquals, &testItem{"TestGetNotInMemoryGzipped"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")
}

func (s *MemoryBackedFileCacheSuite) TestObjectGetNotInMemoryPutsInMemory(c *check.C) {
	defer leaktest.Check(c)

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	err := m.Put("one", &CacheReturn{Value: &testItem{"one"}})

	// We create a file with our cached contents. This is then loaded into the memory cache when first requested.
	//    This mimics our general workflow, where a "runner" of some sort will create the result on the file store,
	//    and that will then be read in to give the result on the service side.
	fPath := filepath.Join(s.tempdirhelper.Dir(), "putsInMemoryTest")
	f, err := os.Create(fPath)
	c.Assert(err, check.IsNil)

	enc := gob.NewEncoder(f)
	err = enc.Encode(&testItem{"putsInMemoryTest"})
	c.Assert(err, check.IsNil)

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "putsInMemoryTest",
		},
	}
	// The first "get" call should put it into memory
	cacheValue := st.GetObject(context.Background(), spec, &testItem{})
	c.Assert(cacheValue.Error(), check.IsNil)
	obj, err := cacheValue.AsObject()
	c.Check(obj, check.DeepEquals, &testItem{"putsInMemoryTest"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")

	// Second get should have it in memory. It will be stored as a decoded object in memory
	//    TODO: it would be nice to mock the gob decode functionality somehow to ensure that it is not called.
	cacheValue = st.GetObject(context.Background(), spec, &testItem{})
	c.Assert(cacheValue.Error(), check.IsNil)
	obj, err = cacheValue.AsObject()
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"putsInMemoryTest"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "memory")
}

func (s *MemoryBackedFileCacheSuite) TestCheckInMemory(c *check.C) {
	defer leaktest.Check(c)

	server := &rsstorage.DummyStorageServer{
		GetOk: false,
	}

	m := NewFakeMemoryCache(true)
	d := &fakeDebugLogger{}
	fc := NewFileCache(fileCfg(nil, nil, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "one",
		},
	}

	ok, err := st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, false)

	err = m.Put("one", &CacheReturn{Value: &testItem{"one"}, ReturnedFrom: "space"})
	c.Assert(err, check.IsNil)

	cacheValue := m.Get("one")
	obj, err := cacheValue.AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
	c.Check(cacheValue.ReturnedFrom, check.Equals, "memory")

	ok, err = st.Check(spec)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)

	mbfcCacheValue := st.GetObject(context.Background(), spec, &testItem{})
	obj, err = mbfcCacheValue.AsObject()
	c.Assert(err, check.IsNil)
	c.Check(obj.(*testItem), check.DeepEquals, &testItem{"one"})
	c.Check(mbfcCacheValue.ReturnedFrom, check.Equals, "memory")
}

func (s *MemoryBackedFileCacheSuite) TestGetObjectTooLargeForMemory(c *check.C) {
	defer leaktest.Check(c)

	ctx := context.Background()

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)

	// Add an item to the cache on disk
	fPath := filepath.Join(s.tempdirhelper.Dir(), "two")
	f, err := os.Create(fPath)
	_, err = f.WriteString("two")
	c.Assert(err, check.IsNil)
	f.Close()

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 1, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "two",
		},
	}
	buf := new(bytes.Buffer)

	// First pass loads the content from the FileStorageServer
	obj, err := st.Get(ctx, spec).AsReader()
	c.Assert(err, check.IsNil)
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.DeepEquals, "two")

	// Not cached in memory because the file is larger than our allowed memory threshold
	cacheValue := st.Get(ctx, spec)
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")

	// Change item cached on disk
	fPath = filepath.Join(s.tempdirhelper.Dir(), "two")
	f, err = os.Create(fPath)
	c.Assert(err, check.IsNil)
	_, err = f.WriteString("two-updated")
	c.Assert(err, check.IsNil)
	f.Close()

	// Ensure we get the current item on disk. Since it is too large for memory, we should always get the on-disk value
	obj, err = st.Get(ctx, spec).AsReader()
	c.Check(err, check.IsNil)
	buf.Reset()
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.Equals, "two-updated")
}

func (s *MemoryBackedFileCacheSuite) TestGetObjectStoringInMemoryFailsDoesNotError(c *check.C) {
	defer leaktest.Check(c)

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	m.PutErr = errors.New("oh no")

	// We create a file with our cached contents. This is then loaded into the memory cache when first requested.
	//    This mimics our general workflow, where a "runner" of some sort will create the result on the file store,
	//    and that will then be read in to give the result on the service side.
	fPath := filepath.Join(s.tempdirhelper.Dir(), "putsInMemoryTest")
	f, err := os.Create(fPath)
	c.Assert(err, check.IsNil)

	enc := gob.NewEncoder(f)
	err = enc.Encode(&testItem{"putsInMemoryTest"})
	c.Assert(err, check.IsNil)

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	dup := &fakeDupMatcher{}
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "putsInMemoryTest",
		},
	}

	val, err := st.GetObject(context.Background(), spec, &testItem{}).AsObject()
	c.Assert(err, check.IsNil)
	c.Assert(val, check.DeepEquals, &testItem{"putsInMemoryTest"})
}

func (s *MemoryBackedFileCacheSuite) TestGetStoringInMemoryFailsDoesNotError(c *check.C) {
	defer leaktest.Check(c)

	// In memory cache doesn't have the item we need
	m := NewFakeMemoryCache(true)
	m.PutErr = errors.New("oh no")

	errs := make(chan error)
	fakeQueue := &fakeQueue{
		PollErrs: errs,
	}
	d := &fakeDebugLogger{}
	dup := &fakeDupMatcher{}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          s.tempdirhelper.Dir(),
		ChunkSize:    4096,
		Class:        "test",
		DebugLogger:  d,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	fc := NewFileCache(fileCfg(fakeQueue, dup, server, &fakeRecurser{}, time.Second*30, d, d))
	st := NewMemoryBackedFileCache(memCfg(fc, m, 10000000, d))

	close(errs)

	fPath := filepath.Join(s.tempdirhelper.Dir(), "TestGetNotInMemory")
	f, err := os.Create(fPath)
	c.Assert(err, check.IsNil)
	_, err = f.WriteString("TestGetNotInMemory")
	c.Assert(err, check.IsNil)
	f.Close()

	spec := ResolverSpec{
		CacheInMemory: true,
		Work: &FakeWork{
			address: "TestGetNotInMemory",
		},
	}

	cacheValue := st.Get(context.Background(), spec)
	c.Assert(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")

	obj, err := cacheValue.AsReader()

	buf := new(bytes.Buffer)
	buf.ReadFrom(obj)
	c.Check(buf.String(), check.DeepEquals, "TestGetNotInMemory")

	buf.Reset()

	// Still not in memory
	cacheValue = st.Get(context.Background(), spec)
	c.Assert(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.ReturnedFrom, check.Equals, "file")

	obj, err = cacheValue.AsReader()

	buf.ReadFrom(obj)
	c.Check(buf.String(), check.DeepEquals, "TestGetNotInMemory")
}
