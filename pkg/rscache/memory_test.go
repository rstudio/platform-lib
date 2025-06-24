package rscache

// Copyright (C) 2025 By Posit Software, PBC

import (
	"bytes"
	"encoding/gob"
	"flag"
	"io/ioutil"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/rstudio/platform-lib/v2/pkg/rscache/test"
	"gopkg.in/check.v1"
)

type MemoryCacheSuite struct {
}

var _ = check.Suite(&MemoryCacheSuite{})

func (s *MemoryCacheSuite) TestNew(c *check.C) {
	dur := time.Hour * 2
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 100,
		MaxCost:     1000,
		BufferItems: 64,
	})
	c.Assert(err, check.IsNil)
	m := NewMemoryCache(MemoryCacheConfig{TTL: dur, Ristretto: rc})
	c.Check(m, check.DeepEquals, &memoryCache{
		ttl:       time.Hour * 2,
		ristretto: rc,
	})
}

type testItem struct {
	Name string
}

// After a `Set` to a ristretto cache, a few milliseconds may elapse before the item is
// available via a `Get`. This test helper attempts a `Get` every 5ms with a 300ms timeout
func pollingGet(address string, obj interface{}, m MemoryCache) (value *CacheReturn) {
	tick := time.NewTicker(time.Millisecond * 5)
	defer tick.Stop()
	timeout := time.NewTimer(time.Millisecond * 300)
	defer timeout.Stop()
	for {
		select {
		case <-tick.C:
			// this is a valid CacheReturn struct, even if it's empty. IsNull() means it wasn't found.
			value = m.Get(address)
			if !value.IsNull() || value.Error() != nil {
				return
			}
		case <-timeout.C:
			return &CacheReturn{}
		}
	}
}

func (s *MemoryCacheSuite) TestCacheStream(c *check.C) {
	dur := time.Hour
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 100,
		MaxCost:     1000,
		BufferItems: 64,
	})
	c.Assert(err, check.IsNil)
	m := NewMemoryCache(MemoryCacheConfig{TTL: dur, Ristretto: rc})
	reader := ioutil.NopCloser(bytes.NewBuffer([]byte(`bob`)))
	input := &CacheReturn{Value: reader, Size: 3}
	err = m.Put("one", input)
	c.Assert(err, check.IsNil)
	testBuf := make([]byte, 10)
	// We replace the reader when we "Put" it, such that a subsequent read should work without a separate "Get" right away
	//    If you try to read from 'reader' it will be depleted
	reader, err = input.AsReader()
	c.Assert(err, check.IsNil)
	count, err := reader.Read(testBuf)
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 3)

	// sleep to allow the cache to have the value
	time.Sleep(100 * time.Millisecond)

	storedStream, err := m.Get("one").AsReader()
	c.Assert(err, check.IsNil)
	count, err = storedStream.Read(testBuf)
	c.Assert(err, check.IsNil)
	c.Check(count, check.Equals, 3)

	cacheValue := m.Get("three")
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.IsNull(), check.Equals, true)

	m.Uncache("one")
	cacheValue = m.Get("one")
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.IsNull(), check.Equals, true)
}

func (s *MemoryCacheSuite) TestCacheObject(c *check.C) {
	dur := time.Hour
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 100,
		MaxCost:     1000,
		BufferItems: 64,
	})
	c.Assert(err, check.IsNil)
	m := NewMemoryCache(MemoryCacheConfig{TTL: dur, Ristretto: rc})

	gob.Register(testItem{})

	err = m.Put("one", &CacheReturn{Value: &testItem{"one"}})
	c.Assert(err, check.IsNil)

	cacheValue := pollingGet("one", &testItem{}, m)
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.IsNull(), check.Equals, false)
	value, err := cacheValue.AsObject()
	c.Check(value.(*testItem), check.DeepEquals, &testItem{"one"})

	cacheValue = pollingGet("three", &testItem{}, m)
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.IsNull(), check.Equals, true)

	m.Uncache("one")
	cacheValue = pollingGet("one", &testItem{}, m)
	c.Check(cacheValue.Error(), check.IsNil)
	c.Check(cacheValue.IsNull(), check.Equals, true)
}

// A special suite that uses a memory cache + object cache to verify that
// we correctly cache items in memory when requested and also verifies that
// limits are honored
type MemoryCacheIntegrationSuite struct {
	tempdirhelper test.TempDirHelper
}

var UpdateGolden = flag.Bool("update", false, "update .golden files")

var _ = check.Suite(&MemoryCacheIntegrationSuite{})

func (s *MemoryCacheIntegrationSuite) SetUpSuite(c *check.C) {
	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *MemoryCacheIntegrationSuite) TearDownSuite(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}
