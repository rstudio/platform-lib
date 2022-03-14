package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/file"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rscache/test"
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

// Create a struct that we'll cache
type testCachePerson struct {
	Time      time.Time
	Name      string
	Guid      string
	Age       uint8
	NetWorth  float64
	Children  []testCacheChild
	Dwellings map[string]testCacheDwelling
}

type testCacheChild struct {
	Name string
	Guid string
	Age  int
}

type testCacheDwelling struct {
	Guid    string
	Address string
	City    string
	State   string
	Zip     string
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

// Freecache will only cache an item if it is <= 1024th the size
// of the cache. It follows roughly this sequence:
//
// 1. When creating the cache, it creates 256 buffers. For example, if
//    you create a cache 524288 (512KB) in size, there will be 256 buffers,
//    each 2048 bytes in size.
// 2. When adding an item to the cache, freecache divides the buffer size
//    by 4 and subtracts ENTRY_HDR_SIZE to determine the max key+val size.
//    Based on the 2048 byte buffer size, we get (2048/4)-24 = 488. In this
//    example, 488 would be the max key+val size.
//
// Below, we initialize the cache based on the minimum size that will allow
// us to store data. This is equal to the key size + the value size +
// ENTRY_HDR_SIZE, multiplied by 1024.

func (s *MemoryCacheIntegrationSuite) TestInMemoryCaching(c *check.C) {

	// Used to verify that we pull the correct item from the cache. This
	// data is identical to the golden data in `testdata/person_01.gob`
	// and `testdata/person_01.gzip.gob` (compressed).
	person := testCachePerson{
		Time:     time.Time{},
		Name:     "John Doe",
		Guid:     "e0108c9e-d514-417c-8c31-3d2dc0e7ed4d",
		Age:      44,
		NetWorth: 2500000,
		Children: []testCacheChild{
			{
				Name: "Jane Doe",
				Guid: "72c75025-d70d-4ea1-ad43-408bdf31887c",
				Age:  5,
			},
			{
				Name: "John Doe, Jr",
				Guid: "03ae0879-bd6a-49b8-a316-7c9c7ac0a71c",
				Age:  8,
			},
		},
		Dwellings: map[string]testCacheDwelling{
			"a": {
				Guid:    "2c747f2d-0024-474f-a2f5-d5f88f08825e",
				Address: "123 Anywhere St",
				City:    "Chicago",
				State:   "IL",
				Zip:     "44456",
			},
			"b": {
				Guid:    "99f2f3a8-84df-467e-97f5-ec9a5437ab24",
				Address: "456 There Pl",
				City:    "New York",
				State:   "NY",
				Zip:     "44489",
			},
		},
	}

	// If updating our golden files, cache the test person and copy assets to the
	// testdata directory.
	if *UpdateGolden {
		func() {
			// Encode uncompressed
			f, err := os.Create("testdata/person_01.gob")
			c.Assert(err, check.IsNil)
			defer f.Close()
			gobber := gob.NewEncoder(f)
			err = gobber.Encode(&person)
			c.Assert(err, check.IsNil)

			// Encode a gzipped version so we can test grabbing a compressed asset from the cache
			f2, err := os.Create("testdata/person_01.gzip.gob")
			c.Assert(err, check.IsNil)
			defer f2.Close()
			gzipper := gzip.NewWriter(f2)
			defer gzipper.Close()
			gobber = gob.NewEncoder(gzipper)
			err = gobber.Encode(&person)
			c.Assert(err, check.IsNil)
		}()
	}

	// Prepare for the tests by copying the two gobs to the test temp dir.
	func() {
		src1, err := os.Open("testdata/person_01.gob")
		c.Assert(err, check.IsNil)
		defer src1.Close()
		dst, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "person_01.gob"))
		c.Assert(err, check.IsNil)
		defer dst.Close()
		_, err = io.Copy(dst, src1)
		c.Assert(err, check.IsNil)
		src2, err := os.Open("testdata/person_01.gzip.gob")
		c.Assert(err, check.IsNil)
		defer src2.Close()
		dst2, err := os.Create(filepath.Join(s.tempdirhelper.Dir(), "person_01.gzip.gob"))
		c.Assert(err, check.IsNil)
		defer dst2.Close()
		_, err = io.Copy(dst2, src2)
		c.Assert(err, check.IsNil)
	}()

	// Figure out the size of a cached item in memory. The byte size of the file
	// is accurate since we're caching a []byte array in memory.
	src := filepath.Join(s.tempdirhelper.Dir(), "person_01.gob")
	fi, err := os.Stat(src)
	c.Assert(err, check.IsNil)
	entrySize := fi.Size()

	// Make additional links to the uncompressed asset. We started (above) with a file
	// name `person_01.gob`. This simply adds symlinks to `person_02.gob`, etc., up to
	// `person_99.gob`.
	numEntries := 99
	for i := 2; i <= numEntries; i++ {
		dst := filepath.Join(s.tempdirhelper.Dir(), fmt.Sprintf("person_%02d.gob", i))
		err := os.Link(src, dst)
		c.Assert(err, check.IsNil)
	}

	// Create an in-memory cache that is just large enough to hold 64 entries
	maxCost := entrySize * 64
	log.Printf("Creating cache with MaxCost=%d", maxCost)
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     maxCost,
		BufferItems: 64,
		// Enable metrics so we can test some things
		Metrics: true,
	})
	c.Assert(err, check.IsNil)
	mc := NewMemoryCache(MemoryCacheConfig{TTL: time.Hour, Ristretto: rc})
	c.Check(mc, check.DeepEquals, &memoryCache{
		ttl:       time.Hour,
		ristretto: rc,
	})

	// Also create an object cache that uses the in-memory cache
	timeout := time.Second * 30
	q := &fakeQueue{}
	d := &fakeDebugLogger{}
	fcs := file.NewFileStorageServer(s.tempdirhelper.Dir(), 4096, nil, nil, "test", d, time.Minute, time.Minute)
	fc := NewFileCache(fileCfg(q, &fakeDupMatcher{}, fcs, &fakeRecurser{}, timeout, d, d))

	mbfc := NewMemoryBackedFileCache(memCfg(fc, mc, 10000000, d))

	// First, we test loading uncompressed and compressed data
	// from the cache.
	//
	// Load uncompressed item from the cache
	spec := ResolverSpec{
		CacheInMemory: false,
		Work: &FakeWork{
			address: "person_01.gob",
		},
	}
	gotPerson, err := mbfc.GetObject(context.Background(), spec, &testCachePerson{}).AsObject()
	c.Assert(err, check.IsNil)
	c.Assert(gotPerson, check.DeepEquals, &person)
	//
	// Load compressed data from the cache
	spec2 := ResolverSpec{
		CacheInMemory: false,
		Gzip:          true,
		Work: &FakeWork{
			address: "person_01.gzip.gob",
		},
	}
	gotPersonGzipped, err := mbfc.GetObject(context.Background(), spec2, &testCachePerson{}).AsObject()
	c.Assert(err, check.IsNil)
	c.Assert(gotPersonGzipped, check.DeepEquals, &person)

	// Since both retrievals above included `CacheInMemory: false`, there
	// should be nothing cached in memory. Verify this.
	c.Check(rc.Metrics.KeysAdded(), check.Equals, uint64(0))

	gob.Register(testCachePerson{})

	// DRY helper for loading items and putting them into the memory cache
	load := func(i int) {
		spec = ResolverSpec{
			CacheInMemory: true,
			Work: &FakeWork{
				address: fmt.Sprintf("person_%02d.gob", i),
			},
		}
		gotPerson, err = mbfc.GetObject(context.Background(), spec, &testCachePerson{}).AsObject()
		c.Assert(err, check.IsNil)
		c.Assert(gotPerson, check.DeepEquals, &person)
	}

	// Load the first 64 test items and cache them in memory. This
	// should fill the cache to its MaxCost setting.
	for i := 1; i <= 64; i++ {
		load(i)
	}

	time.Sleep(1000 * time.Millisecond) // brief pause to allow ristretto to process work

	// The first 64 items should all be successfully added since they don't
	// overrun the cache's MaxCost setting. This is also testing that rc (the ristretto cache) is not being copied unintentionally.
	//   It should be the same object we pass in and it should be used by the cache.
	total := rc.Metrics.KeysAdded()
	c.Assert(total >= uint64(64), check.Equals, true)

	// Load additional items, overrunning the MaxCost setting.
	for i := 65; i <= numEntries; i++ {
		load(i)
	}

	// We can't guarantee that all `Set`s will be successful after we start overrunning the
	// MaxCost. The sum of dropped, rejected, and successfully added entries should equal
	// `numEntries`.
	time.Sleep(100 * time.Millisecond) // brief pause to allow ristretto to process work
	mt := rc.Metrics
	total = mt.SetsDropped() + mt.SetsRejected() + mt.KeysAdded()
	c.Assert(total >= uint64(numEntries), check.Equals, true)
}
