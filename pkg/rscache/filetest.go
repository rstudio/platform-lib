package rscache

// Copyright (C) 2025 by Posit Software, PBC

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"
)

type FakeWork struct {
	address string
	dir     string
}

func (*FakeWork) Type() uint64 {
	return 0
}

func (f *FakeWork) Address() string {
	return f.address
}

func (f *FakeWork) Dir() string {
	return f.dir
}

type FakeMemoryCache struct {
	GetObjectResult map[string]*CacheReturn
	GetErr          error
	IsEnabled       bool
	PutErr          error
}

func NewFakeMemoryCache(enabled bool) *FakeMemoryCache {
	return &FakeMemoryCache{
		GetObjectResult: make(map[string]*CacheReturn),
		IsEnabled:       enabled,
	}
}

func (f *FakeMemoryCache) Enabled() bool {
	return f.IsEnabled
}

func (f *FakeMemoryCache) Get(address string) (value *CacheReturn) {
	var ok bool
	value, ok = f.GetObjectResult[address]
	if ok {
		value.ReturnedFrom = "memory"
	} else {
		value = &CacheReturn{}
	}
	return
}

func (f *FakeMemoryCache) Put(address string, item *CacheReturn) (err error) {
	if f.PutErr != nil {
		return f.PutErr
	}

	if reader, ok := item.Value.(io.ReadCloser); ok {
		bVal := new(bytes.Buffer)
		defer reader.Close()

		_, err = io.Copy(bVal, reader)
		if err != nil {
			return err
		}
		item.Value = bVal.Bytes()
	}

	f.GetObjectResult[address] = item

	return nil
}

func (f *FakeMemoryCache) Uncache(address string) {
	delete(f.GetObjectResult, address)
	return
}

type DummyMemoryBackedFileCache struct {
	HeadCount       int
	HeadErr         error
	RcFunc          func(spec ResolverSpec) (io.ReadCloser, int64)
	CheckRes        bool
	CheckErr        error
	GetWait         time.Duration
	GetCount        int
	GetObjectCount  int
	GetLock         sync.Mutex
	GotSpec         ResolverSpec
	GotSpecs        []ResolverSpec
	RemoveCount     int
	MC              MemoryCache
	GetObjectResult *CacheReturn
	GetObjects      map[string]*CacheReturn
	RetryCount      int
	UncacheErr      error
}

func NewDummyFileCache() *DummyMemoryBackedFileCache {
	return &DummyMemoryBackedFileCache{
		MC: NewFakeMemoryCache(true),
	}
}

func (f *DummyMemoryBackedFileCache) Check(resolver ResolverSpec) (bool, error) {
	f.GotSpec = resolver
	if f.GotSpecs == nil {
		f.GotSpecs = make([]ResolverSpec, 0)
		f.GotSpecs = append(f.GotSpecs, resolver)
	}
	return f.CheckRes, f.CheckErr
}

func (f *DummyMemoryBackedFileCache) Get(ctx context.Context, resolver ResolverSpec) (value CacheReturn) {
	f.GotSpec = resolver
	if f.GotSpecs == nil {
		f.GotSpecs = make([]ResolverSpec, 0)
		f.GotSpecs = append(f.GotSpecs, resolver)
	}

	f.GetLock.Lock()
	defer f.GetLock.Unlock()

	if f.GetWait > 0 {
		time.Sleep(f.GetWait)
	}
	if f.RcFunc != nil {
		var rc io.ReadCloser
		var sz int64
		rc, sz = f.RcFunc(resolver)
		value = CacheReturn{
			Value:    rc,
			Complete: true,
			Err:      nil,
			Size:     sz,
		}
	} else {
		value = f.GetObject(ctx, resolver, []byte{})
	}
	if value.Error() == nil {
		f.GetCount++
	}

	return
}

func (f *DummyMemoryBackedFileCache) getObject(address string, typeExample interface{}) (value CacheReturn) {
	var item *CacheReturn
	if f.MC != nil && f.MC.Enabled() {
		item = f.MC.Get(address)
		if item != nil && (!item.IsNull() || item.Error() != nil) {
			return *item
		}
	}
	if len(f.GetObjects) > 0 {
		item = f.GetObjects[address]
	} else {
		item = f.GetObjectResult
	}

	if item != nil {
		value = *item
	} else {
		value = CacheReturn{CacheKey: address}
	}

	return
}

func (f *DummyMemoryBackedFileCache) GetObject(ctx context.Context, resolver ResolverSpec, typeExample interface{}) (value CacheReturn) {
	f.GotSpec = resolver
	if f.GotSpecs == nil {
		f.GotSpecs = make([]ResolverSpec, 0)
		f.GotSpecs = append(f.GotSpecs, resolver)
	}

	// only wait when we don't have an error
	if f.GetWait > 0 && (f.GetObjectResult == nil || f.GetObjectResult.Error() == nil) {
		time.Sleep(f.GetWait)
	}

	if f.HeadErr != nil {
		return CacheReturn{Err: f.HeadErr}
	}

	if f.RcFunc != nil {
		readCloser, _ := f.RcFunc(resolver)
		object, err := decompressAndDecodeGob(readCloser, resolver.Gzip, typeExample)

		if err != nil && resolver.Retries < 5 {
			resolver.Retries++
			// recurse if err. We only get this if the RCfunc returns an error. We do that
			//    in tests when we want to test the retry behavior. The retries count check prevents infinite loops.
			return f.GetObject(ctx, resolver, typeExample)
		}

		value = CacheReturn{
			Value:     object,
			Size:      0,
			Complete:  true,
			Timestamp: time.Now(),
			Err:       err,
		}
	} else if f.GetObjectResult != nil && (f.GetObjectResult.Err != nil || !f.GetObjectResult.IsNull()) {
		// Some tests don't set valid address info, preferring to just return a single object instead of populating the map.
		//    For those tests, we need to return that object, and avoid computing the Address, because it'll panic.
		value = *f.GetObjectResult
	} else {
		address := resolver.Address()
		value = f.getObject(address, typeExample)
	}

	f.GetObjectCount++

	return
}

func (f *DummyMemoryBackedFileCache) Head(ctx context.Context, resolver ResolverSpec) (int64, time.Time, error) {
	f.GotSpec = resolver
	if f.GotSpecs == nil {
		f.GotSpecs = make([]ResolverSpec, 0)
		f.GotSpecs = append(f.GotSpecs, resolver)
	}
	var dummy interface{}
	obj := f.GetObject(ctx, resolver, dummy)

	return obj.GetSize(), obj.GetTimestamp(), f.HeadErr
}

func (f *DummyMemoryBackedFileCache) Uncache(resolver ResolverSpec) error {
	f.RemoveCount++
	return f.UncacheErr
}
