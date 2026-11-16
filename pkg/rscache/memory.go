package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/ristretto"
)

// MemoryCache is an in-memory cache, backs FileCache, which controls whether cache entries are put here
// via the resolver.CacheInMemory flag.
type MemoryCache interface {
	Enabled() bool
	Get(address string) (cacheReturn *CacheReturn)
	Put(address string, cacheReturn *CacheReturn) (err error)
	Uncache(address string)
}

type MemoryCacheConfig struct {
	TTL       time.Duration
	Ristretto *ristretto.Cache
}

// NewMemoryCache - A memory cache isn't bound to a disk cache. It's possible that the two could
// be out of sync. Since our goal is immutability, this shouldn't matter, but,
// for example, you can delete a cached item from the disk and it will remain
// in memory.
func NewMemoryCache(cfg MemoryCacheConfig) MemoryCache {
	m := memoryCache{
		ttl:       cfg.TTL,
		ristretto: cfg.Ristretto,
	}
	return &m
}

// MemoryCache implementation
type memoryCache struct {
	ttl       time.Duration
	ristretto *ristretto.Cache
}

func (m *memoryCache) Enabled() (enabled bool) {
	return m.ristretto != nil && m.ristretto.MaxCost() > 0
}

func (m *memoryCache) Get(address string) (value *CacheReturn) {
	val, ok := m.ristretto.Get(address)
	if !ok {
		value = &CacheReturn{}
	} else if value, ok = val.(*CacheReturn); !ok {
		value = &CacheReturn{
			// return the value here so that it is available for inspection. Note that this makes the result "not null"
			//    so watch out for those checks.
			Value: val,
			Err:   errors.New("could not cast memory-cached object into CacheReturn type"),
		}
	} else {
		value = val.(*CacheReturn)
	}
	return
}

func (m *memoryCache) Put(address string, item *CacheReturn) (err error) {
	var sz int64
	reader, err := item.AsReader()
	if err == nil {
		// if we passed in a file reader, then what we want to store is the contents
		bVal := new(bytes.Buffer)

		sz, err = io.Copy(bVal, reader)
		if err != nil {
			return
		}

		item.Value = bVal.Bytes()
	} else {
		b := new(bytes.Buffer)

		// When we pass pointers, the raw size we get from unsafe.Sizeof is the size of the pointer (a few bytes). By
		//    encoding to gob, we get a more accurate size, which is important for ristretto so that it knows how "costly"
		//    a cache item is.
		if err = gob.NewEncoder(b).Encode(item); err == nil {
			sz = int64(b.Len())
		}
	}

	ok := m.ristretto.SetWithTTL(address, item, sz, m.ttl)

	if !ok {
		err = fmt.Errorf("could not set value for address %s", address)
		return
	}
	return
}

func (m *memoryCache) Uncache(address string) {
	m.ristretto.Del(address)
}
