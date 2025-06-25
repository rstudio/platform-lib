package rscache

// Copyright (C) 2025 by Posit Software, PBC

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

func decompressAndDecodeGob(reader io.ReadCloser, compressed bool, typeExample interface{}) (result interface{}, err error) {
	// The data may be gzipped on disk. If so, we need to stream the
	// data through a gzip decoder.
	var uncompressedReader io.ReadCloser
	if compressed {
		uncompressedReader, err = gzip.NewReader(reader)
		if err != nil {
			return
		}
	} else {
		// we don't defer close this because we have already deferred its closure above.
		uncompressedReader = reader
	}

	// If we need to cache this object in memory, then we should pipe the data
	// to the in-memory cache before decoding it. This will save time since we
	// won't need to re-gob-encode it in the memory cache.
	//
	// At this point, the stream represented by `uncompressedReader` may be
	// gob-encoded. Create a reader to represent this gob-encoded data.
	var gobReader io.Reader
	gobReader = uncompressedReader

	// use a closure to perform error handling rather than calling the function
	// directly with the defer statement
	defer func() {
		// Only call Close() on uncompressedReader and not both it and reader,
		// they share state and doing so will call Close() twice, resulting in a
		// 'file already closed' error.
		err = errors.Join(err, uncompressedReader.Close())
	}()

	// Decode and return result into our passed-in example struct
	r := bufio.NewReader(gobReader)
	dec := gob.NewDecoder(r)
	err = dec.Decode(typeExample)
	result = typeExample
	return
}

type MemoryBackedFileCache struct {
	fc                 FileCache
	mc                 MemoryCache
	maxMemoryPerObject int64
}

type MemoryBackedFileCacheConfig struct {
	FileCache          FileCache
	MemoryCache        MemoryCache
	MaxMemoryPerObject int64
}

func NewMemoryBackedFileCache(cfg MemoryBackedFileCacheConfig) *MemoryBackedFileCache {
	return &MemoryBackedFileCache{
		fc:                 cfg.FileCache,
		mc:                 cfg.MemoryCache,
		maxMemoryPerObject: cfg.MaxMemoryPerObject,
	}
}

func (mbfc *MemoryBackedFileCache) Get(ctx context.Context, resolver ResolverSpec) (value CacheReturn) {
	var err error

	address := resolver.Address()

	var ptr *CacheReturn

	if resolver.CacheInMemory && mbfc.mc != nil && mbfc.mc.Enabled() {
		ptr = mbfc.mc.Get(address)
		if !ptr.IsNull() {
			ptr.ReturnedFrom = "memory"
			return *ptr
		}
	}

	ptr = mbfc.fc.Get(ctx, resolver)

	if resolver.CacheInMemory && mbfc.mc != nil && mbfc.mc.Enabled() && ptr.GetSize() < mbfc.maxMemoryPerObject {
		err = mbfc.mc.Put(address, ptr)
		if err != nil {
			slog.Debug(fmt.Sprintf("error caching to memory: %s", err.Error()))
		}
	}
	return *ptr
}

func (mbfc *MemoryBackedFileCache) GetObject(ctx context.Context, resolver ResolverSpec, typeExample interface{}) (value CacheReturn) {
	var err error
	var ptr *CacheReturn

	address := resolver.Address()

	if resolver.CacheInMemory && mbfc.mc != nil && mbfc.mc.Enabled() {
		ptr = mbfc.mc.Get(address)
		if !ptr.IsNull() {
			ptr.ReturnedFrom = "memory"
			return *ptr
		}
	}

	var reader io.ReadCloser
	ptr = mbfc.fc.Get(ctx, resolver)
	reader, err = ptr.AsReader()
	if err != nil {
		return CacheReturn{Err: err}
	}

	var obj interface{}
	obj, err = decompressAndDecodeGob(reader, resolver.Gzip, typeExample)
	if err != nil {
		return CacheReturn{Err: err}
	}

	ptr.Value = obj
	value = *ptr

	if resolver.CacheInMemory && mbfc.mc != nil && mbfc.mc.Enabled() && ptr.GetSize() < mbfc.maxMemoryPerObject {
		err = mbfc.mc.Put(address, ptr)
		if err != nil {
			slog.Debug(fmt.Sprintf("error caching to memory: %s", err.Error()))
		}
	}

	return
}

func (mbfc *MemoryBackedFileCache) Uncache(ctx context.Context, resolver ResolverSpec) (err error) {
	if mbfc.mc != nil && mbfc.mc.Enabled() {
		mbfc.mc.Uncache(resolver.Address())
	}
	err = mbfc.fc.Uncache(ctx, resolver)
	return
}

func (mbfc *MemoryBackedFileCache) Check(ctx context.Context, resolver ResolverSpec) (bool, error) {
	if mbfc.mc != nil && mbfc.mc.Enabled() {
		obj := mbfc.mc.Get(resolver.Address())
		if !obj.IsNull() && obj.Error() == nil {
			return true, nil
		}
	}
	return mbfc.fc.Check(ctx, resolver)
}

func (mbfc *MemoryBackedFileCache) Head(ctx context.Context, resolver ResolverSpec) (size int64, modTime time.Time, err error) {
	if mbfc.mc != nil && mbfc.mc.Enabled() {
		obj := mbfc.mc.Get(resolver.Address())
		if !obj.IsNull() && obj.Error() == nil {
			size = obj.GetSize()
			modTime = obj.GetTimestamp()
			return
		}
	}
	return mbfc.fc.Head(ctx, resolver)
}
