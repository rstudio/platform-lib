package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/rstudio/platform-lib/v3/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/types"
)

type QueueWork interface {
	Type() uint64
}

type Queue interface {
	AddressedPush(ctx context.Context, priority uint64, groupId int64, address string, work QueueWork) error
	PollAddress(ctx context.Context, address string) (errs <-chan error)
}

type DuplicateMatcher interface {
	IsDuplicate(err error) bool
}

type OptionalRecurser interface {
	OptionallyRecurse(ctx context.Context, run func())
}

type FileCache interface {
	// Get The queue Work to perform if the item is not cached
	Get(ctx context.Context, resolver ResolverSpec) (value *CacheReturn)

	// Check Returns `true` if we have the entire asset cached.
	Check(ctx context.Context, resolver ResolverSpec) (ok bool, err error)

	Head(ctx context.Context, resolver ResolverSpec) (int64, time.Time, error)

	Uncache(ctx context.Context, resolver ResolverSpec) error
}

type FileCacheConfig struct {
	Queue            Queue
	DuplicateMatcher DuplicateMatcher
	StorageServer    rsstorage.StorageServer
	Recurser         OptionalRecurser
	Timeout          time.Duration
}

func NewFileCache(cfg FileCacheConfig) FileCache {
	return &fileCache{
		queue:            cfg.Queue,
		duplicateMatcher: cfg.DuplicateMatcher,
		server:           cfg.StorageServer,
		timeout:          cfg.Timeout,
		recurser:         cfg.Recurser,

		retry: time.Millisecond * 200,
	}
}

// FileCache implementation
type fileCache struct {

	// A queue
	queue            Queue
	duplicateMatcher DuplicateMatcher

	// Knows how to retrieve or create cached files
	server rsstorage.StorageServer

	recurser OptionalRecurser

	// timeout for waiting for NFS sync
	timeout time.Duration

	// retry delay
	retry time.Duration
}

func (o *fileCache) retryingGet(ctx context.Context, dir, address string, get func() bool) bool {

	// Record start time
	start := time.Now()
	flushed := 0

	// Wrap the get
	flushingGet := func() bool {
		if get() {
			return true
		} else {
			// Attempt to flush the NFS cache
			o.server.Flush(ctx, dir, address)
			flushed++
		}
		return get()
	}

	// Preemptive get attempt
	if flushingGet() {
		if flushed == 0 {
			slog.Log(ctx, LevelTrace, "Found cached item immediately", "address", address)
		} else {
			slog.Log(ctx, LevelTrace, "Found cached item after one flush", "address", address)
		}
		return true
	}

	retry := time.NewTicker(o.retry)
	timeout := time.NewTimer(o.timeout)
	defer retry.Stop()
	defer timeout.Stop()
	for {
		select {
		case <-retry.C:
			if flushingGet() {
				elapsed := time.Now().Sub(start) / 1000000
				slog.Log(
					ctx,
					LevelTrace,
					fmt.Sprintf(
						"Found cached item at address '%s' after %d ms and %d flushes",
						address,
						elapsed,
						flushed,
					),
				)
				return true
			}
		case <-timeout.C:
			return false
		}
	}
}

func (o *fileCache) Check(ctx context.Context, resolver ResolverSpec) (ok bool, err error) {
	address := resolver.Address()

	var chunked *types.ChunksInfo
	ok, chunked, _, _, err = o.server.Check(ctx, resolver.Dir(), address)
	if chunked != nil && !chunked.Complete {
		// We treat incomplete chunked assets as missing
		ok = false
	}
	return
}

func (o *fileCache) Head(ctx context.Context, resolver ResolverSpec) (size int64, modTime time.Time, err error) {
	head := func() bool {
		var ok bool
		var chunks *types.ChunksInfo
		ok, chunks, size, modTime, err = o.server.Check(ctx, resolver.Dir(), resolver.Address())

		// `Check` returns `ok==true` for chunked assets even if they're not complete.
		// Since we don't know if the queue has in-progress work for incomplete chunked
		// assets, we should indicate that `ok = false` so the work is pushed into the
		// queue if not already in progress. This ensures that partial chunked assets
		// that have been aborted before storage fulfillment are restarted.
		if ok && chunks != nil && !chunks.Complete {
			ok = false
		}

		// If we got the item successfully (ok), or if there was an error (err != nil),
		// then we return `true` so the caller knows we have all the info we need
		// to return from the parent (`Get`) function.
		//
		// At this point, the `reader` and `err` named return values are appropriately
		// set, so the parent function can simply `return`.
		return ok || err != nil
	}

	// Attempt to head the cached item preemptively.
	if head() {
		return
	}

	o.recurser.OptionallyRecurse(ctx, func() {

		// Push a job into the queue. AddressedPush is a no-op if the queue
		// already contains an item with the same address.
		err = o.queue.AddressedPush(ctx, resolver.Priority, resolver.GroupId, resolver.Address(), resolver.Work)
		if o.duplicateMatcher.IsDuplicate(err) {
			// Do nothing since; someone else has already inserted the work we need.
			slog.Debug("FileCache: duplicate address push", "address", resolver.Address())
		} else if err != nil {
			return
		}

		// Find out when the job in the queue is done
		errCh := o.queue.PollAddress(ctx, resolver.Address())

		// Wait
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case err = <-errCh:
				if err != nil {
					return
				}
				if o.retryingGet(ctx, resolver.Dir(), resolver.Address(), head) {
					return
				} else {
					slog.Debug("error: FileCache reported address complete, but item was not found in cache", "address", resolver.Address())
					err = fmt.Errorf("error: FileCache reported address '%s' complete, but item was not found in cache", resolver.Address())
					return
				}
			}
		}
	})

	return
}

func (o *fileCache) Get(ctx context.Context, resolver ResolverSpec) (value *CacheReturn) {

	var (
		ok      bool
		chunks  *types.ChunksInfo
		size    int64
		modTime time.Time
		reader  io.ReadCloser
		err     error
	)

	address := resolver.Address()

	get := func() (ok bool) {
		reader, _, size, modTime, ok, err = o.server.Get(ctx, resolver.Dir(), address)

		// If we got the item successfully (ok), or if there was an error (err != nil),
		// then we return `true` so the caller knows we have all the info we need
		// to return from the parent (`Get`) function.
		//
		// At this point, the `reader` and `err` named return values are appropriately
		// set, so the parent function can simply `return`.
		return ok || err != nil
	}

	// Do a pre-emptive check to see if we have the asset in full.
	//
	// `Check` returns `ok==true` for chunked assets even if they're not complete.
	// Since we don't know if the queue has in-progress work for incomplete chunked
	// assets, we should indicate that `ok = false` so the work is pushed into the
	// queue if not already in progress. This ensures that partial chunked assets
	// that have been aborted before storage fulfillment are restarted.
	ok, chunks, size, modTime, err = o.server.Check(ctx, resolver.Dir(), address)
	if ok && chunks != nil && !chunks.Complete {
		ok = false
	}

	// Attempt to get the cached item preemptively, if it is available in full.
	if ok && get() {
		return &CacheReturn{
			Complete:     true,
			Value:        reader,
			ReturnedFrom: "file",
			Size:         size,
			Timestamp:    modTime,
			Err:          err,
			CacheKey:     address,
		}
	}

	// Otherwise, push the work into the queue and wait for the asset to be ready.
	o.recurser.OptionallyRecurse(ctx, func() {

		// Push a job into the queue. AddressedPush is a no-op if the queue
		// already contains an item with the same address.
		err = o.queue.AddressedPush(ctx, resolver.Priority, resolver.GroupId, address, resolver.Work)
		if o.duplicateMatcher.IsDuplicate(err) {
			// Do nothing since; someone else has already inserted the work we need.
			slog.Debug("FileCache: duplicate address push", "address", address)
		} else if err != nil {
			value = &CacheReturn{
				Err:      err,
				CacheKey: address,
			}
			return
		}

		// Find out when the job in the queue is done
		errCh := o.queue.PollAddress(ctx, address)

		// Wait
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case err = <-errCh:
				if err != nil {
					return
				}
				if o.retryingGet(ctx, resolver.Dir(), address, get) {
					return
				} else {
					err = fmt.Errorf("error: FileCache reported address '%s' complete, but item was not found in cache", address)
					slog.Debug(err.Error())
					return
				}
			}
		}
	})

	value = &CacheReturn{
		Complete:     true,
		Value:        reader,
		ReturnedFrom: "file",
		Size:         size,
		Timestamp:    modTime,
		Err:          err,
		CacheKey:     address,
	}

	return
}

func (o *fileCache) Uncache(ctx context.Context, resolver ResolverSpec) error {
	return o.server.Remove(ctx, resolver.Dir(), resolver.Address())
}
