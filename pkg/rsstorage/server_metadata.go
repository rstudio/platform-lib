package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"io"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

// MetadataStorageServer overrides the `Get` and `Put` methods for interacting with the database
// to record access times.
type MetadataStorageServer struct {
	StorageServer
	store CacheStore
	name  string
}

type MetadataStorageServerArgs struct {
	Name   string
	Server StorageServer
	Store  CacheStore
}

func NewMetadataStorageServer(args MetadataStorageServerArgs) StorageServer {
	return &MetadataStorageServer{
		StorageServer: args.Server,
		name:          args.Name,
		store:         args.Store,
	}
}

// EnumeratePrefix implements PrefixEnumerator by forwarding to the wrapped
// server.
//
// This has to be written out. MetadataStorageServer embeds the StorageServer
// INTERFACE, and embedding an interface promotes only the methods that interface
// declares. EnumeratePrefix is not one of them, so without this method a
// PrefixEnumerator type assertion against a wrapped server fails and the caller
// silently loses prefix scoping -- on exactly the servers that are wrapped.
//
// Enumeration is a read that records no access time, so there is nothing to
// account for here; contrast Get, which exists to do that bookkeeping.
func (s *MetadataStorageServer) EnumeratePrefix(ctx context.Context, prefix string) ([]types.StoredItem, error) {
	inner, ok := s.StorageServer.(PrefixEnumerator)
	if !ok {
		return nil, ErrPrefixEnumerationUnsupported
	}
	return inner.EnumeratePrefix(ctx, prefix)
}

func (s *MetadataStorageServer) Get(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	r, c, sz, ts, ok, err := s.StorageServer.Get(ctx, dir, address)
	if ok && err == nil {
		// Record access of cached object
		err = s.store.CacheObjectMarkUse(s.name, dir+"/"+address, time.Now())
		if err != nil {
			return nil, nil, 0, time.Time{}, false, err
		}
	}
	return r, c, sz, ts, ok, err
}

func (s *MetadataStorageServer) PutChunked(ctx context.Context, resolve types.Resolver, dir, address string, sz uint64) (string, string, error) {
	dirOut, addrOut, err := s.StorageServer.PutChunked(ctx, resolve, dir, address, sz)
	if err == nil {
		// Record cached object
		err = s.store.CacheObjectEnsureExists(s.name, dirOut+"/"+addrOut)
		if err != nil {
			return "", "", err
		}
	}
	return dirOut, addrOut, err
}

func (s *MetadataStorageServer) Put(ctx context.Context, resolve types.Resolver, dir, address string) (string, string, error) {
	dirOut, addrOut, err := s.StorageServer.Put(ctx, resolve, dir, address)
	if err == nil {
		// Record cached object
		err = s.store.CacheObjectEnsureExists(s.name, dirOut+"/"+addrOut)
		if err != nil {
			return "", "", err
		}
	}
	return dirOut, addrOut, err
}

func (s *MetadataStorageServer) Base() StorageServer {
	return s.StorageServer.Base()
}
