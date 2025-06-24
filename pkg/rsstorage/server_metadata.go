package rsstorage

// Copyright (C) 2022 by Posit, PBC

import (
	"context"
	"io"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
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
