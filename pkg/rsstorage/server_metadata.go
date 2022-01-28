package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"io"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

// MetadataStorageServer overrides the `Get` and `Put` methods for interacting with the database
// to record access times.
type MetadataStorageServer struct {
	StorageServer
	store CacheStore
	name  string
}

func NewMetadataStorageServer(name string, server StorageServer, store CacheStore) StorageServer {
	return &MetadataStorageServer{
		StorageServer: server,
		name:          name,
		store:         store,
	}
}

func (s *MetadataStorageServer) Get(dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	r, c, sz, ts, ok, err := s.StorageServer.Get(dir, address)
	if ok && err == nil {
		// Record access of cached object
		err = s.store.CacheObjectMarkUse(s.name, dir+"/"+address, time.Now())
		if err != nil {
			return nil, nil, 0, time.Time{}, false, err
		}
	}
	return r, c, sz, ts, ok, err
}

func (s *MetadataStorageServer) PutChunked(resolve types.Resolver, dir, address string, sz uint64) (string, string, error) {
	dirOut, addrOut, err := s.StorageServer.PutChunked(resolve, dir, address, sz)
	if err == nil {
		// Record cached object
		err = s.store.CacheObjectEnsureExists(s.name, dirOut+"/"+addrOut)
		if err != nil {
			return "", "", err
		}
	}
	return dirOut, addrOut, err
}

func (s *MetadataStorageServer) Put(resolve types.Resolver, dir, address string) (string, string, error) {
	dirOut, addrOut, err := s.StorageServer.Put(resolve, dir, address)
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
