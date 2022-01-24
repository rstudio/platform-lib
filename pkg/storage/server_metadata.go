package storage

// Copyright (C) 2022 by RStudio, PBC

import (
	"io"
	"time"
)

// Overrides the `Get` and `Put` methods for interacting with the database
// to record access times for the persistent storage services.
type MetadataPersistentStorageServer struct {
	PersistentStorageServer
	store PersistentStorageStore
	name  string
}

func NewMetadataPersistentStorageServer(name string, server PersistentStorageServer, store PersistentStorageStore) PersistentStorageServer {
	return &MetadataPersistentStorageServer{
		PersistentStorageServer: server,
		name:                    name,
		store:                   store,
	}
}

func (s *MetadataPersistentStorageServer) Get(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, bool, error) {
	r, c, sz, ts, ok, err := s.PersistentStorageServer.Get(dir, address)
	if ok && err == nil {
		// Record access of cached object
		err = s.store.CacheObjectMarkUse(s.name, dir+"/"+address, time.Now())
		if err != nil {
			return nil, nil, 0, time.Time{}, false, err
		}
	}
	return r, c, sz, ts, ok, err
}

func (s *MetadataPersistentStorageServer) PutChunked(resolve Resolver, dir, address string, sz uint64) (string, string, error) {
	dirOut, addrOut, err := s.PersistentStorageServer.PutChunked(resolve, dir, address, sz)
	if err == nil {
		// Record cached object
		err = s.store.CacheObjectEnsureExists(s.name, dirOut+"/"+addrOut)
		if err != nil {
			return "", "", err
		}
	}
	return dirOut, addrOut, err
}

func (s *MetadataPersistentStorageServer) Put(resolve Resolver, dir, address string) (string, string, error) {
	dirOut, addrOut, err := s.PersistentStorageServer.Put(resolve, dir, address)
	if err == nil {
		// Record cached object
		err = s.store.CacheObjectEnsureExists(s.name, dirOut+"/"+addrOut)
		if err != nil {
			return "", "", err
		}
	}
	return dirOut, addrOut, err
}

func (s *MetadataPersistentStorageServer) Base() PersistentStorageServer {
	return s.PersistentStorageServer.Base()
}
