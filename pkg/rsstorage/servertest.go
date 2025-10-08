package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

type GetResult struct {
	GetReader  io.ReadCloser
	GetOk      bool
	GetChunked *types.ChunksInfo
	GetSize    int64
	GetModTime time.Time
	GetErr     error
}

type DummyStorageServer struct {
	GetAttempts    int
	GetReader      io.ReadCloser
	GetOk          bool
	GetChunked     *types.ChunksInfo
	GetSize        int64
	GetModTime     time.Time
	GetErr         error
	GetMap         map[string]GetResult
	RemoveErr      error
	RemoveMap      map[string]bool
	RemoveCount    int
	Flushed        int
	PutDelay       time.Duration
	PutErr         error
	PutCalled      int
	PutChunks      bool
	Address        []string
	Placed         []string
	Buffer         *bytes.Buffer
	EnumItems      []types.StoredItem
	EnumErr        error
	MoveErr        error
	Moved          []string
	CopyErr        error
	Copied         []string
	Location       string
	MockType       types.StorageType
	MockDir        string
	MockUsage      types.Usage
	MockUsageError error

	GetCheckLock sync.RWMutex
}

func (f *DummyStorageServer) Check(ctx context.Context, dir, address string) (bool, *types.ChunksInfo, int64, time.Time, error) {
	f.GetCheckLock.RLock()
	defer f.GetCheckLock.RUnlock()

	f.GetAttempts++
	if f.GetMap != nil {
		return f.GetMap[address].GetOk, f.GetMap[address].GetChunked, f.GetMap[address].GetSize, f.GetMap[address].GetModTime, f.GetMap[address].GetErr
	} else {
		return f.GetOk, f.GetChunked, f.GetSize, f.GetModTime, f.GetErr
	}
}

func (f *DummyStorageServer) Dir() string {
	return f.MockDir
}

func (f *DummyStorageServer) Type() types.StorageType {
	return f.MockType
}

func (f *DummyStorageServer) CalculateUsage() (types.Usage, error) {
	return f.MockUsage, f.MockUsageError
}

func (f *DummyStorageServer) Get(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	f.GetCheckLock.RLock()
	defer f.GetCheckLock.RUnlock()

	f.GetAttempts++
	if f.GetMap != nil {
		return f.GetMap[address].GetReader, f.GetMap[address].GetChunked, f.GetMap[address].GetSize, f.GetMap[address].GetModTime, f.GetMap[address].GetOk, f.GetMap[address].GetErr
	} else {
		return f.GetReader, f.GetChunked, f.GetSize, f.GetModTime, f.GetOk, f.GetErr
	}
}

func (f *DummyStorageServer) Put(ctx context.Context, resolve types.Resolver, dir, address string) (string, string, error) {
	if f.PutDelay > 0 {
		time.Sleep(f.PutDelay)
	}
	f.PutCalled++
	if f.PutErr != nil {
		return "", "", f.PutErr
	} else {
		f.Buffer = bytes.NewBuffer([]byte{})
		d, a, err := resolve(f.Buffer)
		if dir == "" && address == "" {
			dir = d
			address = a
		}
		addressAndOutput := fmt.Sprintf("%s-%s", dir, f.Buffer.String())
		f.Address = append(f.Address, address)
		f.Placed = append(f.Placed, addressAndOutput)
		return dir, address, err
	}
}

func (f *DummyStorageServer) PutChunked(ctx context.Context, resolve types.Resolver, dir, address string, sz uint64) (string, string, error) {
	f.PutChunks = true
	return f.Put(ctx, resolve, dir, address)
}

func (f *DummyStorageServer) Remove(ctx context.Context, dir, address string) error {
	if f.RemoveMap == nil {
		f.RemoveMap = make(map[string]bool)
	}
	if f.RemoveErr == nil {
		f.RemoveCount++
		f.RemoveMap[dir+"/"+address] = true
	}
	return f.RemoveErr
}

func (f *DummyStorageServer) Flush(ctx context.Context, dir, address string) {
	f.Flushed++
}

func (f *DummyStorageServer) Enumerate(ctx context.Context) ([]types.StoredItem, error) {
	return f.EnumItems, f.EnumErr
}

func (f *DummyStorageServer) Move(ctx context.Context, dir, address string, server StorageServer) error {
	if f.MoveErr == nil && f.Moved != nil {
		f.Moved = append(f.Moved, dir+"/"+address)
	}
	return f.MoveErr
}

func (f *DummyStorageServer) Copy(ctx context.Context, dir, address string, server StorageServer) error {
	if f.CopyErr == nil && f.Copied != nil {
		f.Copied = append(f.Copied, dir+"/"+address)
	}
	return f.CopyErr
}

func (f *DummyStorageServer) Locate(dir, address string) string {
	return f.Location
}

func (f *DummyStorageServer) Base() StorageServer {
	return f
}
