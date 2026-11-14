package rsstorage

// Copyright (C) 2026 by Posit Software, PBC

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

type RangeSuite struct{}

var _ = check.Suite(&RangeSuite{})

// mapServer serves objects from a map and records every Get. It has no GetRange.
type mapServer struct {
	DummyStorageServer
	objects map[string][]byte
	gets    []string
}

func (s *mapServer) Get(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	key := dir + "/" + address
	s.gets = append(s.gets, key)
	data, ok := s.objects[key]
	if !ok {
		return nil, nil, 0, time.Time{}, false, nil
	}
	return io.NopCloser(bytes.NewReader(data)), nil, int64(len(data)), time.Time{}, true, nil
}

// rangeMapServer adds a GetRange that returns canned values.
type rangeMapServer struct {
	mapServer
	err    error
	called [3]int64 // offset, length, calls
}

func (s *rangeMapServer) GetRange(ctx context.Context, dir, address string, offset, length int64) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	s.called = [3]int64{offset, length, s.called[2] + 1}
	if s.err != nil {
		return nil, nil, 0, time.Time{}, false, s.err
	}
	return io.NopCloser(bytes.NewReader([]byte("xyz"))), nil, 10, time.Time{}, true, nil
}

func (s *RangeSuite) TestClampRange(c *check.C) {
	n, err := ClampRange(0, 10, 100)
	c.Check(err, check.IsNil)
	c.Check(n, check.Equals, int64(10))

	n, err = ClampRange(95, 10, 100)
	c.Check(err, check.IsNil)
	c.Check(n, check.Equals, int64(5))

	// RFC 9110: a first position at or past the length is not satisfiable.
	for _, off := range []int64{100, 101} {
		_, err = ClampRange(off, 1, 100)
		var rerr *RangeNotSatisfiableError
		c.Assert(errors.As(err, &rerr), check.Equals, true)
		c.Check(rerr.Size, check.Equals, int64(100))
		c.Check(errors.Is(err, ErrRangeNotSatisfiable), check.Equals, true)
	}
	_, err = ClampRange(0, 1, 0)
	c.Check(errors.Is(err, ErrRangeNotSatisfiable), check.Equals, true)

	for _, in := range [][2]int64{{-1, 5}, {0, 0}, {0, -1}} {
		_, err = ClampRange(in[0], in[1], 100)
		c.Check(errors.Is(err, ErrInvalidRange), check.Equals, true)
	}
}

func (s *RangeSuite) TestMetadataGetRange(c *check.C) {
	ctx := context.Background()
	store := &cacheStore{}

	// Forwards, and records the access like Get.
	inner := &rangeMapServer{}
	server := NewMetadataStorageServer(MetadataStorageServerArgs{Name: "test", Server: inner, Store: store})
	r, _, size, _, ok, err := server.(RangeGetter).GetRange(ctx, "dir", "addr", 4, 3)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(size, check.Equals, int64(10))
	got, _ := io.ReadAll(r)
	c.Check(string(got), check.Equals, "xyz")
	c.Check(inner.called, check.Equals, [3]int64{4, 3, 1})
	c.Check(store.used, check.Equals, "dir/addr")

	// A not-found item records no access.
	store.used = ""
	inner.err = nil
	notFound := &notFoundRange{}
	server = NewMetadataStorageServer(MetadataStorageServerArgs{Name: "test", Server: notFound, Store: store})
	_, _, _, _, ok, err = server.(RangeGetter).GetRange(ctx, "dir", "addr", 0, 1)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(store.used, check.Equals, "")

	// Inner errors pass through, and record nothing.
	inner = &rangeMapServer{err: ErrRangeNotSupported}
	server = NewMetadataStorageServer(MetadataStorageServerArgs{Name: "test", Server: inner, Store: store})
	_, _, _, _, _, err = server.(RangeGetter).GetRange(ctx, "dir", "addr", 0, 1)
	c.Check(errors.Is(err, ErrRangeNotSupported), check.Equals, true)
	c.Check(store.used, check.Equals, "")

	// An inner server without GetRange is not supported (and the wrapper does not panic).
	server = NewMetadataStorageServer(MetadataStorageServerArgs{Name: "test", Server: &DummyStorageServer{}, Store: store})
	_, _, _, _, _, err = server.(RangeGetter).GetRange(ctx, "dir", "addr", 0, 1)
	c.Check(errors.Is(err, ErrRangeNotSupported), check.Equals, true)
	c.Check(store.used, check.Equals, "")
}

type notFoundRange struct{ DummyStorageServer }

func (s *notFoundRange) GetRange(ctx context.Context, dir, address string, offset, length int64) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	return nil, nil, 0, time.Time{}, false, nil
}

// Without a ranged read on the server, only the first chunk is read and trimmed; chunks
// before the offset are never opened.
func (s *RangeSuite) TestReadChunkedRangeFallback(c *check.C) {
	data := make([]byte, 95)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	info, err := json.Marshal(types.ChunksInfo{ChunkSize: 10, NumChunks: 10, FileSize: 95, Complete: true})
	c.Assert(err, check.IsNil)
	server := &mapServer{objects: map[string][]byte{"dir/item/info.json": info}}
	for i := 0; i < 10; i++ {
		server.objects[fmt.Sprintf("dir/item/%08d", i+1)] = data[i*10 : min((i+1)*10, 95)]
	}

	r, _, size, _, ok, err := ReadChunkedRange(context.Background(), server, "dir", "item", 28, 20)
	c.Assert(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(size, check.Equals, int64(95))
	got, err := io.ReadAll(r)
	c.Assert(err, check.IsNil)
	c.Check(r.Close(), check.IsNil)
	c.Check(string(got), check.Equals, string(data[28:48]))
	c.Check(server.gets, check.DeepEquals, []string{
		"dir/item/info.json", "dir/item/00000003", "dir/item/00000004", "dir/item/00000005",
	})
}
