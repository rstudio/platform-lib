package file

// Copyright (C) 2026 by Posit Software, PBC

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/internal/servertest"
)

const rangeChunkSize = 10

// rangeData is 95 bytes: nine full chunks and a short last chunk of 5.
func rangeData() []byte {
	b := make([]byte, 95)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func (s *FileStorageServerSuite) rangeServer() rsstorage.StorageServer {
	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 100)}
	return NewStorageServer(StorageServerArgs{
		Dir:       s.tempDirHelper.Dir(),
		ChunkSize: rangeChunkSize,
		Waiter:    wn,
		Notifier:  wn,
	})
}

func (s *FileStorageServerSuite) putRangeData(c *check.C, server rsstorage.StorageServer, chunked bool) []byte {
	data := rangeData()
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write(data)
		return "", "", err
	}
	var err error
	if chunked {
		_, _, err = server.PutChunked(context.Background(), resolve, "dir", "item", uint64(len(data)))
	} else {
		_, _, err = server.Put(context.Background(), resolve, "dir", "item")
	}
	c.Assert(err, check.IsNil)
	return data
}

type rangeCase struct {
	name           string
	offset, length int64
	want           [2]int // expected slice of the data
}

var rangeCases = []rangeCase{
	{"offset zero", 0, 7, [2]int{0, 7}},
	{"middle, inside one chunk", 12, 5, [2]int{12, 17}},
	{"middle, across chunks", 8, 25, [2]int{8, 33}},
	{"chunk boundary", 20, 10, [2]int{20, 30}},
	{"last chunk", 91, 4, [2]int{91, 95}},
	{"clamped at the end", 90, 1000, [2]int{90, 95}},
	{"last byte", 94, 1, [2]int{94, 95}},
	{"whole object", 0, 95, [2]int{0, 95}},
}

func (s *FileStorageServerSuite) runRangeCases(c *check.C, chunked bool) {
	server := s.rangeServer()
	data := s.putRangeData(c, server, chunked)
	rg := server.(rsstorage.RangeGetter)
	ctx := context.Background()

	for _, tc := range rangeCases {
		r, info, size, mod, ok, err := rg.GetRange(ctx, "dir", "item", tc.offset, tc.length)
		c.Assert(err, check.IsNil, check.Commentf(tc.name))
		c.Assert(ok, check.Equals, true, check.Commentf(tc.name))
		c.Check(size, check.Equals, int64(len(data)), check.Commentf(tc.name))
		c.Check(mod.IsZero(), check.Equals, false, check.Commentf(tc.name))
		c.Check(info != nil, check.Equals, chunked, check.Commentf(tc.name))
		got, err := io.ReadAll(r)
		c.Assert(err, check.IsNil, check.Commentf(tc.name))
		c.Check(r.Close(), check.IsNil, check.Commentf(tc.name))
		c.Check(string(got), check.Equals, string(data[tc.want[0]:tc.want[1]]), check.Commentf(tc.name))
	}

	// Not found.
	_, _, _, _, ok, err := rg.GetRange(ctx, "dir", "missing", 0, 1)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)

	// At the end and past the end are not satisfiable, and carry the size.
	for _, off := range []int64{95, 96, 5000} {
		_, _, _, _, _, err = rg.GetRange(ctx, "dir", "item", off, 1)
		c.Check(err, check.ErrorMatches, ".*not satisfiable.*", check.Commentf("offset %d", off))
		c.Check(errors.Is(err, rsstorage.ErrRangeNotSatisfiable), check.Equals, true, check.Commentf("offset %d", off))
		var rerr *rsstorage.RangeNotSatisfiableError
		c.Assert(errors.As(err, &rerr), check.Equals, true)
		c.Check(rerr.Size, check.Equals, int64(95))
	}

	// Negative offset, zero length and negative length are rejected.
	for _, in := range [][2]int64{{-1, 5}, {0, 0}, {0, -3}} {
		_, _, _, _, _, err = rg.GetRange(ctx, "dir", "item", in[0], in[1])
		c.Check(errors.Is(err, rsstorage.ErrInvalidRange), check.Equals, true, check.Commentf("%v", in))
	}
}

func (s *FileStorageServerSuite) TestGetRangePlain(c *check.C) {
	s.runRangeCases(c, false)
}

func (s *FileStorageServerSuite) TestGetRangeChunked(c *check.C) {
	s.runRangeCases(c, true)
}

// Chunks before the offset are deleted: the read only works if they are never opened.
func (s *FileStorageServerSuite) TestGetRangeChunkedSkipsEarlierChunks(c *check.C) {
	server := s.rangeServer()
	data := s.putRangeData(c, server, true)
	for i := 1; i <= 4; i++ {
		c.Assert(os.Remove(filepath.Join(s.tempDirHelper.Dir(), "dir", "item", fmt.Sprintf("%08d", i))), check.IsNil)
	}

	// Offset 45 is in chunk 5. Read through the short last chunk.
	r, _, _, _, ok, err := server.(rsstorage.RangeGetter).GetRange(context.Background(), "dir", "item", 45, 1000)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
	got, err := io.ReadAll(r)
	c.Assert(err, check.IsNil)
	c.Check(r.Close(), check.IsNil)
	c.Check(bytes.Equal(got, data[45:]), check.Equals, true)

	// And the control: a range that does need chunk 1 fails.
	r, _, _, _, _, err = server.(rsstorage.RangeGetter).GetRange(context.Background(), "dir", "item", 0, 5)
	c.Assert(err, check.IsNil)
	_, err = io.ReadAll(r)
	c.Check(errors.Is(err, rsstorage.ErrNoChunk), check.Equals, true)
	_ = r.Close()
}
