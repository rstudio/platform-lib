package rsstorage

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

var (
	// ErrRangeNotSupported means the server cannot do a ranged read. Callers fall back to Get.
	ErrRangeNotSupported = errors.New("ranged read not supported")

	// ErrInvalidRange means the offset is negative or the length is not positive.
	ErrInvalidRange = errors.New("invalid range")

	// ErrRangeNotSatisfiable means the offset is at or past the end of the object.
	// The returned error is a *RangeNotSatisfiableError that carries the object size.
	ErrRangeNotSatisfiable = errors.New("range not satisfiable")
)

// RangeNotSatisfiableError matches ErrRangeNotSatisfiable with errors.Is.
type RangeNotSatisfiableError struct {
	Size int64
}

func (e *RangeNotSatisfiableError) Error() string {
	return fmt.Sprintf("%s: offset is at or past the end of the %d byte object", ErrRangeNotSatisfiable, e.Size)
}

func (e *RangeNotSatisfiableError) Is(target error) bool {
	return target == ErrRangeNotSatisfiable
}

// RangeGetter is an optional interface for a StorageServer that can read part of an object.
//
// GetRange returns the bytes [offset, offset+length), clamped at the end of the object.
// Size and modtime are for the whole object. Errors: see the Err vars above.
type RangeGetter interface {
	GetRange(ctx context.Context, dir, address string, offset, length int64) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error)
}

// ClampRange checks a range against the object size and returns the number of bytes to read.
func ClampRange(offset, length, size int64) (int64, error) {
	if offset < 0 || length < 1 {
		return 0, ErrInvalidRange
	}
	if offset >= size {
		return 0, &RangeNotSatisfiableError{Size: size}
	}
	if length > size-offset {
		length = size - offset
	}
	return length, nil
}

// ReadChunkedRange reads a range of a chunked object from server. It opens only the chunks
// the range touches. If server is a RangeGetter, each chunk is read with a ranged read, so
// bytes before the offset are never fetched. Otherwise bytes before the offset in the first
// chunk are read and dropped.
func ReadChunkedRange(
	ctx context.Context,
	server StorageServer,
	dir, address string,
	offset, length int64,
) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	chunkDir := filepath.Join(dir, address)
	infoFile, _, _, _, ok, err := server.Get(ctx, chunkDir, "info.json")
	if err != nil || !ok {
		return nil, nil, 0, time.Time{}, false, err
	}
	info := types.ChunksInfo{}
	err = json.NewDecoder(infoFile).Decode(&info)
	closeErr := infoFile.Close()
	if err != nil {
		return nil, nil, 0, time.Time{}, false, err
	}
	if closeErr != nil {
		return nil, nil, 0, time.Time{}, false, closeErr
	}

	size := int64(info.FileSize)
	n, err := ClampRange(offset, length, size)
	if err != nil {
		return nil, nil, 0, time.Time{}, false, err
	}
	if info.ChunkSize == 0 {
		return nil, nil, 0, time.Time{}, false, fmt.Errorf("chunked asset %s has no chunk size", address)
	}

	chunkSize := int64(info.ChunkSize)
	return &chunkRangeReader{
		ctx:       ctx,
		server:    server,
		chunkDir:  chunkDir,
		chunkSize: chunkSize,
		next:      offset/chunkSize + 1,
		skip:      offset % chunkSize,
		remaining: n,
	}, &info, size, info.ModTime, true, nil
}

// chunkRangeReader reads consecutive chunks, lazily, for ReadChunkedRange.
type chunkRangeReader struct {
	ctx       context.Context
	server    StorageServer
	chunkDir  string
	chunkSize int64
	next      int64 // 1-based index of the next chunk to open
	skip      int64 // bytes to skip in the next chunk
	remaining int64 // bytes left to return
	cur       io.ReadCloser
}

func (r *chunkRangeReader) open() error {
	// Never ask for more than is left, so the last chunk is read only as far as needed.
	want := min(r.remaining, r.chunkSize-r.skip)
	name := fmt.Sprintf("%08d", r.next)

	var rc io.ReadCloser
	if rg, ok := r.server.(RangeGetter); ok {
		got, _, _, _, found, err := rg.GetRange(r.ctx, r.chunkDir, name, r.skip, want)
		if err != nil {
			return err
		}
		if !found {
			return ErrNoChunk
		}
		rc = got
	} else {
		got, _, _, _, found, err := r.server.Get(r.ctx, r.chunkDir, name)
		if err != nil {
			return err
		}
		if !found {
			return ErrNoChunk
		}
		if _, err = io.CopyN(io.Discard, got, r.skip); err != nil {
			_ = got.Close()
			return err
		}
		rc = &limitedReadCloser{Reader: io.LimitReader(got, want), Closer: got}
	}
	r.cur = rc
	r.next++
	r.skip = 0
	return nil
}

func (r *chunkRangeReader) Read(p []byte) (int, error) {
	for {
		if r.remaining == 0 {
			return 0, io.EOF
		}
		if r.cur == nil {
			if err := r.open(); err != nil {
				return 0, err
			}
		}
		n, err := r.cur.Read(p)
		r.remaining -= int64(n)
		if err == io.EOF {
			closeErr := r.cur.Close()
			r.cur = nil
			if closeErr != nil {
				return n, closeErr
			}
			if r.remaining > 0 && n == 0 {
				continue
			}
			if r.remaining > 0 {
				return n, nil
			}
			return n, io.EOF
		}
		return n, err
	}
}

func (r *chunkRangeReader) Close() error {
	if r.cur == nil {
		return nil
	}
	err := r.cur.Close()
	r.cur = nil
	return err
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}
