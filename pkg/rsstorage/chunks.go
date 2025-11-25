package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/types"
)

var (
	ErrNoChunkMetadata = errors.New("metadata not found for chunked asset")
	ErrNoChunk         = errors.New("chunk not found for chunked asset")
)

const (
	DefaultChunkPollTimeout = time.Second * 5
	DefaultMaxChunkAttempts = 100
)

type ChunkUtils interface {
	WriteChunked(ctx context.Context, dir, address string, sz uint64, resolve types.Resolver) error
	ReadChunked(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, error)
}

type ChunkWaiter interface {
	WaitForChunk(ctx context.Context, c *types.ChunkNotification)
}

type ChunkNotifier interface {
	Notify(ctx context.Context, c *types.ChunkNotification) error
}
