package internal

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"

	"gopkg.in/check.v1"
)

type ChunksSuite struct{}

var _ = check.Suite(&ChunksSuite{})

func (s *ChunksSuite) TestWriteChunkedZeroChunkSize(c *check.C) {
	w := &DefaultChunkUtils{}
	err := w.WriteChunked(context.Background(), "dir", "address", 10, nil)
	c.Assert(err, check.ErrorMatches, "chunk size must be greater than zero")
}
