package internal

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

type DefaultChunkUtils struct {
	ChunkSize   uint64
	Server      rsstorage.StorageServer
	Waiter      rsstorage.ChunkWaiter
	Notifier    rsstorage.ChunkNotifier
	PollTimeout time.Duration
	MaxAttempts int
}

func (w *DefaultChunkUtils) WriteChunked(
	ctx context.Context,
	dir string,
	address string,
	sz uint64,
	resolve types.Resolver,
) (err error) {

	// Determine number of chunks we will need to create
	numChunks := sz / w.ChunkSize
	if sz%w.ChunkSize > 0 {
		numChunks++
	}

	// Clear the directory if it already exists
	err = w.Server.Remove(ctx, dir, address)
	if err != nil {
		return nil
	}

	// Write an `info.json` to the directory
	info := types.ChunksInfo{
		ChunkSize: w.ChunkSize,
		NumChunks: numChunks,
		FileSize:  sz,
		ModTime:   time.Now(),
	}
	infoResolver := func(writer io.Writer) (dir, address string, err error) {
		en := json.NewEncoder(writer)
		err = en.Encode(&info)
		return
	}
	chunkDir := filepath.Join(dir, address)
	_, _, err = w.Server.Put(ctx, infoResolver, chunkDir, "info.json")
	if err != nil {
		return
	}

	// Pipe results from resolver (pW) to chunk writer (pR)
	pR, pW := io.Pipe()

	// Clean up on error
	defer func(err *error) {
		if *err != nil {
			// TODO: Handle this error gracefully
			w.Server.Remove(ctx, dir, address)
		}
	}(&err)

	// Write all chunks
	results := make(chan uint64)
	errs := make(chan error)
	go w.writeChunks(ctx, numChunks, chunkDir, pR, results, errs)

	// Resolve/get the data we need
	resolverErrs := make(chan error)
	go func() {
		defer close(resolverErrs)
		defer pW.Close()
		_, _, err = resolve(pW)
		if err != nil {
			resolverErrs <- err
		}
	}()

	// Wait for all results to complete
	err = func() error {
		for {
			select {
			case err = <-resolverErrs:
				if err != nil {
					return err
				}
			case err = <-errs:
				return err
			case count := <-results:
				err = w.Notifier.Notify(&types.ChunkNotification{
					Address: address,
					Chunk:   count,
				})
				if err != nil {
					// TODO: Update DefaultChunkUtils to acceptable a logger
					log.Printf("Error notifying store of chunk completion for address=%s; chunk=%d: %s", address, count, err)
				}
				if count == numChunks {
					return nil
				}
			}
		}
	}()
	if err != nil {
		return
	}

	// Update `info.json` when complete
	info.Complete = true
	info.ModTime = time.Now()
	_, _, err = w.Server.Put(ctx, infoResolver, chunkDir, "info.json")
	if err != nil {
		return
	}

	return
}

func (w *DefaultChunkUtils) writeChunks(
	ctx context.Context,
	numChunks uint64,
	chunkDir string,
	r *io.PipeReader,
	results chan uint64,
	errs chan error,
) {
	// TODO: Handle this error
	defer r.Close()
	defer close(results)
	defer close(errs)
	for i := uint64(1); i <= numChunks; i++ {
		err := func() error {
			resolve := func(writer io.Writer) (dir, address string, err error) {
				_, err = io.CopyN(writer, r, int64(w.ChunkSize))
				if err != nil && err == io.EOF {
					err = nil
				}
				return
			}

			chunkFile := fmt.Sprintf("%08d", i)
			_, _, err := w.Server.Put(ctx, resolve, chunkDir, chunkFile)
			if err != nil {
				return err
			}

			results <- i
			return nil
		}()
		if err != nil {
			errs <- err
			return
		}
	}
}

func (w *DefaultChunkUtils) ReadChunked(
	ctx context.Context,
	dir string,
	address string,
) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, error) {
	chunkDir := filepath.Join(dir, address)

	infoFile, _, _, _, ok, err := w.Server.Get(ctx, chunkDir, "info.json")
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	} else if !ok {
		return nil, nil, 0, time.Time{}, rsstorage.ErrNoChunkMetadata
	}
	defer infoFile.Close()

	info := types.ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	pR, pW := io.Pipe()
	go w.readChunks(ctx, address, chunkDir, info.NumChunks, info.Complete, pW)

	return pR, &info, int64(info.FileSize), info.ModTime, nil
}

func (w *DefaultChunkUtils) readChunks(
	ctx context.Context,
	address string,
	chunkDir string,
	numChunks uint64,
	complete bool,
	writer *io.PipeWriter,
) {
	// TODO: Handle this error
	defer writer.Close()

	for i := uint64(1); i <= numChunks; i++ {
		err := w.retryingChunkRead(ctx, i, address, chunkDir, complete, writer)
		if err != nil {
			// TODO: Handle this error
			writer.CloseWithError(err)
			return
		}
	}

	return
}

func (w *DefaultChunkUtils) retryingChunkRead(
	ctx context.Context,
	chunkIndex uint64,
	address string,
	chunkDir string,
	complete bool,
	writer *io.PipeWriter,
) (err error) {
	attempts := 0
	for {
		attempts += 1
		var done bool
		done, err = w.tryChunkRead(ctx, attempts, chunkIndex, address, chunkDir, complete, writer)
		if err != nil || done {
			return
		}
	}
}

func (w *DefaultChunkUtils) tryChunkRead(
	ctx context.Context,
	attempts int,
	chunkIndex uint64,
	address string,
	chunkDir string,
	complete bool,
	writer *io.PipeWriter,
) (bool, error) {
	chunkFile := fmt.Sprintf("%08d", chunkIndex)

	// Open the chunks sequentially
	chunk, _, _, _, ok, err := w.Server.Get(ctx, chunkDir, chunkFile)
	if err != nil {
		return false, fmt.Errorf("error opening chunk file at %s: %s", chunkDir, err)
	} else if !ok {
		if !complete {
			// If we've waited 5 minutes for this chunk to appear, err to avoid
			// blocking forever
			if attempts > w.MaxAttempts {
				return false, rsstorage.ErrNoChunk
			}
			// Wait for the next chunk, then retry in for loop.
			w.Waiter.WaitForChunk(&types.ChunkNotification{
				Timeout: w.PollTimeout,
				Address: address,
				Chunk:   chunkIndex,
			})
			return false, nil
		} else {
			// If already done, return error
			return false, rsstorage.ErrNoChunk
		}
	}
	// TODO: handle this error
	defer chunk.Close()

	// Read the current chunk
	_, err = io.Copy(writer, chunk)
	if err != nil {
		return false, fmt.Errorf("error reading from chunk: %s", err)
	}

	return true, nil
}

func FilterChunks(input []types.StoredItem) []types.StoredItem {
	output := make([]types.StoredItem, 0)
	chunkDirs := make(map[string]bool)

	// Find all directories that are chunked and append one
	// result for each chunked directory.
	for _, i := range input {
		if i.Dir != "" && i.Address == "info.json" {
			chunkDirs[i.Dir] = true
			d, f := filepath.Split(i.Dir)
			output = append(output, types.StoredItem{
				Dir:     strings.TrimSuffix(d, "/"),
				Address: f,
				Chunked: true,
			})
		}
	}

	// Eliminate chunk data from results
	for _, i := range input {
		if !chunkDirs[i.Dir] {
			output = append(output, i)
		}
	}

	return output
}
