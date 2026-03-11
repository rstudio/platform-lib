package internal

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/rstudio/platform-lib/v3/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/types"
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
		// TODO: Handle this error gracefully
		defer pW.Close()
		_, _, err = resolve(pW)
		if err != nil {
			resolverErrs <- err
		}
	}()

	// tally up the number of bytes written so it can be compared to
	// the file's size to ensure all the file's content are written
	totalBytesWritten := uint64(0)
	chunkCount := uint64(0)

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
			case bytesWritten := <-results:
				chunkCount++
				// tally up the bytes written so it can be checked later
				totalBytesWritten += bytesWritten
				err = w.Notifier.Notify(ctx, &types.ChunkNotification{
					Address: address,
					Chunk:   chunkCount,
				})
				if err != nil {
					slog.Error("unable to notify of chunk completion", "address", address, "chunk", chunkCount, "error", err)
				}
				if chunkCount == numChunks {
					// if the total number of bytes written doesn't equal the size of the file, then something
					// went wrong
					if totalBytesWritten != sz {
						return fmt.Errorf("expected to write '%d; bytes but only wrote '%d' bytes", sz, totalBytesWritten)
					}
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
	defer func(r *io.PipeReader) {
		closeErr := r.Close()
		if closeErr != nil {
			errs <- closeErr
		}
	}(r)
	defer close(results)
	defer close(errs)
	for i := uint64(1); i <= numChunks; i++ {
		err := func() error {
			var copiedBytes uint64
			resolve := func(writer io.Writer) (dir, address string, err error) {
				written, err := io.CopyN(writer, r, int64(w.ChunkSize))
				if err != nil {
					// an End of File error should be considered a critical error if it is
					// returned before the last chunk
					if errors.Is(err, io.EOF) && i == numChunks {
						err = nil
					}
				}
				// record the number of bytes written
				copiedBytes = uint64(written)
				return
			}

			chunkFile := fmt.Sprintf("%08d", i)
			_, _, err := w.Server.Put(ctx, resolve, chunkDir, chunkFile)
			if err != nil {
				return err
			}

			// if no error was encountered, report the number of bytes copied so it can be
			// computed to ensure the download was successful
			results <- copiedBytes
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
	defer func(infoFile io.ReadCloser) {
		closeErr := infoFile.Close()
		if closeErr != nil {
			errors.Join(err, closeErr)
		}
	}(infoFile)

	info := types.ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	errs := make(chan error)
	results := make(chan uint64)
	// tally up the number of bytes written so it can be compared to
	// the file's size to ensure all the file's content are written
	totalBytesRead := uint64(0)
	chunkCount := uint64(0)

	pR, pW := io.Pipe()
	go w.readChunks(ctx, address, chunkDir, info.NumChunks, info.Complete, pW, results, errs)

	// Wait for all results to complete and report errors
	err = func() error {
		for {
			select {
			case err = <-errs:
				if err != nil {
					return err
				}
			case bytesRead := <-results:
				chunkCount++
				totalBytesRead += bytesRead
				if chunkCount == info.NumChunks {
					// if the total number of bytes read doesn't equal the size of the file, then something
					// went wrong
					if info.Complete && totalBytesRead != info.FileSize {
						return fmt.Errorf(
							"filesize reported as '%d' but only '%d' bytes actually read",
							info.FileSize,
							totalBytesRead,
						)
					}
				}
			}
		}
	}()
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	return pR, &info, int64(info.FileSize), info.ModTime, nil
}

func (w *DefaultChunkUtils) readChunks(
	ctx context.Context,
	address string,
	chunkDir string,
	numChunks uint64,
	complete bool,
	writer *io.PipeWriter,
	results chan uint64,
	errs chan error,
) {
	defer func(writer *io.PipeWriter) {
		closeErr := writer.Close()
		if closeErr != nil {
			// close errors shouldn't result in a failed read, instead log the error
			slog.Error("unable to close chunked write", "error", closeErr.Error())
		}
	}(writer)

	for i := uint64(1); i <= numChunks; i++ {
		result, err := w.retryingChunkRead(ctx, i, address, chunkDir, complete, writer)
		if err != nil {
			errs <- err
			err = writer.CloseWithError(err)
			if err != nil {
				errs <- err
			}
			return
		}
		results <- result
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
) (bytesRead uint64, err error) {
	attempts := 0
	for {
		attempts += 1
		var done bool
		done, bytesRead, err = w.tryChunkRead(ctx, attempts, chunkIndex, address, chunkDir, complete, writer)
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
) (bool, uint64, error) {
	chunkFile := fmt.Sprintf("%08d", chunkIndex)
	readBytes := int64(0)

	// Open the chunks sequentially
	chunk, _, _, _, ok, err := w.Server.Get(ctx, chunkDir, chunkFile)
	if err != nil {
		return false, 0, fmt.Errorf("error opening chunk file at %s: %s", chunkDir, err)
	} else if !ok {
		if !complete {
			// If we've waited 5 minutes for this chunk to appear, err to avoid
			// blocking forever
			if attempts > w.MaxAttempts {
				return false, 0, rsstorage.ErrNoChunk
			}
			// Wait for the next chunk, then retry in for loop.
			w.Waiter.WaitForChunk(ctx, &types.ChunkNotification{
				Timeout: w.PollTimeout,
				Address: address,
				Chunk:   chunkIndex,
			})
			return false, 0, nil
		}
		// If already done, return error
		return false, 0, rsstorage.ErrNoChunk
	}

	defer func(chunk io.ReadCloser) {
		closeErr := chunk.Close()
		if closeErr != nil {
			// close errors shouldn't result in a failed read, instead log the error
			slog.Error("unable to close chunked read", "error", closeErr.Error())
		}
	}(chunk)

	// Read the current chunk
	readBytes, err = io.Copy(writer, chunk)
	if err != nil {
		return false, 0, fmt.Errorf("error reading from chunk: %s", err)
	}

	return true, uint64(readBytes), nil
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
