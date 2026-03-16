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
		err = fmt.Errorf("unable to clear directory before writing: %w", err)
		return
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
	defer func() {
		if err != nil {
			removeErr := w.Server.Remove(ctx, dir, address)
			if removeErr != nil {
				err = errors.Join(err, removeErr)
			}
		}
	}()

	// Write all chunks
	results := make(chan uint64)
	errs := make(chan error)
	go w.writeChunks(ctx, numChunks, chunkDir, pR, results, errs)

	// Resolve/get the data we need
	resolverErrs := make(chan error)
	go func() {
		defer close(resolverErrs)
		defer func(pW *io.PipeWriter) {
			closeErr := pW.Close()
			if closeErr != nil {
				resolverErrs <- closeErr
			}
		}(pW)
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
						return fmt.Errorf("expected to write '%d' bytes but only wrote '%d' bytes", sz, totalBytesWritten)
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
		err := r.Close()
		if err != nil {
			errs <- err
		}
		close(results)
		close(errs)
	}(r)
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
						copiedBytes = uint64(written)
						return
					}
					return
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
) (rc io.ReadCloser, chunksInfo *types.ChunksInfo, size int64, modTime time.Time, err error) {
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
			err = errors.Join(err, closeErr)
		}
	}(infoFile)

	info := types.ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	pR, pW := io.Pipe()

	err = w.readChunks(ctx, address, chunkDir, info.NumChunks, info.Complete, info.FileSize, pW)
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	return pR, &info, int64(info.FileSize), info.ModTime, err
}

func (w *DefaultChunkUtils) readChunks(
	ctx context.Context,
	address string,
	chunkDir string,
	numChunks uint64,
	complete bool,
	fileSize uint64,
	writer *io.PipeWriter,
) error {
	var err error
	bytesRead := uint64(0)

	defer func(writer *io.PipeWriter) {
		closeErr := writer.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}(writer)

	totalBytesWritten := uint64(0)

	for i := uint64(0); i <= numChunks; i++ {
		bytesRead, err = w.retryingChunkRead(ctx, i, address, chunkDir, complete, writer)
		if err != nil {
			closeErr := writer.CloseWithError(err)
			if closeErr != nil {
				return errors.Join(err, closeErr)
			}
			return err
		}
		totalBytesWritten += bytesRead
	}

	if totalBytesWritten != fileSize {
		err = fmt.Errorf("expected to read '%d' bytes from file but only read '%d' bytes", fileSize, totalBytesWritten)
		return err
	}

	return nil
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
) (done bool, bytesRead uint64, err error) {
	chunkFile := fmt.Sprintf("%08d", chunkIndex)

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
			err = errors.Join(err, closeErr)
		}
	}(chunk)

	// Read the current chunk
	bytesCopied, copyErr := io.Copy(writer, chunk)
	if copyErr != nil {
		err = fmt.Errorf("error reading from chunk: %s", copyErr)
		return
	}
	bytesRead = uint64(bytesCopied)
	done = true
	return
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
