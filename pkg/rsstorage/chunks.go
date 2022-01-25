package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
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
	WriteChunked(dir, address string, sz uint64, resolve Resolver) error
	ReadChunked(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, error)
}

type ChunkWaiter interface {
	WaitForChunk(c *types.ChunkNotification)
}

type ChunkNotifier interface {
	Notify(c *types.ChunkNotification) error
}

type ChunksInfo struct {
	ChunkSize uint64    `json:"chunk_size"`
	FileSize  uint64    `json:"file_size"`
	ModTime   time.Time `json:"mod_time"`
	NumChunks uint64    `json:"num_chunks"`
	Complete  bool      `json:"complete"`
}

type DefaultChunkUtils struct {
	ChunkSize   uint64
	Server      PersistentStorageServer
	Waiter      ChunkWaiter
	Notifier    ChunkNotifier
	PollTimeout time.Duration
	MaxAttempts int
}

func (w *DefaultChunkUtils) WriteChunked(dir, address string, sz uint64, resolve Resolver) (err error) {

	// Determine number of chunks we will need to create
	numChunks := sz / w.ChunkSize
	if sz%w.ChunkSize > 0 {
		numChunks++
	}

	// Clear the directory if it already exists
	err = w.Server.Remove(dir, address)
	if err != nil {
		return nil
	}

	// Write an `info.json` to the directory
	info := ChunksInfo{
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
	_, _, err = w.Server.Put(infoResolver, chunkDir, "info.json")
	if err != nil {
		return
	}

	// Pipe results from resolver (pW) to chunk writer (pR)
	pR, pW := io.Pipe()

	// Clean up on error
	defer func(err *error) {
		if *err != nil {
			w.Server.Remove(dir, address)
		}
	}(&err)

	// Write all chunks
	results := make(chan uint64)
	errs := make(chan error)
	go w.writeChunks(numChunks, chunkDir, pR, results, errs)

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
	_, _, err = w.Server.Put(infoResolver, chunkDir, "info.json")
	if err != nil {
		return
	}

	return
}

func (w *DefaultChunkUtils) writeChunks(numChunks uint64, chunkDir string, r *io.PipeReader, results chan uint64, errs chan error) {
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
			_, _, err := w.Server.Put(resolve, chunkDir, chunkFile)
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

func (w *DefaultChunkUtils) ReadChunked(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, error) {
	chunkDir := filepath.Join(dir, address)

	infoFile, _, _, _, ok, err := w.Server.Get(chunkDir, "info.json")
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	} else if !ok {
		return nil, nil, 0, time.Time{}, ErrNoChunkMetadata
	}
	defer infoFile.Close()

	info := ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	if err != nil {
		return nil, nil, 0, time.Time{}, err
	}

	pR, pW := io.Pipe()
	go w.readChunks(address, chunkDir, info.NumChunks, info.Complete, pW)

	return pR, &info, int64(info.FileSize), info.ModTime, nil
}

func (w *DefaultChunkUtils) readChunks(address, chunkDir string, numChunks uint64, complete bool, writer *io.PipeWriter) {
	defer writer.Close()

	for i := uint64(1); i <= numChunks; i++ {
		err := w.retryingChunkRead(i, address, chunkDir, complete, writer)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
	}

	return
}

func (w *DefaultChunkUtils) retryingChunkRead(chunkIndex uint64, address, chunkDir string, complete bool, writer *io.PipeWriter) (err error) {
	attempts := 0
	for {
		attempts += 1
		var done bool
		done, err = w.tryChunkRead(attempts, chunkIndex, address, chunkDir, complete, writer)
		if err != nil || done {
			return
		}
	}
}

func (w *DefaultChunkUtils) tryChunkRead(attempts int, chunkIndex uint64, address, chunkDir string, complete bool, writer *io.PipeWriter) (bool, error) {
	chunkFile := fmt.Sprintf("%08d", chunkIndex)

	// Open the chunks sequentially
	chunk, _, _, _, ok, err := w.Server.Get(chunkDir, chunkFile)
	if err != nil {
		return false, fmt.Errorf("error opening chunk file at %s: %s", chunkDir, err)
	} else if !ok {
		if !complete {
			// If we've waited 5 minutes for this chunk to appear, err to avoid
			// blocking forever
			if attempts > w.MaxAttempts {
				return false, ErrNoChunk
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
			return false, ErrNoChunk
		}
	}
	defer chunk.Close()

	// Read the current chunk
	_, err = io.Copy(writer, chunk)
	if err != nil {
		return false, fmt.Errorf("error reading from chunk: %s", err)
	}

	return true, nil
}

func FilterChunks(input []PersistentStorageItem) []PersistentStorageItem {
	output := make([]PersistentStorageItem, 0)
	chunkDirs := make(map[string]bool)

	// Find all directories that are chunked and append one
	// result for each chunked directory.
	for _, i := range input {
		if i.Dir != "" && i.Address == "info.json" {
			chunkDirs[i.Dir] = true
			d, f := filepath.Split(i.Dir)
			output = append(output, PersistentStorageItem{
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
