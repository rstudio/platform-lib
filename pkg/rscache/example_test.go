package rscache_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dgraph-io/ristretto"

	"github.com/rstudio/platform-lib/v4/pkg/rscache"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/file"
)

// textWork describes one item to cache. It is both the work that is pushed
// into the queue and the description of where the result is stored.
type textWork struct {
	name  string
	write func(w io.Writer) error
}

func (w *textWork) Address() string { return w.name }
func (w *textWork) Dir() string     { return "" }
func (w *textWork) Type() uint64    { return 1 }

// inlineQueue is a stand-in for a real work queue. Instead of handing the
// work to a separate worker, it does the work right away and stores the
// result in the storage server.
type inlineQueue struct {
	server rsstorage.StorageServer
	pushes int
}

func (q *inlineQueue) AddressedPush(ctx context.Context, priority uint64, groupId int64, address string, work rscache.QueueWork) error {
	q.pushes++
	w := work.(*textWork)
	_, _, err := q.server.Put(ctx, func(out io.Writer) (string, string, error) {
		return "", "", w.write(out)
	}, w.Dir(), w.Address())
	return err
}

// PollAddress returns a channel that reports the work is done. A closed
// channel yields a nil error, which means success.
func (q *inlineQueue) PollAddress(ctx context.Context, address string) <-chan error {
	done := make(chan error)
	close(done)
	return done
}

var errDuplicate = errors.New("duplicate address")

// dupMatcher tells the cache which queue error means "the work is already
// queued".
type dupMatcher struct{}

func (dupMatcher) IsDuplicate(err error) bool { return errors.Is(err, errDuplicate) }

// directRecurser runs the function in the calling goroutine.
type directRecurser struct{}

func (directRecurser) OptionallyRecurse(ctx context.Context, run func()) { run() }

// newFileCache builds a file cache on a storage server in a new temporary
// directory. The returned function removes the directory.
func newFileCache() (rscache.FileCache, *inlineQueue, func(), error) {
	dir, err := os.MkdirTemp("", "rscache-example-")
	if err != nil {
		return nil, nil, nil, err
	}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:          dir,
		ChunkSize:    4096,
		Class:        "example",
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})
	queue := &inlineQueue{server: server}
	fc := rscache.NewFileCache(rscache.FileCacheConfig{
		Queue:            queue,
		DuplicateMatcher: dupMatcher{},
		StorageServer:    server,
		Recurser:         directRecurser{},
		Timeout:          5 * time.Second,
	})
	return fc, queue, func() { _ = os.RemoveAll(dir) }, nil
}

func readAll(result rscache.CacheReturn) (string, error) {
	reader, err := result.AsReader()
	if err != nil {
		return "", err
	}
	defer reader.Close()
	b, err := io.ReadAll(reader)
	return string(b), err
}

// The first Get does not find the item in storage, so it pushes work into
// the queue and waits for the work to finish. The second Get finds the item
// in storage and does not push any work.
func ExampleNewFileCache() {
	fc, queue, cleanup, err := newFileCache()
	if err != nil {
		fmt.Println("setup error:", err)
		return
	}
	defer cleanup()

	spec := rscache.ResolverSpec{
		Work: &textWork{
			name: "greeting.txt",
			write: func(w io.Writer) error {
				_, err := io.WriteString(w, "hello, cache")
				return err
			},
		},
	}

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		result := fc.Get(ctx, spec)
		text, err := readAll(*result)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("read %d: %s (from %s)\n", i, text, result.ReturnedFrom)
	}
	fmt.Println("work pushed:", queue.pushes)

	ok, err := fc.Check(ctx, spec)
	fmt.Println("cached:", ok, err)

	// Uncache removes the item from storage.
	if err := fc.Uncache(ctx, spec); err != nil {
		fmt.Println("error:", err)
		return
	}
	ok, err = fc.Check(ctx, spec)
	fmt.Println("cached after uncache:", ok, err)

	// Output:
	// read 1: hello, cache (from file)
	// read 2: hello, cache (from file)
	// work pushed: 1
	// cached: true <nil>
	// cached after uncache: false <nil>
}

// A memory-backed file cache puts a memory cache in front of a file cache.
// Items are kept in memory only when the ResolverSpec sets CacheInMemory.
func ExampleNewMemoryBackedFileCache() {
	fc, queue, cleanup, err := newFileCache()
	if err != nil {
		fmt.Println("setup error:", err)
		return
	}
	defer cleanup()

	// The memory cache is a ristretto cache that you create and own.
	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	if err != nil {
		fmt.Println("setup error:", err)
		return
	}
	defer rc.Close()
	mc := rscache.NewMemoryCache(rscache.MemoryCacheConfig{TTL: time.Hour, Ristretto: rc})

	mbfc := rscache.NewMemoryBackedFileCache(rscache.MemoryBackedFileCacheConfig{
		FileCache:          fc,
		MemoryCache:        mc,
		MaxMemoryPerObject: 1 << 16,
	})

	spec := rscache.ResolverSpec{
		CacheInMemory: true,
		Work: &textWork{
			name: "greeting.txt",
			write: func(w io.Writer) error {
				_, err := io.WriteString(w, "hello, memory")
				return err
			},
		},
	}

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		result := mbfc.Get(ctx, spec)
		text, err := readAll(result)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("read %d: %s (from %s)\n", i, text, result.ReturnedFrom)

		// ristretto applies writes in the background. Wait so that the
		// second read is sure to see the first one.
		rc.Wait()
	}
	fmt.Println("work pushed:", queue.pushes)

	// Output:
	// read 1: hello, memory (from file)
	// read 2: hello, memory (from memory)
	// work pushed: 1
}

type person struct {
	Name string
	Age  int
}

// GetObject reads a cached gob file and decodes it into the value that you
// pass as the type example.
func ExampleMemoryBackedFileCache_GetObject() {
	fc, _, cleanup, err := newFileCache()
	if err != nil {
		fmt.Println("setup error:", err)
		return
	}
	defer cleanup()

	rc, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	if err != nil {
		fmt.Println("setup error:", err)
		return
	}
	defer rc.Close()

	mbfc := rscache.NewMemoryBackedFileCache(rscache.MemoryBackedFileCacheConfig{
		FileCache:          fc,
		MemoryCache:        rscache.NewMemoryCache(rscache.MemoryCacheConfig{TTL: time.Hour, Ristretto: rc}),
		MaxMemoryPerObject: 1 << 16,
	})

	// The memory cache gob-encodes an object to measure its size. Gob needs
	// the type to be registered for that.
	gob.Register(&person{})

	spec := rscache.ResolverSpec{
		CacheInMemory: true,
		Work: &textWork{
			name: "ada.gob",
			write: func(w io.Writer) error {
				return gob.NewEncoder(w).Encode(&person{Name: "Ada", Age: 36})
			},
		},
	}

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		result := mbfc.GetObject(ctx, spec, &person{})
		obj, err := result.AsObject()
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		p := obj.(*person)
		fmt.Printf("read %d: %s, %d (from %s)\n", i, p.Name, p.Age, result.ReturnedFrom)
		rc.Wait()
	}

	// Output:
	// read 1: Ada, 36 (from file)
	// read 2: Ada, 36 (from memory)
}
