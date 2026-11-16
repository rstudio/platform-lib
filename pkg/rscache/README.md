# `pkg/rscache`

## Description

A persistent storage cache intended to be used with `rsstorage` and `rsqueue`
to create and cache files and objects on disk and in memory.

The cache looks for an item in a storage server. If the item is not there, the
cache pushes work into a queue and waits for a worker to create the item. Then
it reads the item from storage and returns it. If the same item is requested
again, it comes straight from storage, or from memory if you ask for that.

## Examples

- [markdownRenderer](../../examples/cmd/markdownRenderer/README.md)
  demonstrates how to use `rscache` to create and retrieve files.
- [example_test.go](example_test.go) has runnable examples that need no
  database or network.

## Definitions

The important pieces of the cache include:

- **ResolverSpec** Describes the item you want. It holds the `Work` (what to
  queue if the item is missing), the queue `Priority` and `GroupId`, and flags
  such as `CacheInMemory` and `Gzip`.

- **AddressableWork** The `Work` in a `ResolverSpec`. It has an `Address()`
  (the name of the item in storage), a `Dir()` (the directory or prefix in
  storage, may be empty) and a `Type()`. The same value is pushed into the
  queue, so it must be work that your queue runner understands.

- **FileCache** Gets files from the storage server. When a file is missing, it
  uses the queue to create it. Create one with `NewFileCache`.

- **MemoryCache** Keeps items in memory. It is built on a
  [ristretto](https://github.com/dgraph-io/ristretto) cache that you create and
  pass in. Create one with `NewMemoryCache`.

- **MemoryBackedFileCache** Puts a `MemoryCache` in front of a `FileCache`. It
  can also decode a cached gob file into an object. Create one with
  `NewMemoryBackedFileCache`.

- **CacheReturn** The result of a `Get` or `GetObject`. It holds the `Value`,
  an `Err`, the `Size` and `Timestamp` of the item, and `ReturnedFrom` (`file`
  or `memory`). Use `AsReader()` for a file and `AsObject()` for an object.

- **Queue**, **DuplicateMatcher** and **OptionalRecurser** Small interfaces
  that you implement to connect the cache to your queue. See below.

- **CacheProvider** The interface with `Get`, `GetObject`, `Check`, `Head` and
  `Uncache`. `MemoryBackedFileCache` has all of these methods.

## Details

### How a file cache resolves a request

`FileCache.Get` takes these steps:

1. It asks the storage server if the item exists. A chunked item that is not
   complete counts as missing.
2. If the item is there, it opens the item and returns it. `ReturnedFrom` is
   `file`.
3. If not, it pushes `Work` into the queue with `AddressedPush`, using the
   address of the item.
4. It listens with `PollAddress` until the queue reports that the work at that
   address has finished, or until the context is cancelled.
5. If the queue reports an error, `Get` returns it in `CacheReturn.Err`.
6. If the queue reports success, `Get` reads the item from storage again.
   Shared file systems such as NFS can be slow to show a new file, so if the
   item is not found it asks the storage server to `Flush` and tries again. It
   keeps trying every 200 milliseconds until `FileCacheConfig.Timeout` passes.
   If the item never shows up, `Get` returns an error.

`Head` works the same way but returns only the size and modification time.
`Check` only looks in storage. It never pushes work. `Uncache` removes the
item from storage.

The cache does not do the work itself. A worker that you run takes the work
from the queue and writes the item into the same storage server (for example,
with the storage server's `Put` method). See the
[markdownRenderer](../../examples/cmd/markdownRenderer/README.md) for a worker.

### Duplicate work

Many callers may ask for the same missing item at once. The cache pushes the
work with `AddressedPush`, and the queue allows only one queued item for each
address. Your queue returns an error when the address is already queued. The
`DuplicateMatcher` tells the cache which errors mean this. The cache then
ignores the error and waits for the work that is already queued, so the work
runs once. Any other push error is returned to the caller.

### The queue interface

`rscache.Queue` has two methods: `AddressedPush` and `PollAddress`. The
`rsqueue` queue has the same methods, but the markdownRenderer example wraps it
to make it fit the cache. In that example, `DuplicateMatcher` returns true for
`queue.ErrDuplicateAddressedPush`.

### The recurser

`FileCacheConfig.Recurser` runs the push and wait steps. It exists for the case
where a queue job calls the cache and waits for more work from the same queue,
which can deadlock when all workers are busy waiting. The markdownRenderer
example uses `queue.NewOptionalRecurser`. A simple implementation can just call
the function it is given, as the examples do.

The cache calls the queue, the duplicate matcher and the recurser without
checking for `nil`, so set all of them whenever `Get` or `Head` may need to
create an item.

### Memory cache

`MemoryBackedFileCache.Get` and `GetObject` look in memory only when
`ResolverSpec.CacheInMemory` is true and the memory cache is enabled. A memory
cache is enabled when it has a ristretto cache with a `MaxCost` above zero.

- On a memory hit, the result is returned with `ReturnedFrom` set to `memory`.
- On a miss, the file cache is used. If there was no error and the item is
  smaller than `MaxMemoryPerObject`, the item is also put in memory.
- Items expire from memory after `MemoryCacheConfig.TTL`. Ristretto may also
  drop or reject items when it is full.
- The memory cache is keyed by address only. It is not tied to the files on
  disk, so changing or removing a file does not change what is in memory. Use
  `MemoryBackedFileCache.Uncache` to remove an item from both.

Ristretto applies writes in the background, so an item may not be readable
from memory right after the first `Get`. Call `Wait()` on the ristretto cache
if you need to be sure, as the examples do.

### Files and objects

`Get` returns the file contents. `Value` is an `io.ReadCloser`, or a `[]byte`
when the item was put in memory. `AsReader()` handles both. Close the reader
when you are done.

`GetObject` expects the cached file to be a [gob](https://pkg.go.dev/encoding/gob)
file. Pass a pointer to an empty value of the type you want as the type
example, and `AsObject()` returns it filled in. Set `ResolverSpec.Gzip` if the
cached gob file is gzip-compressed. When an object is put in memory, the memory
cache gob-encodes it to measure its size, so register the type with
`gob.Register`. If the type is not registered, the object is not kept in
memory. `GetObject` still returns it from storage, and the error is logged at
debug level.

## Usage

Create a file cache from your queue and storage server:

```go
fc := rscache.NewFileCache(rscache.FileCacheConfig{
	Queue:            myQueue,          // implements rscache.Queue
	DuplicateMatcher: myDupMatcher,     // implements rscache.DuplicateMatcher
	StorageServer:    storageServer,    // an rsstorage.StorageServer
	Recurser:         myRecurser,       // implements rscache.OptionalRecurser
	Timeout:          30 * time.Second, // wait for storage after the queue is done
})
```

Optionally, add a memory cache in front of it:

```go
rc, err := ristretto.NewCache(&ristretto.Config{
	NumCounters: 1000,
	MaxCost:     1 << 20,
	BufferItems: 64,
})
if err != nil {
	return err
}
mc := rscache.NewMemoryCache(rscache.MemoryCacheConfig{TTL: time.Hour, Ristretto: rc})

cache := rscache.NewMemoryBackedFileCache(rscache.MemoryBackedFileCacheConfig{
	FileCache:          fc,
	MemoryCache:        mc,
	MaxMemoryPerObject: 1 << 16, // items this size or larger stay out of memory
})
```

Ask for an item. Your work type implements `rscache.AddressableWork`:

```go
spec := rscache.ResolverSpec{
	CacheInMemory: true,
	Work:          myWork,
}

result := cache.Get(ctx, spec)
reader, err := result.AsReader()
if err != nil {
	return err
}
defer reader.Close()
```

To read a gob file as an object:

```go
result := cache.GetObject(ctx, spec, &MyType{})
obj, err := result.AsObject()
if err != nil {
	return err
}
value := obj.(*MyType)
```

The [examples](example_test.go) run all of these end to end, with a small
in-process queue in place of a real one.

## Testing helpers

- The [test](test) package has `TempDirHelper`, which creates and removes
  temporary directories, and `TimeEquals`, a gocheck checker that compares
  times by whole seconds.
- `filetest.go` in this package has test doubles for code that uses the cache:
  `FakeMemoryCache` and `DummyMemoryBackedFileCache`.

## Tests

Run the tests for this package with:

```bash
just test ./pkg/rscache/...
```

The tests include:

- Unit tests in this package. They use fake queues and a dummy storage server.
- Tests in [internal/integration_test](internal/integration_test/README.md).
  They run the cache against the real file storage server from
  `pkg/rsstorage/servers/file`, including a memory cache with size limits and
  gob files with and without gzip. They live in an `internal` package so that
  applications that import `rscache` do not also pull in the storage
  implementations.
- Tests for the `test` package.

None of these tests need Postgres, S3, Docker, or a network connection.

`just test-integration` runs the tests inside Docker Compose, with a Postgres
database and a MinIO (S3-compatible) server from
`docker/docker-compose.test.yml`. The `rscache` tests do not use those
services, so you do not need it for this package. It is for packages that do.
To run only this package that way, use:

```bash
just test-integration ./pkg/rscache/...
```
