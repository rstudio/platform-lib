# `/pkg/rsstorage`

## Description

A storage library. Useful for applications that need to store and
retrieve data from centralized locations like NFS or AWS S3.

The package defines one interface, `StorageServer`. Your code talks to the
interface, and you pick the implementation: a file system, an S3 bucket, or a
PostgreSQL database. Large items can be stored in chunks, so a reader can start
before the writer has finished.

## Examples

- [markdownRenderer](../../examples/cmd/markdownRenderer/README.md)
  demonstrates how to use `rsstorage` to create and retrieve files.
- The `example_test.go` files in this package and in
  [servers/file](servers/file/example_test.go) are runnable examples. They use a
  temporary directory and need no database or network. See them with
  `go doc` or run them with `go test -run Example`.

## Implementations

| Implementation | Package | Items are stored in | Use it when |
| --- | --- | --- | --- |
| File | `servers/file` | Files in a directory | You have a local disk or a shared file system such as NFS. |
| S3 | `servers/s3server` | Objects in an S3 bucket | You want object storage, or any service that speaks the S3 API. Supports client-side encryption. |
| PostgreSQL | `servers/postgres` | PostgreSQL large objects | You want to keep items in the database you already run. Experimental. |

See [servers](servers/README.md) for how to build each one.

## Definitions

- **Directory and address.** Every item is identified by two strings: a
  directory (`dir`) and an address. The directory can be empty. It can also
  contain slashes, as in `reports/2026`. Each server maps the pair to its own
  storage: a file path, an S3 key, or a database row.
- **Resolver.** A function of type `types.Resolver` that writes the item's data
  to the `io.Writer` it is given. `Put` calls it for you. It can also return a
  directory and an address.
- **Chunked item.** An item written with `PutChunked`. The data is split into
  chunks of a fixed size. See _Chunked storage_ below.
- **Key.** A directory and an address joined with `/`, or just the address if
  the directory is empty. `ItemKey` builds it. Prefix listing matches against
  keys.
- **Class.** A name in the arguments of the file and PostgreSQL servers. The
  PostgreSQL server uses it as a prefix on every stored address, so several
  servers can share one table. The file server accepts it and does not use it.

## The `StorageServer` interface

| Group | Methods | What they do |
| --- | --- | --- |
| Read | `Check`, `Get` | `Check` says whether an item exists and returns its size and modification time without opening it. `Get` also returns a reader. Both return `found` as `false`, and no error, for a missing item. |
| Write | `Put`, `PutChunked` | `Put` stores one item. `PutChunked` stores it in chunks. Both call a `Resolver` to get the data. |
| Remove | `Remove` | Deletes an item, including all the chunks of a chunked item. Removing an item that is not there is not an error. |
| List | `Enumerate` | Returns every item the server holds. A chunked item appears once, with `Chunked` set to `true`. |
| Transfer | `Copy`, `Move` | Copy or move an item to another `StorageServer`. See _Copy and move_ below. |
| Describe | `Dir`, `Type`, `Locate`, `Base` | `Dir` and `Type` identify the server. `Locate` returns where an item lives, in the server's own terms. `Base` returns the underlying server when a server is wrapped. |
| Capacity | `CalculateUsage` | Reports size, free space, and used space. Only the file server implements it. The others return an error. |
| NFS | `Flush` | Asks a shared file system to refresh its view of an item. Only the file server does anything. The others do nothing. |

`Check` and `Get` return a `*types.ChunksInfo` that is `nil` for a regular item.
Both also return the size and modification time. The PostgreSQL server does not
return a modification time for regular items; it returns the zero time.

### Put and Resolver

`Put(ctx, resolve, dir, address)` calls `resolve` with a writer. Write the data
to it. If you pass an empty `dir` and an empty `address`, `Put` uses the
directory and address that `resolve` returns. This is useful when the location
depends on the content. If you pass either one, `Put` uses yours and ignores
what `resolve` returns. `Put` returns the directory and address it used.

If `resolve` returns an error, nothing is stored.

### Chunked storage

`PutChunked(ctx, resolve, dir, address, size)` needs a non-empty address and a
non-zero `size` up front. It splits the data into chunks of the server's
`ChunkSize`. The item is stored under a directory named `address`. It holds:

- `info.json`, a `types.ChunksInfo`: the chunk size, the file size, the number
  of chunks, the modification time, and a `complete` flag.
- One file per chunk, named `00000001`, `00000002`, and so on.

`info.json` is written first with `complete` set to `false`. It is rewritten
with `complete` set to `true` after the last chunk. Any old item at the same
location is removed first. If the write fails, for example because `resolve`
returns an error or writes fewer than `size` bytes, the partly written item is
removed.

A server that supports chunks needs two helpers, both defined in this package:

- A `ChunkNotifier`. The writer calls `Notify` after each chunk is written.
- A `ChunkWaiter`. A reader calls `WaitForChunk` when the next chunk is not
  there yet and the item is not complete. It passes a 5 second timeout. After
  100 tries the reader gives up with `ErrNoChunk`.

You decide how the two talk to each other. In one process, a channel is enough.
Across several nodes, they might use a database or a message system. The
[markdownRenderer](../../examples/cmd/markdownRenderer/README.md) example has a
complete pair. If you only write and read chunked items one after the other, a
notifier that does nothing and a waiter that returns at once are enough.

`Get` on a chunked item returns a reader that streams the chunks in order. If
the item is still being written, the reader waits for the next chunk as above.
`ErrNoChunkMetadata` is returned when the directory has no `info.json`.

### Copy and move

`Copy` and `Move` take the destination server as an argument.

- Between two servers of the same type, `Move` and `Copy` can take a faster
  path. For `Move`, the file server renames the file and the PostgreSQL server
  rewrites the address in its table. For `Copy`, the S3 server asks S3 to copy
  the object, so the bytes do not pass through your process. If the faster path
  fails, the file and S3 servers fall back to the next case.
- Otherwise, the data is read from the source and written to the destination
  with `Put` or `PutChunked`. This is the case for servers of different types,
  and when the destination is wrapped in `MetadataStorageServer`. The wrapper's
  `Base()` is used to reach the real destination.
- Where there is no faster path, `Move` is a copy followed by a remove of the
  source. The S3 server always does this.

### Prefix listing

`PrefixEnumerator` is an optional interface with one method,
`EnumeratePrefix(ctx, prefix)`. It lists only the items whose key starts with
`prefix`, so a caller does not have to list everything. All servers in this
module implement it, and so does `MetadataStorageServer`. It is a separate
interface so that adding it did not change `StorageServer`. Use a type
assertion:

```go
if pe, ok := server.(rsstorage.PrefixEnumerator); ok {
	items, err := pe.EnumeratePrefix(ctx, "GIT_")
	// ...
}
```

A prefix is matched against the whole key as a string. It does not have to end
at a `/`: the prefix `GIT_` matches the key `GIT_1.gob`, and the prefix `a/b`
matches both `a/bc` and `a/b/c`. An empty prefix lists everything. The
`Dir` of each returned item is relative to the root of the server, so you can
pass the item straight to `Get`, `Check`, or `Remove`.

If a server cannot list by prefix, it returns `ErrPrefixEnumerationUnsupported`.
Do not fall back to `Enumerate` in that case, because that is the cost the
prefix was meant to avoid.

`ItemKey`, `KeyHasPrefix`, and `SubtreeMayMatch` are helpers for writing your
own implementation.

### MetadataStorageServer

`NewMetadataStorageServer` wraps any `StorageServer` and records activity in a
`CacheStore` that you provide:

- After a successful `Put` or `PutChunked`, it calls
  `CacheObjectEnsureExists(name, dir + "/" + address)`.
- After a successful `Get`, it calls `CacheObjectMarkUse(name, dir + "/" + address, now)`.

If the store returns an error, the call returns that error. Everything else is
passed to the wrapped server. Use `Base()` to get the wrapped server back.

### Other types

- `Config`, `ConfigFile`, and `ConfigS3` are plain structs for holding settings.
  The constructors in `servers/` do not take them. Your code reads them and
  passes the values on.
- `DummyStorageServer` is a fake that you can use in your own tests.

## Using a server

This builds a file server, writes an item, reads it back, and removes it. The
file server needs a directory that already exists.

```go
server := file.NewStorageServer(file.StorageServerArgs{
	Dir:       "/var/lib/myapp/storage",
	ChunkSize: 1 << 20, // only needed for PutChunked
	Waiter:    myWaiter,   // only needed for chunked items
	Notifier:  myNotifier, // only needed for chunked items
})

// Write.
resolve := func(w io.Writer) (string, string, error) {
	_, err := io.WriteString(w, "hello")
	return "", "", err
}
dir, address, err := server.Put(ctx, resolve, "docs", "hello.txt")

// Check without reading.
found, _, size, modTime, err := server.Check(ctx, "docs", "hello.txt")

// Read. Always close the reader.
r, _, _, _, found, err := server.Get(ctx, "docs", "hello.txt")
if err == nil && found {
	defer r.Close()
	body, err := io.ReadAll(r)
	// ...
}

// Remove.
err = server.Remove(ctx, "docs", "hello.txt")
```

To track use, wrap the server:

```go
server = rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
	Name:   "docs",
	Server: server,
	Store:  myCacheStore,
})
```

## Tests

Run the unit tests for this package and the servers with:

```bash
just test ./pkg/rsstorage/...
```

`just test` passes `-short`. With `-short`, the suites that need a PostgreSQL
database or an S3 service are skipped. Everything else runs: the tests for this
package, the file server tests, the S3 server tests (they use a fake S3 client),
and the examples.

The integration tests need a PostgreSQL database and an S3-compatible service.
Run them with Docker:

```bash
just test-integration ./pkg/rsstorage/...
```

`just test-integration` uses `docker/docker-compose.test.yml` to start a
PostgreSQL 16 container and a MinIO container (an S3-compatible service). It runs
the tests in a Go container linked to them, without `-short`, and removes the
containers afterward. It leaves a log file named `test-integration-<id>.log` in
the repository root. Each PostgreSQL test creates its own empty database.

The tests in
[internal/integration_test](internal/integration_test/README.md) run the same
checks against the file, S3, and PostgreSQL servers. They cover chunked reads
and writes, moving, copying, and prefix listing. See that README to run only
those tests.
