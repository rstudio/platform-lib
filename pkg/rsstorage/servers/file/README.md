# `/pkg/rsstorage/servers/file`

## Description

A storage server implementation for file-based storage. Suitable
for shared or non-shared file systems.

An item is the file at `<Dir>/<dir>/<address>`. A chunked item is a directory
at that path. It holds `info.json` and one file per chunk (`00000001`,
`00000002`, and so on).

## Constructing a server

```go
server := file.NewStorageServer(file.StorageServerArgs{
	Dir:          "/var/lib/myapp/storage",
	ChunkSize:    1 << 20,
	Waiter:       myWaiter,
	Notifier:     myNotifier,
	CacheTimeout: time.Minute,
	WalkTimeout:  30 * time.Second,
})
```

| Argument | Meaning |
| --- | --- |
| `Dir` | The root directory. It must already exist. If it does not, `Put` fails. |
| `ChunkSize`, `Waiter`, `Notifier` | For chunked storage. See [servers](../README.md). |
| `Class` | Accepted so that the arguments match the other servers. The file server does not use it. |
| `CacheTimeout` | The longest time that `CalculateUsage` spends adding up file sizes. If it is 0, `CalculateUsage` fails at once with a `cacheTimeout reached` error. |
| `WalkTimeout` | The longest gap between two files while walking the directory, in `Enumerate`, `EnumeratePrefix`, and `CalculateUsage`. The default is 5 minutes. It limits the time between files, not the whole walk. In `Enumerate` and `EnumeratePrefix` the timer restarts only when an item is returned, so files that do not match the prefix do not restart it. |

See [example_test.go](example_test.go) for runnable examples.

## Behavior

- `Put` writes to a staging file in the root of `Dir`, then renames it into
  place. A reader never sees a partly written regular item. Directories are
  created with mode `0700` as needed.
- Because the staging file lives in the root of `Dir`, an `Enumerate` that runs
  during a `Put` can list it.
- `Put` replaces an existing item at the same location.
- `Remove` of a chunked item removes its whole directory.
- `Move` to another file server renames the file. The rename needs both
  servers to be on the same file system. If it fails, `Move` copies the item
  and then removes the source.
- `Flush` changes the owner of the file to the current user and group, and
  reads its attributes. This makes an NFS client refresh what it knows about
  the file. Errors are logged at debug level and ignored.
- `CalculateUsage` reports the capacity and free space of the file system that
  holds `Dir`, and the total size of the files in `Dir`. It is not supported on
  Windows and returns an error there.
- `Enumerate` and `EnumeratePrefix` return each chunked item once, with
  `Chunked` set. `EnumeratePrefix` skips directories that cannot hold a match.
  A prefix without a `/` reads the root directory and any top-level directory
  whose name starts with the prefix.
- The `Dir` of a listed item is relative to the root of the server.
