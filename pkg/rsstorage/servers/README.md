# `/pkg/rsstorage/servers`

This directory contains implementations of `rsstorage.StorageServer`. See the
[rsstorage README](../README.md) for what the interface does and for the
concepts used below.

| Package | Constructor | Stores items in | Details |
| --- | --- | --- | --- |
| `file` | `file.NewStorageServer` | A directory on a local or shared file system | [file](file/README.md) |
| `s3server` | `s3server.NewStorageServer` | An S3 bucket | [s3server](s3server/README.md) |
| `postgres` | `postgres.NewStorageServer` | PostgreSQL large objects (experimental) | [postgres](postgres/README.md) |

All three packages are part of the one Go module, `github.com/rstudio/platform-lib/v4`.
Import only the package you need. Your program links only the packages it
imports, so a program that uses the file server does not link the AWS or `pgx`
libraries.

Each constructor takes a `StorageServerArgs` struct and returns an
`rsstorage.StorageServer`.

## Arguments they share

| Argument | Meaning |
| --- | --- |
| `ChunkSize` | The size of each chunk, in bytes, for `PutChunked`. Required for chunked storage. `PutChunked` panics with a divide by zero if it is left at 0. |
| `Waiter` | An `rsstorage.ChunkWaiter`. Called by readers that wait for the next chunk of an item that is still being written. |
| `Notifier` | An `rsstorage.ChunkNotifier`. Called by the writer after each chunk. A chunked write calls it, so it must not be `nil` if you use `PutChunked`. |

You can leave `ChunkSize`, `Waiter`, and `Notifier` unset if you never call
`PutChunked`.

## How they differ

| | `file` | `s3server` | `postgres` |
| --- | --- | --- | --- |
| `Type()` | `file` | `s3` | `postgres` |
| `Dir()` | The directory | `s3:` and the bucket name | `pg:` and the class |
| `Locate(dir, address)` | The full file path | `s3://<bucket>/<prefix>/<dir>/<address>` | `<class>/<dir>/<address>` |
| `CalculateUsage` | Works (not on Windows) | Returns an error | Returns an error |
| `Flush` | Re-reads file attributes for NFS | Does nothing | Does nothing |
| Modification time of a regular item | Yes | Yes | No (zero time) |
| `EnumeratePrefix` | Skips directories that cannot match | Sends the prefix to S3 | Sends the prefix to the database |

Every server implements `rsstorage.PrefixEnumerator`.
