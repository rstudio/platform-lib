# `/pkg/rsstorage/servers`

This directory contains implementations of
`/pkg/rsstorage/server.PersistentStorageServer`.

Each implementation should have its own `go.mod`. This allows consumers to import
only the storage server types they need for their use cases.
