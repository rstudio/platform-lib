// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package rsstorage defines a common interface for storing and retrieving
// named items, and the helpers that the implementations share.
//
// An item is identified by a directory and an address. Both are plain strings.
// The directory may be empty. The StorageServer interface covers writing
// (Put and PutChunked), reading (Get and Check), removing, listing (Enumerate),
// and copying or moving items between servers.
//
// The implementations live in subpackages: servers/file for a file system,
// servers/s3server for Amazon S3, and servers/postgres for PostgreSQL large
// objects. Import only the ones you need.
//
// Large items can be stored as a series of fixed-size chunks with PutChunked.
// A reader can start reading a chunked item before the writer has finished it.
// The ChunkWaiter and ChunkNotifier interfaces connect the writer to the
// readers.
//
// MetadataStorageServer wraps a server and records writes and reads in a
// CacheStore. PrefixEnumerator is an optional interface for listing only the
// items whose key starts with a given prefix.
package rsstorage
