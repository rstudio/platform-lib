// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package rscache caches files and objects.
//
// A cache looks for an item in a storage server (see package rsstorage). If
// the item is missing, the cache pushes work into a queue (see package
// rsqueue) and waits for a worker to create the item. It then reads the item
// from storage and returns it. Items are identified by an address.
//
// The package has three parts:
//
//   - FileCache gets files from the storage server and makes them when they
//     are missing. Create one with NewFileCache.
//   - MemoryCache keeps items in memory. Create one with NewMemoryCache.
//   - MemoryBackedFileCache puts a MemoryCache in front of a FileCache. It
//     can also decode cached gob files into objects. Create one with
//     NewMemoryBackedFileCache.
//
// Describe the item you want with a ResolverSpec and pass it to Get or
// GetObject. Both return a CacheReturn.
//
// See the package README for more details and a walk-through.
package rscache
