// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package postgres provides an rsstorage.StorageServer that keeps items as
// PostgreSQL large objects.
//
// The server needs a large_objects table that maps each address to the object
// identifier of its large object. The table is not created by this package.
// The server is experimental.
package postgres
