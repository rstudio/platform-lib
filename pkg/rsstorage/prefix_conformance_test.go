package rsstorage_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/postgres"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/s3server"
)

// Every storage server in this module must implement PrefixEnumerator.
//
// Callers reach it by type assertion, which fails silently: a server that
// stopped implementing it would not break a build or fail a test, it would just
// quietly fall back to whatever the caller does without prefix scoping -- for
// PPM, skipping a cleanup, and in general re-listing an entire storage class.
// These assertions turn that into a compile error.
//
// MetadataStorageServer is the case most likely to regress. It embeds the
// StorageServer INTERFACE, and embedding an interface promotes only the methods
// that interface declares, so it needs an explicit forwarding method and does
// not get one for free.
var (
	_ rsstorage.PrefixEnumerator = (*file.StorageServer)(nil)
	_ rsstorage.PrefixEnumerator = (*s3server.StorageServer)(nil)
	_ rsstorage.PrefixEnumerator = (*postgres.StorageServer)(nil)
	_ rsstorage.PrefixEnumerator = (*rsstorage.MetadataStorageServer)(nil)
	_ rsstorage.PrefixEnumerator = (*rsstorage.DummyStorageServer)(nil)
)
