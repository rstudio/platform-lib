// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package file provides an rsstorage.StorageServer that keeps items in a
// directory on a local or shared file system.
//
// An item is the file at <Dir>/<dir>/<address>. A chunked item is a directory
// of that name that holds an info.json file and one file per chunk. Put
// writes the data to a staging file inside Dir and then renames it into place,
// so a reader never sees a partly written regular item.
package file
