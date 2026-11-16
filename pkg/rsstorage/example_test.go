package rsstorage_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/file"
)

// recordingStore is a CacheStore that remembers what it was asked to record.
type recordingStore struct {
	calls []string
}

func (s *recordingStore) CacheObjectEnsureExists(cacheName, key string) error {
	s.calls = append(s.calls, fmt.Sprintf("ensure %s %s", cacheName, key))
	return nil
}

func (s *recordingStore) CacheObjectMarkUse(cacheName, key string, accessTime time.Time) error {
	s.calls = append(s.calls, fmt.Sprintf("use %s %s", cacheName, key))
	return nil
}

func ExampleNewMetadataStorageServer() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "rsstorage-example-")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	store := &recordingStore{}
	server := rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
		Name:   "docs-cache",
		Server: file.NewStorageServer(file.StorageServerArgs{Dir: dir}),
		Store:  store,
	})

	// Put records that the item exists.
	resolve := func(w io.Writer) (string, string, error) {
		_, err := io.WriteString(w, "cached")
		return "", "", err
	}
	if _, _, err := server.Put(ctx, resolve, "docs", "a.txt"); err != nil {
		panic(err)
	}

	// Get records that the item was used.
	r, _, _, _, found, err := server.Get(ctx, "docs", "a.txt")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("docs/a.txt was not found")
	}
	_ = r.Close()

	fmt.Println(strings.Join(store.calls, "\n"))

	// Base returns the wrapped server.
	fmt.Println(server.Base().Type())

	// Output:
	// ensure docs-cache docs/a.txt
	// use docs-cache docs/a.txt
	// file
}

func ExampleItemKey() {
	fmt.Println(rsstorage.ItemKey("", "a.txt"))
	fmt.Println(rsstorage.ItemKey("docs/old", "a.txt"))

	// Output:
	// a.txt
	// docs/old/a.txt
}

func ExampleKeyHasPrefix() {
	// A prefix is matched over the whole key. It does not need to end at a "/".
	fmt.Println(rsstorage.KeyHasPrefix("", "GIT_1.gob", "GIT_"))
	fmt.Println(rsstorage.KeyHasPrefix("a", "bc", "a/b"))
	fmt.Println(rsstorage.KeyHasPrefix("a", "bc", "x"))

	// Output:
	// true
	// true
	// false
}

func ExampleSubtreeMayMatch() {
	// A directory is worth walking when the prefix reaches into it, or when the
	// directory is already inside the prefix.
	fmt.Println(rsstorage.SubtreeMayMatch("a", "a/b/c"))
	fmt.Println(rsstorage.SubtreeMayMatch("GIT_x", "GIT_"))
	fmt.Println(rsstorage.SubtreeMayMatch("ab", "a/b"))

	// Output:
	// true
	// true
	// false
}
