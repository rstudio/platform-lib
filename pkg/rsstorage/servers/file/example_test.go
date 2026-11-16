package file_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

// noopNotifier ignores chunk notifications. A single process that writes a
// chunked item and reads it only after the write finishes does not need them.
type noopNotifier struct{}

func (noopNotifier) Notify(ctx context.Context, c *types.ChunkNotification) error {
	return nil
}

// noopWaiter is never called when every item that is read is already complete.
type noopWaiter struct{}

func (noopWaiter) WaitForChunk(ctx context.Context, c *types.ChunkNotification) {}

// newServer creates a file server in a new temporary directory. The returned
// function removes the directory.
func newServer(chunkSize uint64) (rsstorage.StorageServer, func()) {
	dir, err := os.MkdirTemp("", "rsstorage-example-")
	if err != nil {
		panic(err)
	}
	server := file.NewStorageServer(file.StorageServerArgs{
		Dir:       dir,
		ChunkSize: chunkSize,
		Waiter:    noopWaiter{},
		Notifier:  noopNotifier{},
	})
	return server, func() { _ = os.RemoveAll(dir) }
}

// writeString returns a Resolver that writes s and leaves the location to the
// caller of Put.
func writeString(s string) types.Resolver {
	return func(w io.Writer) (string, string, error) {
		_, err := io.WriteString(w, s)
		return "", "", err
	}
}

func ExampleNewStorageServer() {
	ctx := context.Background()
	server, cleanup := newServer(1024)
	defer cleanup()

	// Put writes the item to "docs/hello.txt".
	dir, address, err := server.Put(ctx, writeString("hello, storage"), "docs", "hello.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("stored at:", dir, address)

	// Check reports whether the item exists, without reading it.
	found, chunked, size, _, err := server.Check(ctx, "docs", "hello.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("found:", found, "chunked:", chunked != nil, "size:", size)

	// Get returns a reader. Close it when done. For an item that is not
	// there, Get returns found=false, a nil reader and no error, so check
	// found before reading.
	r, _, _, _, found, err := server.Get(ctx, "docs", "hello.txt")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("docs/hello.txt was not found")
	}
	body, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	_ = r.Close()
	fmt.Println("read:", found, string(body))

	// Remove deletes the item. Removing an item that is not there is not an error.
	if err := server.Remove(ctx, "docs", "hello.txt"); err != nil {
		panic(err)
	}
	found, _, _, _, err = server.Check(ctx, "docs", "hello.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("found after remove:", found)

	// Output:
	// stored at: docs hello.txt
	// found: true chunked: false size: 14
	// read: true hello, storage
	// found after remove: false
}

// When both dir and address are empty, Put uses the location that the
// Resolver returns. This lets the content decide where it is stored.
func ExampleStorageServer_Put_resolverChoosesLocation() {
	ctx := context.Background()
	server, cleanup := newServer(1024)
	defer cleanup()

	resolve := func(w io.Writer) (string, string, error) {
		_, err := io.WriteString(w, "chosen by the resolver")
		return "reports", "q1.txt", err
	}
	dir, address, err := server.Put(ctx, resolve, "", "")
	if err != nil {
		panic(err)
	}
	fmt.Println(dir, address)

	// Output:
	// reports q1.txt
}

func ExampleStorageServer_PutChunked() {
	ctx := context.Background()
	// Use a tiny chunk size so that ten bytes of data make three chunks.
	server, cleanup := newServer(4)
	defer cleanup()

	// PutChunked needs the address and the total size up front.
	_, _, err := server.PutChunked(ctx, writeString("0123456789"), "docs", "big.txt", 10)
	if err != nil {
		panic(err)
	}

	_, info, size, _, err := server.Check(ctx, "docs", "big.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("chunks:", info.NumChunks, "chunk size:", info.ChunkSize, "complete:", info.Complete, "size:", size)

	// Get reads the chunks back as one stream.
	r, _, _, _, _, err := server.Get(ctx, "docs", "big.txt")
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	_ = r.Close()
	fmt.Println(string(body))

	// Output:
	// chunks: 3 chunk size: 4 complete: true size: 10
	// 0123456789
}

func ExampleStorageServer_Enumerate() {
	ctx := context.Background()
	server, cleanup := newServer(4)
	defer cleanup()

	put := func(dir, address string) {
		if _, _, err := server.Put(ctx, writeString("data"), dir, address); err != nil {
			panic(err)
		}
	}
	put("", "root.txt")
	put("docs", "a.txt")
	put("docs/old", "b.txt")
	if _, _, err := server.PutChunked(ctx, writeString("0123456789"), "docs", "big.txt", 10); err != nil {
		panic(err)
	}

	// A chunked item is listed once, with Chunked set, not as its chunk files.
	items, err := server.Enumerate(ctx)
	if err != nil {
		panic(err)
	}

	// The order of the results is not specified, so sort before printing.
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, fmt.Sprintf("dir=%q address=%q chunked=%t", item.Dir, item.Address, item.Chunked))
	}
	sort.Strings(lines)
	fmt.Println(strings.Join(lines, "\n"))

	// Output:
	// dir="" address="root.txt" chunked=false
	// dir="docs" address="a.txt" chunked=false
	// dir="docs" address="big.txt" chunked=true
	// dir="docs/old" address="b.txt" chunked=false
}

func ExampleStorageServer_Copy() {
	ctx := context.Background()
	source, cleanupSource := newServer(1024)
	defer cleanupSource()
	destination, cleanupDestination := newServer(1024)
	defer cleanupDestination()

	if _, _, err := source.Put(ctx, writeString("moving data"), "docs", "note.txt"); err != nil {
		panic(err)
	}

	// Copy leaves the item in the source. Move removes it from the source.
	if err := source.Copy(ctx, "docs", "note.txt", destination); err != nil {
		panic(err)
	}
	inSource, _, _, _, err := source.Check(ctx, "docs", "note.txt")
	if err != nil {
		panic(err)
	}
	inDestination, _, _, _, err := destination.Check(ctx, "docs", "note.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("after copy:", inSource, inDestination)

	if err := source.Move(ctx, "docs", "note.txt", destination); err != nil {
		panic(err)
	}
	inSource, _, _, _, err = source.Check(ctx, "docs", "note.txt")
	if err != nil {
		panic(err)
	}
	inDestination, _, _, _, err = destination.Check(ctx, "docs", "note.txt")
	if err != nil {
		panic(err)
	}
	fmt.Println("after move:", inSource, inDestination)

	// Output:
	// after copy: true true
	// after move: false true
}

func ExampleStorageServer_EnumeratePrefix() {
	ctx := context.Background()
	server, cleanup := newServer(1024)
	defer cleanup()

	for _, address := range []string{"GIT_1.gob", "GIT_2.gob", "OTHER_1.gob"} {
		if _, _, err := server.Put(ctx, writeString("data"), "", address); err != nil {
			panic(err)
		}
	}

	// EnumeratePrefix is not part of the StorageServer interface. Use a type
	// assertion to reach it.
	pe, ok := server.(rsstorage.PrefixEnumerator)
	if !ok {
		panic("server does not support prefix enumeration")
	}
	items, err := pe.EnumeratePrefix(ctx, "GIT_")
	if err != nil {
		panic(err)
	}

	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, rsstorage.ItemKey(item.Dir, item.Address))
	}
	sort.Strings(names)
	fmt.Println(names)

	// Output:
	// [GIT_1.gob GIT_2.gob]
}
