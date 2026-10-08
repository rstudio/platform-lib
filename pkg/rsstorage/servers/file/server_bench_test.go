package file

// Copyright (C) 2026 by Posit Software, PBC

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/internal/servertest"
)

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 4096)
	rand.Read(data)
	os.WriteFile(filepath.Join(dir, "testfile"), data, 0644)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, _, _, _, ok, err := server.Get(ctx, "", "testfile")
		if err != nil || !ok {
			b.Fatal(err)
		}
		io.Copy(io.Discard, r)
		r.Close()
	}
}

func BenchmarkPut(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 4096)
	rand.Read(data)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resolve := func(w io.Writer) (string, string, error) {
			_, err := w.Write(data)
			return "", "", err
		}
		_, _, err := server.Put(ctx, resolve, "", fmt.Sprintf("file-%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCheck(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 1024)
	rand.Read(data)
	os.WriteFile(filepath.Join(dir, "testfile"), data, 0644)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, _, _, _, err := server.Check(ctx, "", "testfile")
		if err != nil || !ok {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnumerate(b *testing.B) {
	for _, numFiles := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("files=%d", numFiles), func(b *testing.B) {
			dir := b.TempDir()
			server := &StorageServer{
				dir:    dir,
				fileIO: &defaultFileIO{},
			}

			for i := 0; i < numFiles; i++ {
				os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%d", i)), []byte("data"), 0644)
			}

			ctx := context.Background()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				items, err := server.Enumerate(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != numFiles {
					b.Fatalf("expected %d items, got %d", numFiles, len(items))
				}
			}
		})
	}
}

func BenchmarkEnumeratePrefix(b *testing.B) {
	for _, numFiles := range []int{100, 1000} {
		b.Run(fmt.Sprintf("files=%d", numFiles), func(b *testing.B) {
			dir := b.TempDir()
			server := &StorageServer{
				dir:    dir,
				fileIO: &defaultFileIO{},
			}

			for i := 0; i < numFiles/2; i++ {
				os.WriteFile(filepath.Join(dir, fmt.Sprintf("PREFIX_%d", i)), []byte("data"), 0644)
			}
			for i := 0; i < numFiles/2; i++ {
				os.WriteFile(filepath.Join(dir, fmt.Sprintf("OTHER_%d", i)), []byte("data"), 0644)
			}

			ctx := context.Background()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				items, err := server.EnumeratePrefix(ctx, "PREFIX_")
				if err != nil {
					b.Fatal(err)
				}
				if len(items) != numFiles/2 {
					b.Fatalf("expected %d items, got %d", numFiles/2, len(items))
				}
			}
		})
	}
}

func BenchmarkEnumerateNested(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	for i := 0; i < 10; i++ {
		subdir := filepath.Join(dir, fmt.Sprintf("dir%d", i))
		os.MkdirAll(subdir, 0700)
		for j := 0; j < 100; j++ {
			os.WriteFile(filepath.Join(subdir, fmt.Sprintf("file-%d", j)), []byte("data"), 0644)
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		items, err := server.Enumerate(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(items) != 1000 {
			b.Fatalf("expected 1000 items, got %d", len(items))
		}
	}
}

func BenchmarkDiskUsage(b *testing.B) {
	for _, numFiles := range []int{100, 1000} {
		b.Run(fmt.Sprintf("files=%d", numFiles), func(b *testing.B) {
			dir := b.TempDir()

			data := make([]byte, 1024)
			rand.Read(data)
			for i := 0; i < numFiles; i++ {
				os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%d", i)), data, 0644)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := diskUsage(dir, defaultWalkTimeout, defaultWalkTimeout)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCopy(b *testing.B) {
	srcDir := b.TempDir()
	dstDir := b.TempDir()

	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}
	srcServer := &StorageServer{
		dir:    srcDir,
		fileIO: &defaultFileIO{},
	}
	srcServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    srcServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	dstServer := &StorageServer{
		dir:    dstDir,
		fileIO: &defaultFileIO{},
	}
	dstServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    dstServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	data := make([]byte, 64*1024)
	rand.Read(data)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("file-%d", i)
		os.WriteFile(filepath.Join(srcDir, filename), data, 0644)
		b.StartTimer()

		err := srcServer.Copy(ctx, "", filename, dstServer)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMove(b *testing.B) {
	srcDir := b.TempDir()
	dstDir := b.TempDir()

	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}
	srcServer := &StorageServer{
		dir:    srcDir,
		fileIO: &defaultFileIO{},
	}
	srcServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    srcServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	dstServer := &StorageServer{
		dir:    dstDir,
		fileIO: &defaultFileIO{},
	}
	dstServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    dstServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	data := make([]byte, 64*1024)
	rand.Read(data)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("file-%d", i)
		os.WriteFile(filepath.Join(srcDir, filename), data, 0644)
		b.StartTimer()

		err := srcServer.Move(ctx, "", filename, dstServer)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemove(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 4096)
	rand.Read(data)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		filename := fmt.Sprintf("file-%d", i)
		os.WriteFile(filepath.Join(dir, filename), data, 0644)
		b.StartTimer()

		err := server.Remove(ctx, "", filename)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutChunked(b *testing.B) {
	dir := b.TempDir()

	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}

	data := make([]byte, 64*1024)
	rand.Read(data)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resolve := func(w io.Writer) (string, string, error) {
			_, err := io.Copy(w, bytes.NewReader(data))
			return "", "", err
		}
		_, _, err := server.PutChunked(ctx, resolve, "", fmt.Sprintf("chunk-%d", i), uint64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetChunked(b *testing.B) {
	dir := b.TempDir()

	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}

	data := make([]byte, 64*1024)
	rand.Read(data)

	ctx := context.Background()

	resolve := func(w io.Writer) (string, string, error) {
		_, err := io.Copy(w, bytes.NewReader(data))
		return "", "", err
	}
	_, _, err := server.PutChunked(ctx, resolve, "", "chunked-file", uint64(len(data)))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, _, _, _, ok, err := server.Get(ctx, "", "chunked-file")
		if err != nil || !ok {
			b.Fatal(err)
		}
		io.Copy(io.Discard, r)
		r.Close()
	}
}

func BenchmarkNewStorageServer(b *testing.B) {
	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewStorageServer(StorageServerArgs{
			Dir:       "/tmp/test",
			ChunkSize: 4096,
			Waiter:    wn,
			Notifier:  wn,
			Class:     "test",
		})
	}
}

func BenchmarkCalculateUsage(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:          dir,
		fileIO:       &defaultFileIO{},
		cacheTimeout: defaultWalkTimeout,
		walkTimeout:  defaultWalkTimeout,
	}

	data := make([]byte, 1024)
	rand.Read(data)
	for i := 0; i < 100; i++ {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%d", i)), data, 0644)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := server.CalculateUsage()
		if err != nil {
			b.Fatal(err)
		}
	}
}

type leakDetector struct {
	gets   int
	closes int
}

func (l *leakDetector) recordGet()   { l.gets++ }
func (l *leakDetector) recordClose() { l.closes++ }

type trackingReader struct {
	io.ReadCloser
	detector *leakDetector
}

func (t *trackingReader) Close() error {
	t.detector.recordClose()
	return t.ReadCloser.Close()
}

func BenchmarkCopyWithLeakCheck(b *testing.B) {
	srcDir := b.TempDir()
	dstDir := b.TempDir()

	wn := &servertest.DummyWaiterNotifier{Ch: make(chan bool, 1)}
	srcServer := &StorageServer{
		dir:    srcDir,
		fileIO: &defaultFileIO{},
	}
	srcServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    srcServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	dstServer := &StorageServer{
		dir:    dstDir,
		fileIO: &defaultFileIO{},
	}
	dstServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 4096,
		Server:    dstServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	data := make([]byte, 4*1024)
	rand.Read(data)

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("file-%d", i)
		os.WriteFile(filepath.Join(srcDir, filename), data, 0644)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			filename := fmt.Sprintf("file-%d", j)
			err := srcServer.Copy(ctx, "", filename, dstServer)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkFlush(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 4096)
	rand.Read(data)
	os.WriteFile(filepath.Join(dir, "testfile"), data, 0644)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		server.Flush(ctx, "", "testfile")
	}
}

func BenchmarkLocate(b *testing.B) {
	server := &StorageServer{
		dir: "/some/test/directory",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = server.Locate("subdir", "address")
	}
}

type allocTestResolver struct {
	data []byte
}

func (r *allocTestResolver) resolve(w io.Writer) (string, string, error) {
	_, err := w.Write(r.data)
	return "", "", err
}

func BenchmarkPutAllocs(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	resolver := &allocTestResolver{data: make([]byte, 4096)}
	rand.Read(resolver.data)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := server.Put(ctx, resolver.resolve, "", fmt.Sprintf("file-%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCheckAllocs(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 1024)
	rand.Read(data)
	os.WriteFile(filepath.Join(dir, "testfile"), data, 0644)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, _, err := server.Check(ctx, "", "testfile")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReadCycle(b *testing.B) {
	dir := b.TempDir()
	server := &StorageServer{
		dir:    dir,
		fileIO: &defaultFileIO{},
	}

	data := make([]byte, 16*1024)
	rand.Read(data)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		filename := fmt.Sprintf("file-%d", i)
		resolve := func(w io.Writer) (string, string, error) {
			_, err := w.Write(data)
			return "", "", err
		}
		_, _, err := server.Put(ctx, resolve, "", filename)
		if err != nil {
			b.Fatal(err)
		}

		r, _, _, _, ok, err := server.Get(ctx, "", filename)
		if err != nil || !ok {
			b.Fatal(err)
		}
		io.Copy(io.Discard, r)
		r.Close()
	}
}
