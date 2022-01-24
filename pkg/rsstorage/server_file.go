package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/minio/minio/pkg/disk"

	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

type FileStorageServer struct {
	dir          string
	class        string
	fileIO       FileIO
	chunker      chunkUtils
	cacheTimeout time.Duration
	debugLogger  DebugLogger
}

func NewFileStorageServer(dir string, chunkSize uint64, waiter ChunkWaiter, notifier ChunkNotifier, class string, debugLogger DebugLogger, cacheTimeout time.Duration) PersistentStorageServer {
	fs := &FileStorageServer{
		dir:          dir,
		fileIO:       &defaultFileIO{},
		class:        class,
		debugLogger:  debugLogger,
		cacheTimeout: cacheTimeout,
	}
	return &FileStorageServer{
		dir:          dir,
		fileIO:       &defaultFileIO{},
		debugLogger:  debugLogger,
		cacheTimeout: cacheTimeout,
		chunker: &defaultChunkUtils{
			chunkSize:   chunkSize,
			server:      fs,
			waiter:      waiter,
			notifier:    notifier,
			pollTimeout: chunkPollTimeout,
			maxAttempts: maxChunkAttempts,
		},
		class: class,
	}
}

func (s *FileStorageServer) Check(dir, address string) (bool, *ChunksInfo, int64, time.Time, error) {
	// Determine the location for this file
	filePath := filepath.Join(s.dir, dir, address)

	// Open the file
	stat, err := s.fileIO.Stat(filePath)
	if os.IsNotExist(err) {
		return false, nil, 0, time.Time{}, nil
	} else if err != nil {
		return false, nil, 0, time.Time{}, err
	}

	if stat.IsDir() {
		infoFile, err := s.fileIO.Open(filepath.Join(filePath, "info.json"))
		if err != nil {
			return false, nil, 0, time.Time{}, fmt.Errorf("no chunked directory 'info.json' for %s: %s", address, err)
		}
		defer infoFile.Close()

		info := ChunksInfo{}
		dec := json.NewDecoder(infoFile)
		err = dec.Decode(&info)
		if err != nil {
			return false, nil, 0, time.Time{}, fmt.Errorf("error decoding chunked directory 'info.json' for %s: %s", address, err)
		}

		return true, &info, int64(info.FileSize), info.ModTime, nil
	} else {
		// Return normal file info
		return true, nil, stat.Size(), stat.ModTime(), nil
	}
}

func (s *FileStorageServer) Dir() string {
	return s.dir
}

func (s *FileStorageServer) Type() StorageType {
	return StorageTypeFile
}

func (s *FileStorageServer) CalculateUsage() (types.Usage, error) {
	start := time.Now()
	info, err := disk.GetInfo(s.dir)
	if err != nil {
		return types.Usage{}, fmt.Errorf("error calculating filesystem capacity for %s: %s.\n", s.dir, err)
	}

	timeInfo := time.Now()
	elapsed := timeInfo.Sub(start)
	s.debugLogger.Debugf("Calculated disk info for %s in %s.\n", s.dir, elapsed)

	actual, err := DiskUsage(s.dir, s.cacheTimeout)
	if err != nil {
		return types.Usage{}, fmt.Errorf("error calculating disk usage for %s: %s.\n", s.dir, err)
	}

	timeUsage := time.Now()
	elapsed = timeUsage.Sub(timeInfo)
	s.debugLogger.Debugf("Calculated disk usage for %s in %s.\n", s.dir, elapsed)

	usage := types.Usage{
		SizeBytes:       datasize.ByteSize(info.Total),
		FreeBytes:       datasize.ByteSize(info.Free),
		UsedBytes:       actual,
		CalculationTime: timeUsage.Sub(start),
	}
	return usage, nil
}

// DiskUsage will walk the specified path in a filesystem and
// aggregate the size of the contained files.
func DiskUsage(duPath string, cacheTimeout time.Duration) (size datasize.ByteSize, err error) {
	timeout := time.Now().Add(cacheTimeout)
	sizep := &size

	err = filepath.Walk(duPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if time.Now().After(timeout) {
			return errors.New("timeout in DiskUsage")
		}

		if !info.IsDir() {
			*sizep += datasize.ByteSize(info.Size())
		}

		return nil
	})
	size = *sizep

	return
}

func (s *FileStorageServer) Get(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, bool, error) {
	// Determine the location for this file
	filePath := filepath.Join(s.dir, dir, address)

	// Open the file
	f, err := s.fileIO.Open(filePath)
	if os.IsNotExist(err) {
		return nil, nil, 0, time.Time{}, false, nil
	}
	if err != nil {
		return nil, nil, 0, time.Time{}, false, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, nil, 0, time.Time{}, false, err
	}

	if stat.IsDir() {
		r, c, sz, mod, err := s.chunker.ReadChunked(dir, address)
		if err != nil {
			return nil, nil, 0, time.Time{}, false, fmt.Errorf("error reading chunked directory files for %s: %s", address, err)
		}

		return r, c, sz, mod, true, nil
	} else {
		return f, nil, stat.Size(), stat.ModTime(), true, nil
	}
}

func (s *FileStorageServer) Flush(dir, address string) {
	// Determine location for this file
	filePath := filepath.Join(s.dir, dir, address)

	// Don't err if this fails
	s.fileIO.FlushWithChownAndStat(filePath)
}

func (s *FileStorageServer) Put(resolve Resolver, dir, address string) (string, string, error) {

	// Store the data
	wdir, waddress, staging, err := s.write(resolve)
	defer s.cleanup(staging)
	if err != nil {
		return "", "", err
	}

	// If no dir and address were provided, use the ones optionally returned
	// from the resolver function
	if dir == "" && address == "" {
		dir = wdir
		address = waddress
	}

	// Determine the location for this file
	filePath := filepath.Join(s.dir, dir, address)

	// If the item is to be stored in a directory, create it
	if dir != "" {
		if err := s.fileIO.MkdirAll(filepath.Join(s.dir, dir), 0700); err != nil {
			return "", "", err
		}
	}

	// Move from staging to permanent location
	err = s.fileIO.Move(staging, filePath)
	if err != nil {
		return "", "", err
	}

	return dir, address, nil
}

func (s *FileStorageServer) PutChunked(resolve Resolver, dir, address string, sz uint64) (string, string, error) {
	if address == "" {
		return "", "", fmt.Errorf("cache only supports pre-addressed chunked put commands")
	}
	if sz == 0 {
		return "", "", fmt.Errorf("cache only supports pre-sized chunked put commands")
	}
	err := s.chunker.WriteChunked(dir, address, sz, resolve)
	if err != nil {
		return "", "", err
	}

	return dir, address, nil
}

func (s *FileStorageServer) write(resolve Resolver) (dir, address, staging string, err error) {
	// Open the file where we will stage the data
	stagingFile, err := s.fileIO.OpenStaging(s.dir, "")
	if err != nil {
		return
	}
	defer stagingFile.Close()
	s.debugLogger.Debugf("Opened new staging file for storage: %s.\n", stagingFile.Name())

	// Resolve/get the data we need
	dir, address, err = resolve(stagingFile)
	if err != nil {
		return
	}

	staging = stagingFile.Name()
	return
}

func (s *FileStorageServer) cleanup(staging string) {
	// Clean up, but don't error if we fail
	removeError := s.fileIO.Remove(staging)
	if removeError != nil && !os.IsNotExist(removeError) {
		// Warn and discard errors cleaning up
		s.debugLogger.Debugf("FileStorageServer error while cleaning up staged data: %s", removeError)
	}
}

func (s *FileStorageServer) Remove(dir, address string) error {
	// Determine the location for this file
	ok, chunked, _, _, err := s.Check(dir, address)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	filePath := filepath.Join(s.dir, dir, address)
	if chunked != nil {
		return s.fileIO.RemoveAll(filePath)
	} else {
		return s.fileIO.Remove(filePath)
	}
}

func (s *FileStorageServer) Enumerate() ([]PersistentStorageItem, error) {
	items := make([]PersistentStorageItem, 0)
	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error enumerating persistent storage for directory %s: %s", s.dir, err)
			return nil
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(s.dir, path)
			if err != nil {
				return err
			}
			dir := filepath.Dir(relPath)
			if dir == "." {
				dir = ""
			}
			items = append(items, PersistentStorageItem{
				Dir:     dir,
				Address: info.Name(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filterChunks(items), nil
}

func filterChunks(input []PersistentStorageItem) []PersistentStorageItem {
	output := make([]PersistentStorageItem, 0)
	chunkDirs := make(map[string]bool)

	// Find all directories that are chunked and append one
	// result for each chunked directory.
	for _, i := range input {
		if i.Dir != "" && i.Address == "info.json" {
			chunkDirs[i.Dir] = true
			d, f := filepath.Split(i.Dir)
			output = append(output, PersistentStorageItem{
				Dir:     strings.TrimSuffix(d, "/"),
				Address: f,
				Chunked: true,
			})
		}
	}

	// Eliminate chunk data from results
	for _, i := range input {
		if !chunkDirs[i.Dir] {
			output = append(output, i)
		}
	}

	return output
}

func (s *FileStorageServer) move(dir, address string, server PersistentStorageServer) error {
	source := s.Locate(dir, address)
	dest := server.Locate(dir, address)
	s.debugLogger.Debugf("Renaming %s to %s", source, dest)
	destDir := filepath.Dir(dest)
	if destDir != dest {
		s.debugLogger.Debugf("Ensuring directory %s exists", destDir)
		err := os.MkdirAll(destDir, 0700)
		if err != nil {
			return err
		}
	}
	err := os.Rename(source, dest)
	if err != nil {
		s.debugLogger.Debugf("Error moving with os.Rename: %s", err)
		return err
	}

	return nil
}

func (s *FileStorageServer) Move(dir, address string, server PersistentStorageServer) error {
	copy := true
	switch server.(type) {
	case *FileStorageServer:
		// Attempt move
		err := s.move(dir, address, server)
		if err == nil {
			copy = false
		}
	default:
		// Don't do anything. Just copy
	}

	// Copy the file, if needed
	if copy {
		err := s.Copy(dir, address, server)
		if err != nil {
			return err
		}

		// Then, remove the file
		err = s.Remove(dir, address)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *FileStorageServer) Copy(dir, address string, server PersistentStorageServer) error {
	// Open the file
	f, chunked, sz, _, ok, err := s.Get(dir, address)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("the file at %s to copy does not exist", filepath.Join(dir, address))
	}

	install := func(file io.ReadCloser) Resolver {
		return func(writer io.Writer) (string, string, error) {
			_, err := io.Copy(writer, file)
			return "", "", err
		}
	}

	// Use the server Base() in case the server is wrapped, e.g., `MetadataPersistentStorageServer`
	if chunked != nil {
		_, _, err = server.Base().PutChunked(install(f), dir, address, uint64(sz))
	} else {
		_, _, err = server.Base().Put(install(f), dir, address)
	}
	return err
}

func (s *FileStorageServer) Locate(dir, address string) string {
	return filepath.Join(s.dir, dir, address)
}

func (s *FileStorageServer) Base() PersistentStorageServer {
	return s
}

type FileIO interface {
	MkdirAll(name string, perm os.FileMode) error
	Open(name string) (FileIOFile, error)
	OpenStaging(dir, prefix string) (f FileIOFile, err error)
	Move(stagedAt, permanent string) error
	Remove(location string) error
	RemoveAll(location string) error
	FlushWithChownAndStat(location string) error
	Stat(name string) (os.FileInfo, error)
}

type FileIOFile interface {
	Name() string
	Close() error
	Read(p []byte) (n int, err error)
	Seek(offset int64, whence int) (int64, error)
	Write(p []byte) (n int, err error)
	Stat() (os.FileInfo, error)
}

type defaultFileIO struct{}

func (f *defaultFileIO) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (f *defaultFileIO) MkdirAll(name string, perm os.FileMode) error {
	return os.MkdirAll(name, perm)
}

func (f *defaultFileIO) Open(name string) (FileIOFile, error) {
	return os.Open(name)
}

func (f *defaultFileIO) OpenStaging(dir, prefix string) (FileIOFile, error) {
	return ioutil.TempFile(dir, prefix)
}

func (f *defaultFileIO) Move(stagedAt, permanent string) error {
	return os.Rename(stagedAt, permanent)
}

func (f *defaultFileIO) Remove(location string) error {
	return os.Remove(location)
}

func (f *defaultFileIO) RemoveAll(location string) error {
	return os.RemoveAll(location)
}

func (f *defaultFileIO) FlushWithChownAndStat(location string) error {
	if _, err := os.Stat(location); err != nil {
		return err
	}
	return os.Chown(location, os.Getuid(), os.Getgid())
}
