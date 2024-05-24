package file

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

const (
	walkCheckTime      = 250 * time.Millisecond
	defaultWalkTimeout = 5 * time.Minute
)

var (
	cacheTimeoutErr = errors.New("cacheTimeout reached walking file storage server")
	walktimeoutErr  = errors.New("walkTimeout reached walking file storage server")
)

type StorageServer struct {
	dir          string
	class        string
	fileIO       fileIO
	chunker      rsstorage.ChunkUtils
	cacheTimeout time.Duration
	walkTimeout  time.Duration
}

type StorageServerArgs struct {
	Dir          string
	ChunkSize    uint64
	Waiter       rsstorage.ChunkWaiter
	Notifier     rsstorage.ChunkNotifier
	Class        string
	CacheTimeout time.Duration
	WalkTimeout  time.Duration
}

func NewStorageServer(args StorageServerArgs) rsstorage.StorageServer {
	fs := &StorageServer{
		dir:          args.Dir,
		fileIO:       &defaultFileIO{},
		class:        args.Class,
		cacheTimeout: args.CacheTimeout,
		walkTimeout:  args.WalkTimeout,
	}
	return &StorageServer{
		dir:          args.Dir,
		fileIO:       &defaultFileIO{},
		cacheTimeout: args.CacheTimeout,
		walkTimeout:  args.WalkTimeout,
		chunker: &internal.DefaultChunkUtils{
			ChunkSize:   args.ChunkSize,
			Server:      fs,
			Waiter:      args.Waiter,
			Notifier:    args.Notifier,
			PollTimeout: rsstorage.DefaultChunkPollTimeout,
			MaxAttempts: rsstorage.DefaultMaxChunkAttempts,
		},
		class: args.Class,
	}
}

func (s *StorageServer) Check(dir, address string) (bool, *types.ChunksInfo, int64, time.Time, error) {
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

		info := types.ChunksInfo{}
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

func (s *StorageServer) Dir() string {
	return s.dir
}

func (s *StorageServer) Type() types.StorageType {
	return rsstorage.StorageTypeFile
}

func (s *StorageServer) CalculateUsage() (types.Usage, error) {
	start := time.Now()

	fs := syscall.Statfs_t{}
	err := syscall.Statfs(s.dir, &fs)
	if err != nil {
		return types.Usage{}, fmt.Errorf("error calculating filesystem capacity for %s: %s.\n", s.dir, err)
	}

	all := fs.Blocks * uint64(fs.Bsize)
	free := fs.Bfree * uint64(fs.Bsize)

	timeInfo := time.Now()
	elapsed := timeInfo.Sub(start)
	slog.Debug(fmt.Sprintf("Calculated disk info for %s in %s.\n", s.dir, elapsed))

	actual, err := diskUsage(s.dir, s.cacheTimeout, s.walkTimeout)
	if err != nil {
		return types.Usage{}, fmt.Errorf("error calculating disk usage for %s: %s.\n", s.dir, err)
	}

	timeUsage := time.Now()
	elapsed = timeUsage.Sub(timeInfo)
	slog.Debug(fmt.Sprintf("Calculated disk usage for %s in %s.\n", s.dir, elapsed))

	usage := types.Usage{
		SizeBytes:       datasize.ByteSize(all),
		FreeBytes:       datasize.ByteSize(free),
		UsedBytes:       actual,
		CalculationTime: timeUsage.Sub(start),
	}
	return usage, nil
}

// diskUsage will walk the specified path in a filesystem and
// aggregate the size of the contained files.
func diskUsage(duPath string, cacheTimeout, walkTimeout time.Duration) (size datasize.ByteSize, err error) {
	stop := make(chan struct{})
	defer close(stop)

	sizeChan := make(chan datasize.ByteSize)
	// errChan should have a buffer of two items to prevent deadlock between `<-stop` and `errChan<-err`
	errChan := make(chan error, 2)

	go func(stop <-chan struct{}, sizeChan chan<- datasize.ByteSize, errChan chan<- error) {
		defer close(sizeChan)
		defer close(errChan)

		err = filepath.Walk(duPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			select {
			case <-stop:
				return nil
			default:
			}

			if !info.IsDir() {
				sizeChan <- datasize.ByteSize(info.Size())
			}

			return nil
		})

		if err != nil {
			errChan <- err
		}
	}(stop, sizeChan, errChan)

	cacheTimeoutTimer := time.NewTimer(cacheTimeout)
	defer cacheTimeoutTimer.Stop()

	if walkTimeout == 0 {
		walkTimeout = defaultWalkTimeout
	}

	walkTimeoutTimer := time.NewTimer(walkTimeout)
	defer walkTimeoutTimer.Stop()

	for {
		select {
		case <-cacheTimeoutTimer.C:
			return 0, cacheTimeoutErr
		case sz := <-sizeChan:
			size += sz

			walkTimeoutTimer.Stop()
			walkTimeoutTimer.Reset(walkTimeout)
		case err = <-errChan:
			// Success case error will return `nil`
			return
		case <-walkTimeoutTimer.C:
			return 0, walktimeoutErr
		}
	}
}

func (s *StorageServer) Get(dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
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

func (s *StorageServer) Flush(dir, address string) {
	// Determine location for this file
	filePath := filepath.Join(s.dir, dir, address)

	// Don't err if this fails
	s.fileIO.FlushWithChownAndStat(filePath)
}

func (s *StorageServer) Put(resolve types.Resolver, dir, address string) (string, string, error) {

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

func (s *StorageServer) PutChunked(resolve types.Resolver, dir, address string, sz uint64) (string, string, error) {
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

func (s *StorageServer) write(resolve types.Resolver) (dir, address, staging string, err error) {
	// Open the file where we will stage the data
	stagingFile, err := s.fileIO.OpenStaging(s.dir, "")
	if err != nil {
		return
	}
	defer stagingFile.Close()
	staging = stagingFile.Name()

	slog.Debug(fmt.Sprintf("Opened new staging file for storage: %s.\n", stagingFile.Name()))

	// Resolve/get the data we need
	dir, address, err = resolve(stagingFile)
	if err != nil {
		return
	}

	return
}

func (s *StorageServer) cleanup(staging string) {
	// Clean up, but don't error if we fail
	removeError := s.fileIO.Remove(staging)
	if removeError != nil && !os.IsNotExist(removeError) {
		// Warn and discard errors cleaning up
		slog.Debug(fmt.Sprintf("file.StorageServer error while cleaning up staged data: %s", removeError))
	}
}

func (s *StorageServer) Remove(dir, address string) error {
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

func (s *StorageServer) Enumerate() ([]types.StoredItem, error) {
	items, err := enumerate(s.dir, s.walkTimeout)
	if err != nil {
		log.Printf("Error enumerating storage: %s", err)

		return nil, err
	}

	return internal.FilterChunks(items), nil
}

func enumerate(dir string, walkTimeout time.Duration) ([]types.StoredItem, error) {
	stop := make(chan struct{})
	defer close(stop)

	itemChan := make(chan *types.StoredItem)
	// errChan should have a buffer of two items to prevent deadlock between `<-stop` and `errChan<-err`
	errChan := make(chan error, 2)

	go func(stop <-chan struct{}, itemChan chan<- *types.StoredItem, errChan chan<- error) {
		defer close(itemChan)
		defer close(errChan)

		err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				log.Printf("Error enumerating storage for directory %s: %s", dir, err)
				return nil
			}

			if !info.IsDir() {
				relPath, err := filepath.Rel(dir, path)
				if err != nil {
					return err
				}

				dir := filepath.Dir(relPath)
				if dir == "." {
					dir = ""
				}

				itemChan <- &types.StoredItem{
					Dir:     dir,
					Address: info.Name(),
				}
			}

			return nil
		})

		if err != nil {
			errChan <- err
		}
	}(stop, itemChan, errChan)

	items := make([]types.StoredItem, 0)

	if walkTimeout == 0 {
		walkTimeout = defaultWalkTimeout
	}

	walkTimeoutTimer := time.NewTimer(walkTimeout)
	defer walkTimeoutTimer.Stop()

	for {
		select {
		case item := <-itemChan:
			if item != nil {
				items = append(items, *item)
			}

			walkTimeoutTimer.Stop()
			walkTimeoutTimer.Reset(walkTimeout)
		case err := <-errChan:
			if err != nil {
				return nil, err
			}

			return items, nil
		case <-walkTimeoutTimer.C:
			return nil, walktimeoutErr
		}
	}
}

func (s *StorageServer) move(dir, address string, server rsstorage.StorageServer) error {
	source := s.Locate(dir, address)
	dest := server.Locate(dir, address)
	slog.Debug(fmt.Sprintf("Renaming %s to %s", source, dest))
	destDir := filepath.Dir(dest)
	if destDir != dest {
		slog.Debug(fmt.Sprintf("Ensuring directory %s exists", destDir))
		err := os.MkdirAll(destDir, 0700)
		if err != nil {
			return err
		}
	}
	err := os.Rename(source, dest)
	if err != nil {
		slog.Debug(fmt.Sprintf("Error moving with os.Rename: %s", err))
		return err
	}

	return nil
}

func (s *StorageServer) Move(dir, address string, server rsstorage.StorageServer) error {
	copy := true
	switch server.(type) {
	case *StorageServer:
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

func (s *StorageServer) Copy(dir, address string, server rsstorage.StorageServer) error {
	// Open the file
	f, chunked, sz, _, ok, err := s.Get(dir, address)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("the file at %s to copy does not exist", filepath.Join(dir, address))
	}

	install := func(file io.ReadCloser) types.Resolver {
		return func(writer io.Writer) (string, string, error) {
			_, err := io.Copy(writer, file)
			return "", "", err
		}
	}

	// Use the server Base() in case the server is wrapped, e.g., `Metadatarsstorage.StorageServer`
	if chunked != nil {
		_, _, err = server.Base().PutChunked(install(f), dir, address, uint64(sz))
	} else {
		_, _, err = server.Base().Put(install(f), dir, address)
	}
	return err
}

func (s *StorageServer) Locate(dir, address string) string {
	return filepath.Join(s.dir, dir, address)
}

func (s *StorageServer) Base() rsstorage.StorageServer {
	return s
}

type fileIO interface {
	MkdirAll(name string, perm os.FileMode) error
	Open(name string) (fileIOFile, error)
	OpenStaging(dir, prefix string) (f fileIOFile, err error)
	Move(stagedAt, permanent string) error
	Remove(location string) error
	RemoveAll(location string) error
	FlushWithChownAndStat(location string) error
	Stat(name string) (os.FileInfo, error)
}

type fileIOFile interface {
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

func (f *defaultFileIO) Open(name string) (fileIOFile, error) {
	return os.Open(name)
}

func (f *defaultFileIO) OpenStaging(dir, prefix string) (fileIOFile, error) {
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
