package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"io"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

// Resolver - A function type that populates the cache with some item. This function
// is passed to the `Put` method. The `Put` method accepts `dir` and
// `address` arguments, but if they are not provided, it uses the values
// returned by this function instead.
type Resolver func(writer io.Writer) (dir, address string, err error)

// StorageType provides a way for servers to identify what type of
// underlying storage they are using.
type StorageType string

const (
	StorageTypeFile     = StorageType("file")
	StorageTypePostgres = StorageType("postgres")
	StorageTypeS3       = StorageType("s3")
)

// The PersistentStorageServer provides an interface to the file system
// for:
//  (a) The FileCache in `file.go` (gets data from the cache)
//  (b) The Runners in `cache/runners` (put data into the cache)
type PersistentStorageServer interface {
	// Check to see if an item exists on persistent storage
	// Accepts:
	//  * dir     The prefix or directory in which to look
	//  * address The address of the item
	// Returns:
	//  * bool `true` if found
	//  * ChunkInfo if chunked
	//  * int64 the file size if known
	//  * time.Time the last modification time if known
	//  * error
	Check(dir, address string) (bool, *ChunksInfo, int64, time.Time, error)

	// Dir will present the underlying "directory" for a
	// storage server. This doesn't make sense for all server
	// types, but for interface reasons we need it.
	Dir() string

	// Type will present the "type" of server implementation.
	Type() StorageType

	// CalculateUsage will look at the underlying storage and
	// report information about the usage.
	CalculateUsage() (types.Usage, error)

	// Get an item from persistent storage, if it exists
	// Accepts:
	//  * dir     The prefix or directory in which to look
	//  * address The address of the item
	// Returns:
	//  * io.ReadCloser the file
	//  * int64 The size of the file
	//  * time.Time The last modification time
	//  * bool `true` if found
	//  * error
	Get(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, bool, error)

	// Put writes an item to persistent storage. Creates a file
	// named `address`, and then passes the file writer
	// to the provided `resolve` function for writing.
	// See `cache/runners` for a number of queue runners
	// that are responsible for using this method to store
	// data
	Put(resolve Resolver, dir, address string) (string, string, error)

	// PutChunked writes an item to persistent storage. Creates a directory
	// named `address`, and then passes the file writer
	// to the provided `resolve` function for writing. The
	// data will be written to the directly in a series
	// of chunk files of fixed size. The `dir`, `address`, and
	// `sz` parameters must all be included, unlike the regular
	// Put method, which allows post-determined location and size.
	PutChunked(resolve Resolver, dir, address string, sz uint64) (string, string, error)

	// Remove an item from persistent storage
	Remove(dir, address string) error

	// Flush the NFS cache while waiting for an
	// item to appear
	Flush(dir, address string)

	// Enumerate all items in storage
	Enumerate() ([]PersistentStorageItem, error)

	// Move an item from one storage system to another
	Move(dir, address string, server PersistentStorageServer) error

	// Copy an item from one storage system to another
	Copy(dir, address string, server PersistentStorageServer) error

	// Locate returns the storage location for a given dir, address
	Locate(dir, address string) string

	// Base returns the base storage server in case the server is wrapped
	Base() PersistentStorageServer
}

type Logger interface {
	Debugf(msg string, args ...interface{})
}

type DebugLogger interface {
	Logger
	Enabled() bool
}

type MoveCopyFn func(dir, address string, server PersistentStorageServer) error

type PersistentStorageItem struct {
	Dir     string
	Address string
	Chunked bool
}

type PersistentStorageStore interface {
	CacheObjectEnsureExists(cacheName, key string) error
	CacheObjectMarkUse(cacheName, key string, accessTime time.Time) error
}

type Config struct {
	CacheTimeout   time.Duration
	ChunkSizeBytes uint64
	S3             *ConfigS3
	File           *ConfigFile
}

type ConfigFile struct {
	Location string
}

type ConfigS3 struct {
	Bucket             string
	Prefix             string
	Profile            string
	Region             string
	Endpoint           string
	SkipValidation     bool
	DisableSSL         bool
	S3ForcePathStyle   bool
	EnableSharedConfig bool
}

type CopyPart struct {
	Dir     string
	Address string
}

func NewCopyPart(dir, address string) CopyPart {
	return CopyPart{
		Dir:     dir,
		Address: address,
	}
}
