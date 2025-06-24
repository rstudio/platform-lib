package rsstorage

// Copyright (C) 2022 by Posit, PBC

import (
	"context"
	"io"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

const (
	StorageTypeFile     = types.StorageType("file")
	StorageTypePostgres = types.StorageType("postgres")
	StorageTypeS3       = types.StorageType("s3")
)

// The StorageServer provides an interface to the file system
// for:
//
//	(a) The FileCache in `file.go` (gets data from the cache)
//	(b) The Runners in `cache/runners` (put data into the cache)
type StorageServer interface {
	// Check to see if an item exists
	// Accepts:
	//  * dir     The prefix or directory in which to look
	//  * address The address of the item
	// Returns:
	//  * bool `true` if found
	//  * ChunkInfo if chunked
	//  * int64 the file size if known
	//  * time.Time the last modification time if known
	//  * error
	Check(ctx context.Context, dir, address string) (bool, *types.ChunksInfo, int64, time.Time, error)

	// Dir will present the underlying "directory" for a
	// storage server. This doesn't make sense for all server
	// types, but for interface reasons we need it.
	Dir() string

	// Type will present the "type" of server implementation.
	Type() types.StorageType

	// CalculateUsage will look at the underlying storage and
	// report information about the usage.
	CalculateUsage() (types.Usage, error)

	// Get an item if it exists
	// Accepts:
	//  * dir     The prefix or directory in which to look
	//  * address The address of the item
	// Returns:
	//  * io.ReadCloser the file
	//  * int64 The size of the file
	//  * time.Time The last modification time
	//  * bool `true` if found
	//  * error
	Get(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error)

	// Put writes an item. Creates a file
	// named `address`, and then passes the file writer
	// to the provided `resolve` function for writing.
	// See `cache/runners` for a number of queue runners
	// that are responsible for using this method to store
	// data
	Put(ctx context.Context, resolve types.Resolver, dir, address string) (string, string, error)

	// PutChunked writes an item. Creates a directory
	// named `address`, and then passes the file writer
	// to the provided `resolve` function for writing. The
	// data will be written to the directory in a series
	// of chunk files of fixed size. The `dir`, `address`, and
	// `sz` parameters must all be included, unlike the regular
	// Put method, which allows post-determined location and size.
	PutChunked(ctx context.Context, resolve types.Resolver, dir, address string, sz uint64) (string, string, error)

	// Remove an item
	Remove(ctx context.Context, dir, address string) error

	// Flush the NFS cache while waiting for an
	// item to appear
	Flush(ctx context.Context, dir, address string)

	// Enumerate all items in storage
	Enumerate(ctx context.Context) ([]types.StoredItem, error)

	// Move an item from one storage system to another
	Move(ctx context.Context, dir, address string, server StorageServer) error

	// Copy an item from one storage system to another
	Copy(ctx context.Context, dir, address string, server StorageServer) error

	// Locate returns the storage location for a given dir, address
	Locate(dir, address string) string

	// Base returns the base storage server in case the server is wrapped
	Base() StorageServer
}

type Logger interface {
	Debugf(msg string, args ...interface{})
}

type MoveCopyFn func(dir, address string, server StorageServer) error

type CacheStore interface {
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
	Bucket             string // S3 bucket to use
	Prefix             string // prefix to prepend to item addresses
	Profile            string // AWS profile to use
	Region             string // AWS regions to use
	Endpoint           string // S3 service endpoint to use
	KeyID              string // the AWS KMS ID to use for client-side S3 encryption
	SkipValidation     bool   // skip the validation of the S3 configuration
	DisableSSL         bool   // disable SSL for S3
	S3ForcePathStyle   bool   // force path style for URLs for S3 objects
	EnableSharedConfig bool   // overrides the AWS_SKD_LOAD_CONFIG env var and enables shared config functionality
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
