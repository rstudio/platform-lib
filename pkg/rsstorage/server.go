package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jackc/pgx/v4/pgxpool"

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
	ConnPool() *pgxpool.Pool
}

// Simply wraps getStorageServerAttempt, fatally erring if something goes wrong
func GetStorageServer(cfg *Config, class string, destination string, waiter ChunkWaiter, notifier ChunkNotifier, cstore PersistentStorageStore, debugLogger DebugLogger) (PersistentStorageServer, error) {
	server, err := getStorageServerAttempt(cfg, class, destination, waiter, notifier, cstore, debugLogger)
	if err != nil {
		return nil, err
	}

	return NewMetadataPersistentStorageServer(class, server, cstore), nil
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

// Lets us create persistent storage services generically
func getStorageServerAttempt(cfg *Config, class string, destination string, waiter ChunkWaiter, notifier ChunkNotifier, cstore PersistentStorageStore, debugLogger DebugLogger) (PersistentStorageServer, error) {
	var server PersistentStorageServer
	switch destination {
	case "file":
		if cfg.File == nil {
			return nil, fmt.Errorf("Missing [FileStorage \"%s\"] configuration section", class)
		}
		//todo bioc: configurable size here
		server = NewFileStorageServer(cfg.File.Location, cfg.ChunkSizeBytes, waiter, notifier, class, debugLogger, cfg.CacheTimeout)
	case "s3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("Missing [S3Storage \"%s\"] configuration section", class)
		}
		s3Service, err := newS3Service(cfg.S3)
		if err != nil {
			return nil, fmt.Errorf("Error starting S3 session for '%s': %s", class, err)
		}

		server = NewS3StorageServer(cfg.S3.Bucket, cfg.S3.Prefix, s3Service, cfg.ChunkSizeBytes, waiter, notifier)

		if cfg.S3.SkipValidation {
			break
		}

		s3, _ := server.(*S3StorageServer)
		err = s3.Validate()
		if err != nil {
			return nil, fmt.Errorf("Error validating S3 session for '%s': %s", class, err)
		}
	case "postgres":
		server = NewPgServer(class, cfg.ChunkSizeBytes, waiter, notifier, cstore, debugLogger)
	default:
		return nil, fmt.Errorf("Invalid destination '%s' for '%s'", destination, class)
	}

	return server, nil
}

func getS3Options(configInput *ConfigS3) session.Options {
	// By default decide whether to use shared config (e.g., `~/.aws/config`) from the
	// environment. If the environment contains a truthy value for AWS_SDK_LOAD_CONFIG,
	// then we'll use the shared config automatically. However, if
	// `SharedConfigEnable == true`, then we forcefully enable it.
	sharedConfig := session.SharedConfigStateFromEnv
	if configInput.EnableSharedConfig {
		sharedConfig = session.SharedConfigEnable
	}

	// Optionally support a configured region
	s3config := aws.Config{}
	if configInput.Region != "" {
		s3config.Region = aws.String(configInput.Region)
	}

	// Optionally support a configured endpoint
	if configInput.Endpoint != "" {
		s3config.Endpoint = aws.String(configInput.Endpoint)
	}

	s3config.DisableSSL = aws.Bool(configInput.DisableSSL)
	s3config.S3ForcePathStyle = aws.Bool(configInput.S3ForcePathStyle)

	return session.Options{
		Config:            s3config,
		Profile:           configInput.Profile,
		SharedConfigState: sharedConfig,
	}
}

func newS3Service(configInput *ConfigS3) (S3Service, error) {
	// Create a session
	options := getS3Options(configInput)
	sess, err := session.NewSessionWithOptions(options)
	if err != nil {
		return nil, fmt.Errorf("Error starting AWS session: %s", err)
	}

	return NewS3Service(sess), nil
}

type CopyPart struct {
	Dir     string
	Address string
}

func newCopyPart(dir, address string) CopyPart {
	return CopyPart{
		Dir:     dir,
		Address: address,
	}
}
