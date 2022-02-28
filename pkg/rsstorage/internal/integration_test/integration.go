package integrationtest

// Copyright (C) 2022 by RStudio, PBC

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server"
)

// GetStorageServer wraps getStorageServerAttempt, fatally erring if something goes wrong
func GetStorageServer(cfg *rsstorage.Config, class string, destination string, waiter rsstorage.ChunkWaiter, notifier rsstorage.ChunkNotifier, pool *pgxpool.Pool, cstore rsstorage.CacheStore, debugLogger rsstorage.DebugLogger) (rsstorage.StorageServer, error) {
	server, err := getStorageServerAttempt(cfg, class, destination, waiter, notifier, pool, debugLogger)
	if err != nil {
		return nil, err
	}

	return rsstorage.NewMetadataStorageServer(class, server, cstore), nil
}

// getStorageServerAttempt creates storage services generically
func getStorageServerAttempt(cfg *rsstorage.Config, class string, destination string, waiter rsstorage.ChunkWaiter, notifier rsstorage.ChunkNotifier, pool *pgxpool.Pool, debugLogger rsstorage.DebugLogger) (rsstorage.StorageServer, error) {
	var server rsstorage.StorageServer
	switch destination {
	case "file":
		if cfg.File == nil {
			return nil, fmt.Errorf("Missing [FileStorage \"%s\"] configuration section", class)
		}
		//todo bioc: configurable size here
		server = file.NewFileStorageServer(cfg.File.Location, cfg.ChunkSizeBytes, waiter, notifier, class, debugLogger, cfg.CacheTimeout, 30*time.Second)
	case "s3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("Missing [S3Storage \"%s\"] configuration section", class)
		}
		s3Service, err := s3server.NewS3Wrapper(cfg.S3, "")
		if err != nil {
			return nil, fmt.Errorf("Error starting S3 session for '%s': %s", class, err)
		}

		server = s3server.NewS3StorageServer(cfg.S3.Bucket, cfg.S3.Prefix, s3Service, cfg.ChunkSizeBytes, waiter, notifier)

		if cfg.S3.SkipValidation {
			break
		}

		s3, _ := server.(*s3server.StorageServer)
		err = s3.Validate()
		if err != nil {
			return nil, fmt.Errorf("Error validating S3 session for '%s': %s", class, err)
		}
	case "postgres":
		server = postgres.NewPgStorageServer(class, cfg.ChunkSizeBytes, waiter, notifier, pool, debugLogger)
	default:
		return nil, fmt.Errorf("Invalid destination '%s' for '%s'", destination, class)
	}

	return server, nil
}
