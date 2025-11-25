package integrationtest

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rstudio/platform-lib/v3/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/servers/postgres"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/servers/s3server"
)

// GetStorageServer wraps getStorageServerAttempt, fatally erring if something goes wrong
func GetStorageServer(cfg *rsstorage.Config, class string, destination string, waiter rsstorage.ChunkWaiter, notifier rsstorage.ChunkNotifier, pool *pgxpool.Pool, cstore rsstorage.CacheStore) (rsstorage.StorageServer, error) {
	server, err := getStorageServerAttempt(cfg, class, destination, waiter, notifier, pool)
	if err != nil {
		return nil, err
	}

	return rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
		Name:   class,
		Server: server,
		Store:  cstore,
	}), nil
}

// getStorageServerAttempt creates storage services generically
func getStorageServerAttempt(
	cfg *rsstorage.Config,
	class string,
	destination string,
	waiter rsstorage.ChunkWaiter,
	notifier rsstorage.ChunkNotifier,
	pool *pgxpool.Pool,
) (rsstorage.StorageServer, error) {
	ctx := context.Background()
	var server rsstorage.StorageServer
	switch destination {
	case "file":
		if cfg.File == nil {
			return nil, fmt.Errorf("Missing [FileStorage \"%s\"] configuration section", class)
		}
		//todo bioc: configurable size here
		server = file.NewStorageServer(
			file.StorageServerArgs{
				Dir:          cfg.File.Location,
				ChunkSize:    cfg.ChunkSizeBytes,
				Waiter:       waiter,
				Notifier:     notifier,
				Class:        class,
				CacheTimeout: cfg.CacheTimeout,
				WalkTimeout:  30 * time.Second,
			},
		)
	case "s3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("Missing [S3Storage \"%s\"] configuration section", class)
		}
		s3Opts := s3.Options{
			Region:       cfg.S3.Region,
			UsePathStyle: cfg.S3.S3ForcePathStyle,
			//RetryMaxAttempts: 1,
		}
		if cfg.S3.Endpoint != "" {
			s3Opts.BaseEndpoint = &cfg.S3.Endpoint
		}
		if cfg.S3.DisableSSL {
			s3Opts.EndpointOptions = s3.EndpointResolverOptions{DisableHTTPS: cfg.S3.DisableSSL}
		}

		s3Service, err := s3server.NewS3Wrapper(s3Opts)
		if err != nil {
			return nil, fmt.Errorf("Error starting S3 session for '%s': %s", class, err)
		}

		server = s3server.NewStorageServer(s3server.StorageServerArgs{
			Bucket:    cfg.S3.Bucket,
			Prefix:    cfg.S3.Prefix,
			Svc:       s3Service,
			ChunkSize: cfg.ChunkSizeBytes,
			Waiter:    waiter,
			Notifier:  notifier,
		})

		if cfg.S3.SkipValidation {
			break
		}

		s3, _ := server.(*s3server.StorageServer)
		err = s3.Validate(ctx)
		if err != nil {
			return nil, fmt.Errorf("Error validating S3 session for '%s': %s", class, err)
		}
	case "postgres":
		server = postgres.NewStorageServer(postgres.StorageServerArgs{
			ChunkSize: cfg.ChunkSizeBytes,
			Waiter:    waiter,
			Notifier:  notifier,
			Class:     class,
			Pool:      pool,
		})
	default:
		return nil, fmt.Errorf("Invalid destination '%s' for '%s'", destination, class)
	}

	return server, nil
}
