package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

type AwsOps interface {
	BucketDirs(ctx context.Context, bucket, s3Prefix string) ([]string, error)
	BucketObjects(ctx context.Context, bucket, s3Prefix string, concurrency int, recursive bool, reg *regexp.Regexp) ([]string, error)
	BucketObjectsETagMap(ctx context.Context, bucket, s3Prefix string, concurrency int, recursive bool, reg *regexp.Regexp) (map[string]string, error)
}

type DefaultAwsOps struct {
	s3Client *s3.Client
}

func NewAwsOps(client *s3.Client) *DefaultAwsOps {
	return &DefaultAwsOps{s3Client: client}
}

func (a *DefaultAwsOps) BucketDirs(ctx context.Context, bucket, s3Prefix string) ([]string, error) {
	delimiter := "/"

	query := &s3.ListObjectsInput{
		Bucket:    &bucket,
		Prefix:    &s3Prefix,
		Delimiter: &delimiter,
	}

	resp, err := a.s3Client.ListObjects(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("something went wrong listing objects: %s", err)
	}

	results := make([]string, 0)
	for _, key := range resp.CommonPrefixes {
		results = append(results, strings.TrimSuffix(strings.TrimPrefix(*key.Prefix, s3Prefix), "/"))
	}

	return results, nil
}

func (a *DefaultAwsOps) BucketObjects(
	ctx context.Context,
	bucket, s3Prefix string,
	concurrency int,
	recursive bool,
	reg *regexp.Regexp,
) ([]string, error) {
	var results []string
	var mu sync.Mutex

	err := a.enumerateBucket(ctx, bucket, s3Prefix, concurrency, recursive, func(resp *s3.ListObjectsOutput) {
		bm := getObjectsAll(resp, s3Prefix, reg)
		mu.Lock()
		results = append(results, bm...)
		mu.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (a *DefaultAwsOps) BucketObjectsETagMap(
	ctx context.Context,
	bucket, s3Prefix string,
	concurrency int,
	recursive bool,
	reg *regexp.Regexp,
) (map[string]string, error) {
	results := make(map[string]string)
	var mu sync.Mutex

	err := a.enumerateBucket(ctx, bucket, s3Prefix, concurrency, recursive, func(resp *s3.ListObjectsOutput) {
		bm := getObjectsAllMap(resp, s3Prefix, reg)
		mu.Lock()
		for key, val := range bm {
			results[key] = val
		}
		mu.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// enumerateBucket handles paginated listing of S3 objects with concurrent workers.
// The callback is invoked for each page of results.
func (a *DefaultAwsOps) enumerateBucket(
	ctx context.Context,
	bucket, s3Prefix string,
	concurrency int,
	recursive bool,
	callback func(*s3.ListObjectsOutput),
) error {
	// Channel for markers to process. Buffer allows some lookahead.
	markers := make(chan string, concurrency*2)

	// Track in-flight work to know when we're done
	var pending sync.WaitGroup

	// Use errgroup for proper goroutine lifecycle management
	g, ctx := errgroup.WithContext(ctx)

	// Delimiter for non-recursive listing
	var delimiter *string
	if !recursive {
		delimiter = aws.String("/")
	}

	// Progress tracking
	var totalObjects int64
	var totalMu sync.Mutex

	// Start with empty marker
	pending.Add(1)
	markers <- ""

	// Spawn worker goroutines
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case marker, ok := <-markers:
					if !ok {
						return nil
					}

					query := &s3.ListObjectsInput{
						Bucket:    aws.String(bucket),
						Prefix:    aws.String(s3Prefix),
						Delimiter: delimiter,
					}
					if marker != "" {
						query.Marker = &marker
					}

					resp, err := a.s3Client.ListObjects(ctx, query)
					if err != nil {
						pending.Done()
						return fmt.Errorf("error listing objects: %w", err)
					}

					// Process results
					if len(resp.Contents) > 0 {
						callback(resp)

						// Progress logging
						totalMu.Lock()
						totalObjects += int64(len(resp.Contents))
						if totalObjects%1000 < int64(len(resp.Contents)) {
							slog.Info("Parsed S3 files", "prefix", s3Prefix, "fileCount", totalObjects)
						}
						totalMu.Unlock()
					}

					// Queue next page if there is one
					if resp.IsTruncated != nil && *resp.IsTruncated && resp.NextMarker != nil {
						pending.Add(1)
						select {
						case markers <- *resp.NextMarker:
						case <-ctx.Done():
							pending.Done()
							pending.Done()
							return ctx.Err()
						}
					}

					pending.Done()
				}
			}
		})
	}

	// Close the markers channel when all work is done
	go func() {
		pending.Wait()
		close(markers)
	}()

	return g.Wait()
}

var BinaryReg = regexp.MustCompile(`(.+)(\.tar\.gz|\.zip)$`)

func getObjectsAll(bucketObjectsList *s3.ListObjectsOutput, s3Prefix string, reg *regexp.Regexp) []string {
	binaryMeta := make([]string, 0)

	for _, key := range bucketObjectsList.Contents {

		if reg != nil {
			if s := reg.FindStringSubmatch(*key.Key); len(s) > 1 {
				binaryMeta = append(binaryMeta, strings.TrimPrefix(s[1], s3Prefix))
			}
		} else {
			binaryMeta = append(binaryMeta, strings.TrimPrefix(*key.Key, s3Prefix))
		}

	}

	return binaryMeta
}

func getObjectsAllMap(bucketObjectsList *s3.ListObjectsOutput, s3Prefix string, reg *regexp.Regexp) map[string]string {
	binaryMeta := make(map[string]string)

	for _, key := range bucketObjectsList.Contents {
		if reg != nil {
			if s := reg.FindStringSubmatch(*key.Key); len(s) > 1 {
				binaryMeta[strings.TrimPrefix(s[1], s3Prefix)] = *key.ETag
			}
		} else {
			binaryMeta[strings.TrimPrefix(*key.Key, s3Prefix)] = *key.ETag
		}
	}

	return binaryMeta
}
