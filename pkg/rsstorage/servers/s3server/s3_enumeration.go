package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	nextMarkerChan := make(chan string, 100)
	nextMarkerChan <- ""
	defer close(nextMarkerChan)

	binaryMeta := make([]string, 0)
	binaryL := sync.Mutex{}

	wg := sync.WaitGroup{}
	waitCh := make(chan struct{})
	wg.Add(1)

	var ops uint64
	var total uint64

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If recursive is not true, include a delimiter so we only list the contents
	// of the directory indicated by `s3Prefix`. Otherwise, leave the delimiter nil
	// so we list everything recursively.
	var delimiter *string
	if !recursive {
		delimiter = aws.String("/")
	}

	go func() {
		for i := 0; i < concurrency; i++ {
			go func() {
				for nextMarker := range nextMarkerChan {
					wg.Add(1)

					query := &s3.ListObjectsInput{
						Bucket:    aws.String(bucket),
						Prefix:    aws.String(s3Prefix),
						Delimiter: delimiter,
					}

					if nextMarker != "" {
						query.Marker = &nextMarker
					}

					resp, err := a.s3Client.ListObjects(ctx, query)
					if err != nil {
						errCh <- fmt.Errorf("something went wrong listing objects: %s", err)
						return
					}

					nm := ""

					if resp.NextMarker != nil {
						nm = *resp.NextMarker
						nextMarkerChan <- nm
					}

					// When there are no contents, we need to return
					// early.
					if len(resp.Contents) == 0 {
						wg.Done()
						// TODO: `nm` may always be blank when there are no
						// contents, so this conditional may be unnecessary.
						if nm == "" {
							wg.Done()
						}
						return
					}

					bm := getObjectsAll(resp, s3Prefix, reg)

					binaryL.Lock()
					binaryMeta = append(binaryMeta, bm...)
					binaryL.Unlock()

					wg.Done()
					atomic.AddUint64(&ops, uint64(len(bm)))
					if ops > 1000 {
						atomic.AddUint64(&total, atomic.LoadUint64(&ops))
						slog.Info("Parsed S3 files", "prefix", s3Prefix, "fileCount", atomic.LoadUint64(&total))
						atomic.SwapUint64(&ops, 0)
					}

					if nm == "" {
						wg.Done()
						break
					}
				}
			}()
		}

		wg.Wait()
		close(waitCh)
	}()

	// Block until the wait group is done or we err
	select {
	case <-waitCh:
		return binaryMeta, nil
	case err := <-errCh:
		cancel()
		return nil, err
	}
}

func (a *DefaultAwsOps) BucketObjectsETagMap(
	ctx context.Context,
	bucket, s3Prefix string,
	concurrency int,
	recursive bool,
	reg *regexp.Regexp,
) (map[string]string, error) {

	nextMarkerChan := make(chan string, 100)
	nextMarkerChan <- ""
	defer close(nextMarkerChan)

	binaryMeta := make(map[string]string)
	binaryL := sync.Mutex{}

	wg := sync.WaitGroup{}
	waitCh := make(chan struct{})
	wg.Add(1)

	var ops uint64
	var total uint64

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If recursive is not true, include a delimiter so we only list the contents
	// of the directory indicated by `s3Prefix`. Otherwise, leave the delimiter nil
	// so we list everything recursively.
	var delimiter *string
	if !recursive {
		delimiter = aws.String("/")
	}

	go func() {
		for i := 0; i < concurrency; i++ {
			go func() {
				for nextMarker := range nextMarkerChan {
					wg.Add(1)

					query := &s3.ListObjectsInput{
						Bucket:    aws.String(bucket),
						Prefix:    aws.String(s3Prefix),
						Delimiter: delimiter,
					}

					if nextMarker != "" {
						query.Marker = &nextMarker
					}

					resp, err := a.s3Client.ListObjects(ctx, query)
					if err != nil {
						errCh <- fmt.Errorf("something went wrong listing objects: %s", err)
						return
					}

					nm := ""

					if resp.NextMarker != nil {
						nm = *resp.NextMarker
						nextMarkerChan <- nm
					}

					// When there are no contents, we need to return
					// early.
					if len(resp.Contents) == 0 {
						wg.Done()
						// TODO: `nm` may always be blank when there are no
						// contents, so this conditional may be unnecessary.
						if nm == "" {
							wg.Done()
						}
						return
					}

					bm := getObjectsAllMap(resp, s3Prefix, reg)

					binaryL.Lock()
					for key, val := range bm {
						binaryMeta[key] = val
					}
					binaryL.Unlock()

					wg.Done()
					atomic.AddUint64(&ops, uint64(len(bm)))
					if ops > 1000 {
						atomic.AddUint64(&total, atomic.LoadUint64(&ops))
						slog.Info("Parsed S3 files", "prefix", s3Prefix, "fileCount", atomic.LoadUint64(&total))
						atomic.SwapUint64(&ops, 0)
					}

					if nm == "" {
						wg.Done()
						break
					}
				}
			}()
		}

		wg.Wait()
		close(waitCh)
	}()

	// Block until the wait group is done or we err
	select {
	case <-waitCh:
		return binaryMeta, nil
	case err := <-errCh:
		cancel()
		return nil, err
	}
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
