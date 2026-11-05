package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strings"

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
	var results []string

	err := a.enumerateBucket(ctx, bucket, s3Prefix, concurrency, recursive, func(resp *s3.ListObjectsOutput) {
		results = append(results, getObjectsAll(resp, s3Prefix, reg)...)
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

	err := a.enumerateBucket(ctx, bucket, s3Prefix, concurrency, recursive, func(resp *s3.ListObjectsOutput) {
		maps.Copy(results, getObjectsAllMap(resp, s3Prefix, reg))
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// enumerateBucket handles paginated listing of S3 objects. The callback is
// invoked for each page of results that has contents.
//
// Pages are fetched one at a time: each request needs the marker returned by
// the previous page, so the listing cannot run in parallel. The concurrency
// argument is kept for API compatibility and is not used.
func (a *DefaultAwsOps) enumerateBucket(
	ctx context.Context,
	bucket, s3Prefix string,
	_ int,
	recursive bool,
	callback func(*s3.ListObjectsOutput),
) error {
	// Delimiter for non-recursive listing
	var delimiter *string
	if !recursive {
		delimiter = aws.String("/")
	}

	var totalObjects int64
	var marker *string

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		resp, err := a.s3Client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(s3Prefix),
			Delimiter: delimiter,
			Marker:    marker,
		})
		if err != nil {
			return fmt.Errorf("error listing objects: %w", err)
		}

		if n := int64(len(resp.Contents)); n > 0 {
			callback(resp)

			totalObjects += n
			if totalObjects%1000 < n {
				slog.Info("Parsed S3 files", "prefix", s3Prefix, "fileCount", totalObjects)
			}
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			return nil
		}

		// S3 returns NextMarker only when a delimiter is set. Otherwise, the
		// last key of the page is the marker for the next page.
		switch {
		case resp.NextMarker != nil:
			marker = resp.NextMarker
		case len(resp.Contents) > 0:
			marker = resp.Contents[len(resp.Contents)-1].Key
		default:
			return nil
		}
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
