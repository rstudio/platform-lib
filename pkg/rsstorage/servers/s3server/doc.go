// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package s3server provides an rsstorage.StorageServer that keeps items in an
// Amazon S3 bucket, or in any service that speaks the S3 API.
//
// The server talks to S3 through the S3Wrapper interface. NewS3Wrapper and
// NewEncryptedS3Wrapper build a wrapper from an S3 client that you create and
// configure yourself. The package also holds AwsOps, a set of helpers for
// listing the contents of a bucket.
package s3server
