# `/pkg/rsstorage/servers/s3server`

## Description

A storage server implementation for Amazon AWS S3 storage. Includes
an `S3Wrapper` interface with a default implementation for easier use. Also
includes `s3_copier.go` to provide support for moving/copying files within
S3 without transferring the bytes through the client.
