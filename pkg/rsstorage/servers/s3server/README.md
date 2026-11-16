# `/pkg/rsstorage/servers/s3server`

## Description

A storage server implementation for Amazon AWS S3 storage. Includes
an `S3Wrapper` interface with a default implementation for easier use. The
`S3Wrapper` also provides `CopyObject` and `MoveObject`, which let the server
copy and move items within S3 without sending the bytes through the client.

## Constructing a server

You create and configure the S3 client yourself (region, credentials,
endpoint, and so on). The package wraps it and does not read any configuration.

```go
// client is an *s3.Client from github.com/aws/aws-sdk-go-v2/service/s3.
wrapper, err := s3server.NewS3Wrapper(client)
if err != nil {
	// ...
}

server := s3server.NewStorageServer(s3server.StorageServerArgs{
	Bucket:    "my-bucket",
	Prefix:    "storage",
	Svc:       wrapper,
	ChunkSize: 5 << 20,
	Waiter:    myWaiter,
	Notifier:  myNotifier,
})
```

| Argument | Meaning |
| --- | --- |
| `Bucket` | The bucket to use. It must already exist. The server does not create it. |
| `Prefix` | Optional. A key prefix. An item is stored at `<Prefix>/<dir>/<address>`. Empty parts are left out. |
| `Svc` | An `S3Wrapper`. Use `NewS3Wrapper` or `NewEncryptedS3Wrapper`. |
| `ChunkSize`, `Waiter`, `Notifier` | For chunked storage. See [servers](../README.md). |

`NewS3Wrapper` and `NewEncryptedS3Wrapper` return an error if the client is
`nil`. The client must satisfy the `S3API` interface. The AWS `*s3.Client`
does.

### Checking the configuration

The concrete `*s3server.StorageServer` has a `Validate(ctx)` method. It is not
part of `StorageServer`, so use a type assertion. `Validate` uploads a small
file under `<Prefix>/temp/`, reads its metadata, and deletes it. It confirms
that get, put, and delete work. It does not test every permission the server can
use.

```go
if v, ok := server.(*s3server.StorageServer); ok {
	if err := v.Validate(ctx); err != nil {
		// ...
	}
}
```

### Client-side encryption

For client-side encryption, build an encryption client yourself and pass it to
`NewEncryptedS3Wrapper` instead:

```go
wrapper, err := s3server.NewEncryptedS3Wrapper(encryptionClient)
```

The encryption client must satisfy `S3API`. The wrapper tells the server that
objects are encrypted. For those objects, the server takes the size from the
`x-amz-unencrypted-content-length` metadata of the object instead of from the
S3 content length. If that metadata is missing, the size is 0.

## Behavior

- `Put` streams the data to S3 as an upload to `<Prefix>/temp/<random id>`, then
  moves it to its final key. An `Enumerate` that runs during a `Put` can list
  the temporary object.
- `Enumerate` lists the keys under `Prefix`. The `Dir` it returns includes the
  prefix, so an item it returns cannot be passed straight back to `Get` on a
  server that has a prefix.
- `EnumeratePrefix` lists with `<Prefix>/<prefix>`. The `Dir` it returns is
  relative to `Prefix`, so the item can be passed back to `Get`, `Check`, and
  `Remove`.
- The wrapper follows S3 list pages, so `Enumerate` collects every key before
  it returns.
- `Copy` to another `s3server` server asks S3 to copy each object. If that
  fails, it reads the item and writes it to the destination. `Move` is a copy
  followed by a `Remove` of the source. The source is not removed unless the
  copy succeeds.
- `CalculateUsage` returns an error. `Flush` does nothing.
- `Dir()` returns `s3:` followed by the bucket. `Locate` returns an
  `s3://<bucket>/<key>` URL.

## Listing helpers

`AwsOps` is a separate set of helpers for listing a bucket. `NewAwsOps` takes an
`*s3.Client`. It has `BucketDirs`, which lists the directories under a prefix,
and `BucketObjects` and `BucketObjectsETagMap`, which list object names, or names
and ETags, under a prefix. The prefix is removed from each name. If you pass a
regular expression, only matching keys are returned, and each result is the
first capture group of the match. `BinaryReg` is a ready-made expression that
matches `.tar.gz` and `.zip` names and captures the name without the
extension. The storage server does not
use these helpers. The `concurrency` argument is kept for compatibility and is
not used.
