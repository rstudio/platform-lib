package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

const AmzUnencryptedContentLengthHeader = "X-Amz-Unencrypted-Content-Length"

type moveOrCopyFn func(bucket, key, newBucket, newKey string) (*s3.CopyObjectOutput, error)

type StorageServer struct {
	bucket  string
	prefix  string
	svc     S3Wrapper
	move    moveOrCopyFn
	copy    moveOrCopyFn
	chunker rsstorage.ChunkUtils
}

type StorageServerArgs struct {
	Bucket    string
	Prefix    string
	Svc       S3Wrapper
	ChunkSize uint64
	Waiter    rsstorage.ChunkWaiter
	Notifier  rsstorage.ChunkNotifier
}

func NewStorageServer(args StorageServerArgs) rsstorage.StorageServer {
	s3s := &StorageServer{
		bucket: args.Bucket,
		prefix: args.Prefix,
		svc:    args.Svc,
		move:   args.Svc.MoveObject,
		copy:   args.Svc.CopyObject,
	}
	return &StorageServer{
		bucket: args.Bucket,
		prefix: args.Prefix,
		svc:    args.Svc,
		move:   args.Svc.MoveObject,
		copy:   args.Svc.CopyObject,
		chunker: &internal.DefaultChunkUtils{
			ChunkSize:   args.ChunkSize,
			Server:      s3s,
			Waiter:      args.Waiter,
			Notifier:    args.Notifier,
			PollTimeout: rsstorage.DefaultChunkPollTimeout,
			MaxAttempts: rsstorage.DefaultMaxChunkAttempts,
		},
	}
}

// Validate performs S3 actions to ensure that the s3:GetObject, s3:PutObject, and s3:DeleteObject permissions are
// configured correctly. Note: This doesn't validate all the permissions (e.g. s3:AbortMultipartUpload), but it should
// be enough to confirm that the storage class is working.
func (s *StorageServer) Validate() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	file := "validate." + internal.RandomString(10) + ".txt"
	uploadAddr := internal.NotEmptyJoin([]string{s.prefix, "temp", file}, "/")
	_, err := s.svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(uploadAddr),
		Body:   strings.NewReader("test"),
	}, ctx)
	if err != nil {
		return err
	}

	_, err = s.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(uploadAddr),
	})
	if err != nil {
		return err
	}

	_, err = s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(uploadAddr),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageServer) Check(dir, address string) (bool, *types.ChunksInfo, int64, time.Time, error) {
	var chunked bool
	var contentLength int64
	addr := internal.NotEmptyJoin([]string{s.prefix, dir, address}, "/")
	infoAddr := filepath.Join(addr, "info.json")
	resp, err := s.svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(addr)})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey || aerr.Code() == "NotFound" {

				// If the item was not found, check to see if it was chunked. If so, the original address
				// will be a directory containing an `info.json` file.
				resp, err = s.svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(infoAddr)})
				if err != nil {
					if aerr, ok := err.(awserr.Error); ok {
						if aerr.Code() == s3.ErrCodeNoSuchKey || aerr.Code() == "NotFound" {
							return false, nil, 0, time.Time{}, nil
						}
					}
				} else {
					chunked = true
				}
			}
		}

		// If `err` is still set, we know that neither the standard nor the chunked request
		// were successful
		if err != nil {
			return false, nil, 0, time.Time{}, err
		}
	}

	if chunked {
		// For chunked assets, download the `info.json`, decode it, and use it to craft a response.
		resp, err := s.svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(infoAddr)})
		if err != nil {
			return false, nil, 0, time.Time{}, err
		}
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		info := types.ChunksInfo{}
		err = dec.Decode(&info)
		if err != nil {
			return false, nil, 0, time.Time{}, err
		}
		return true, &info, int64(info.FileSize), info.ModTime, nil
	} else {
		// Check some headers for the unencrypted content length for KMS encrypted objects.
		if s.svc.KmsEncrypted() {
			if cl, ok := resp.Metadata[AmzUnencryptedContentLengthHeader]; ok {
				contentLength, _ = strconv.ParseInt(*cl, 10, 64)
			}
		} else {
			contentLength = *resp.ContentLength
		}

		// For standard assets, the HeadObject response has the information we need.
		return true, nil, contentLength, *resp.LastModified, nil
	}
}

func (s *StorageServer) Dir() string {
	return "s3:" + s.bucket
}

func (s *StorageServer) Type() types.StorageType {
	return rsstorage.StorageTypeS3
}

func (s *StorageServer) CalculateUsage() (types.Usage, error) {
	// Currently unused.
	return types.Usage{}, fmt.Errorf("server s3server.StorageServer does not implement CalculateUsage")
}

func (s *StorageServer) Get(dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, bool, error) {
	var chunked bool
	var contentLength int64
	addr := internal.NotEmptyJoin([]string{s.prefix, dir, address}, "/")
	infoAddr := filepath.Join(addr, "info.json")
	resp, err := s.svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(addr)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey || aerr.Code() == "NotFound" {

				// The item was not found, so check to see if it was chunked.
				_, err = s.svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(infoAddr)})
				if err != nil {
					if aerr, ok := err.(awserr.Error); ok {
						if aerr.Code() == s3.ErrCodeNoSuchKey || aerr.Code() == "NotFound" {
							return nil, nil, 0, time.Time{}, false, nil
						}
					}
				} else {
					chunked = true
				}
			}
		}

		// If `err` is still set, we know that neither the standard nor the chunked request
		// were successful
		if err != nil {
			return nil, nil, 0, time.Time{}, false, err
		}
	}

	if chunked {
		// For chunked assets, use the chunk utils to read the chunks sequentially
		r, c, sz, mod, err := s.chunker.ReadChunked(dir, address)
		if err != nil {
			return nil, nil, 0, time.Time{}, false, fmt.Errorf("error reading chunked directory files for %s: %s", address, err)
		}
		return r, c, sz, mod, true, nil
	} else {
		// Check some headers for the unencrypted content length for KMS encrypted objects.
		if s.svc.KmsEncrypted() {
			if cl, ok := resp.Metadata[AmzUnencryptedContentLengthHeader]; ok {
				contentLength, _ = strconv.ParseInt(*cl, 10, 64)
			}
		} else {
			contentLength = *resp.ContentLength
		}

		// For standard assets, the GetObject response has the information we need.
		return resp.Body, nil, contentLength, *resp.LastModified, true, nil
	}
}

func (s *StorageServer) Flush(dir, address string) {
}

func (s *StorageServer) Put(resolve types.Resolver, dir, address string) (string, string, error) {
	// Pipe the results so we can resolve the item and simultaneously
	// write it to S3
	r, w := io.Pipe()

	// This enables us to cancel uploads when resolution fails
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Resolve (read) the item using the piped writer
	var wdir string
	var waddress string
	var resolverErr error
	go func() {
		// Make sure an EOF gets sent to the pipe writer. Without this, we end
		// up hanging forever while the uploader reads from the pipe.
		wdir, waddress, resolverErr = resolve(w)
		if resolverErr != nil {
			_ = w.CloseWithError(resolverErr)
			return
		}
		// Close with EOF if successful
		_ = w.CloseWithError(io.EOF)
	}()

	// Upload to a temporary S3 address using the piped reader
	uploadAddr := internal.NotEmptyJoin([]string{s.prefix, "temp", uuid.New().String()}, "/")
	_, err := s.svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(uploadAddr),
		Body:   r,
	}, ctx)

	if err != nil {
		cancel()
		// If the upload failed because of a resolver error, return the original error
		// instead of a generic upload error
		if resolverErr != nil {
			return "", "", resolverErr
		}
		return "", "", err
	}

	// If no dir and address were provided, use the ones optionally returned
	// from the resolver function
	if dir == "" && address == "" {
		dir = wdir
		address = waddress
	}

	var permanentAddr string
	permanentAddr = internal.NotEmptyJoin([]string{s.prefix, dir, address}, "/")

	// We need to copy the item on S3 to the new address
	_, err = s.svc.MoveObject(s.bucket, uploadAddr, s.bucket, permanentAddr)
	if err != nil {
		return "", "", err
	}

	return dir, address, nil
}

func (s *StorageServer) PutChunked(resolve types.Resolver, dir, address string, sz uint64) (string, string, error) {
	if address == "" {
		return "", "", fmt.Errorf("cache only supports pre-addressed chunked put commands")
	}
	if sz == 0 {
		return "", "", fmt.Errorf("cache only supports pre-sized chunked put commands")
	}
	err := s.chunker.WriteChunked(dir, address, sz, resolve)
	if err != nil {
		return "", "", err
	}

	return dir, address, nil
}

func (s *StorageServer) Remove(dir, address string) error {
	ok, chunked, _, _, err := s.Check(dir, address)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	if chunked != nil {
		// Delete chunks
		for i := uint64(1); i <= chunked.NumChunks; i++ {
			chunk := fmt.Sprintf("%08d", i)
			addr := internal.NotEmptyJoin([]string{s.prefix, dir, address, chunk}, "/")
			_, err = s.svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(addr)})
			if err != nil {
				return err
			}
		}
		// Delete "info.json"
		addr := internal.NotEmptyJoin([]string{s.prefix, dir, address, "info.json"}, "/")
		_, err = s.svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(addr)})
	} else {
		addr := internal.NotEmptyJoin([]string{s.prefix, dir, address}, "/")
		_, err = s.svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(addr)})
	}

	return err
}

func (s *StorageServer) Enumerate() ([]types.StoredItem, error) {
	items := make([]types.StoredItem, 0)
	s3Objects, err := s.svc.ListObjects(s.bucket, s.prefix)
	if err != nil {
		return nil, err
	}

	for _, path := range s3Objects {
		dir := filepath.Dir(path)
		if dir == "." {
			dir = ""
		}
		items = append(items, types.StoredItem{
			Dir:     dir,
			Address: filepath.Base(path),
		})
	}

	return internal.FilterChunks(items), nil
}

func (s *StorageServer) moveOrCopy(dir, address string, server rsstorage.StorageServer, fn moveOrCopyFn) error {
	// Get a list of parts to copy. This works for either single-part or chunked assets
	parts, err := s.parts(dir, address)
	if err != nil {
		return err
	}

	for _, part := range parts {

		// Determine the path to the source item
		sourcePath := internal.NotEmptyJoin([]string{s.prefix, part.Dir, part.Address}, "/")

		// Determine the path to which we will move/copy the item
		destUrl, err := url.Parse(server.Locate(part.Dir, part.Address))
		if err != nil {
			return err
		}

		// Split URL into bucket and path
		destBucket := destUrl.Host
		destPath := strings.TrimPrefix(destUrl.Path, "/")

		// Attempt move or copy operation
		_, err = fn(s.bucket, sourcePath, destBucket, destPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StorageServer) parts(dir, address string) ([]rsstorage.CopyPart, error) {
	ok, chunked, _, _, err := s.Check(dir, address)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("the S3 object with dir=%s and address=%s to copy does not exist", dir, address)
	}
	parts := make([]rsstorage.CopyPart, 0)
	if chunked != nil {
		if !chunked.Complete {
			return nil, fmt.Errorf("the S3 chunked object with dir=%s and address=%s to copy is incomplete", dir, address)
		}
		chunkDir := filepath.Join(dir, address)
		parts = append(parts, rsstorage.NewCopyPart(chunkDir, "info.json"))
		for i := 1; i <= int(chunked.NumChunks); i++ {
			chunkName := fmt.Sprintf("%08d", i)
			parts = append(parts, rsstorage.NewCopyPart(chunkDir, chunkName))
		}
		return parts, nil
	} else {
		return []rsstorage.CopyPart{rsstorage.NewCopyPart(dir, address)}, nil
	}
}

func (s *StorageServer) Move(dir, address string, server rsstorage.StorageServer) error {
	copy := true
	switch server.(type) {
	case *StorageServer:
		// Attempt move
		err := s.moveOrCopy(dir, address, server, s.move)
		if err == nil {
			copy = false
		}
	default:
		// Don't do anything. Just copy
	}

	// Copy the file
	if copy {
		err := s.Copy(dir, address, server)
		if err != nil {
			return err
		}

		// Then, remove the file
		err = s.Remove(dir, address)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageServer) Copy(dir, address string, server rsstorage.StorageServer) error {
	s3Copy := true
	switch server.(type) {
	case *StorageServer:
		// Attempt copy
		err := s.moveOrCopy(dir, address, server, s.copy)
		if err == nil {
			s3Copy = false
		}
	default:
		// Don't do anything. Use a normal copy
	}

	// Normal copy
	if s3Copy {
		f, chunked, sz, _, ok, err := s.Get(dir, address)
		if err == nil && !ok {
			return fmt.Errorf("the S3 object with dir=%s and address=%s to copy does not exist", dir, address)
		} else if err != nil {
			return err
		}

		install := func(file io.ReadCloser) types.Resolver {
			return func(writer io.Writer) (string, string, error) {
				_, err := io.Copy(writer, file)
				return "", "", err
			}
		}

		// Use the server Base() in case the server is wrapped, e.g., `Metadatarsstorage.StorageServer`
		if chunked != nil {
			_, _, err = server.Base().PutChunked(install(f), dir, address, uint64(sz))
		} else {
			_, _, err = server.Base().Put(install(f), dir, address)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageServer) Locate(dir, address string) string {
	addr := internal.NotEmptyJoin([]string{s.prefix, dir, address}, "/")
	url := fmt.Sprintf("s3://%s/%s", s.bucket, addr)
	return url
}

func (s *StorageServer) Base() rsstorage.StorageServer {
	return s
}
