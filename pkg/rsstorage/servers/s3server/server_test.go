package s3server

// Copyright (C) 2025 by Posit Software, PBC

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fortytw2/leaktest"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/internal/servertest"
	rtypes "github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type S3StorageServerSuite struct{}

var _ = check.Suite(&S3StorageServerSuite{})

type testReadCloser struct {
	io.Reader
}

func (*testReadCloser) Close() error {
	return nil
}

type HeadResponse struct {
	err  error
	head *s3.HeadObjectOutput
}

type GetResponse struct {
	err error
	get *s3.GetObjectOutput
}

type fakeS3 struct {
	head         *s3.HeadObjectOutput
	headErr      error
	headRes      string
	headMap      map[string]HeadResponse
	get          *s3.GetObjectOutput
	getErr       error
	got          string
	getMap       map[string]GetResponse
	delete       *s3.DeleteObjectOutput
	deleteErr    error
	deleted      string
	upload       *s3.UploadPartOutput
	uploadErr    error
	uploaded     string
	bucket       string
	address      string
	moveTo       string
	moveError    error
	copyTo       string
	copyError    error
	list         []string
	listError    error
	bucketIn     *s3.CreateBucketInput
	bucketOut    *s3.CreateBucketOutput
	bucketErr    error
	delBucketErr error
	delBucketIn  *s3.DeleteBucketInput
	delBucketOut *s3.DeleteBucketOutput
}

func (s *fakeS3) KmsEncrypted() bool {
	return false
}

func (s *fakeS3) CreateBucket(ctx context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	if s.bucketErr == nil {
		s.bucketIn = input
	}
	return s.bucketOut, s.bucketErr
}

func (s *fakeS3) DeleteBucket(ctx context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	if s.delBucketErr == nil {
		s.delBucketIn = input
	}
	return s.delBucketOut, s.delBucketErr
}

func (s *fakeS3) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if s.headMap != nil {
		if r, ok := s.headMap[*input.Key]; ok {
			return r.head, r.err
		}
		message := "not found"
		return nil, &types.NoSuchKey{Message: &message}
	}
	if s.headErr == nil {
		s.headRes = *input.Key
	}
	return s.head, s.headErr
}

func (s *fakeS3) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if s.getMap != nil {
		if r, ok := s.getMap[*input.Key]; ok {
			return r.get, r.err
		}
		message := "not found"
		return nil, &types.NoSuchKey{Message: &message}
	}
	if s.getErr == nil {
		s.got = *input.Key
	}
	return s.get, s.getErr
}

func (s *fakeS3) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if s.deleteErr == nil {
		if s.deleted != "" {
			s.deleted += "\n"
		}
		s.deleted += *input.Key
	}
	return s.delete, s.deleteErr
}

func (s *fakeS3) MoveObject(context context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	if s.moveError == nil {
		s.moveTo = newKey
	}
	return nil, s.moveError
}

func (s *fakeS3) CopyObject(ctx context.Context, oldBucket, oldKey, newBucket, newKey string) (*s3.CopyObjectOutput, error) {
	if s.copyError == nil {
		s.copyTo = newKey
	}
	return nil, s.copyError
}

func (s *fakeS3) ListObjects(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	var contents []types.Object

	for _, key := range s.list {
		contents = append(contents, types.Object{Key: &key})
	}

	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, s.listError
}

func (s *fakeS3) Upload(ctx context.Context, input *s3.UploadPartInput, opFns ...func(options *s3.Options)) (*s3.UploadPartOutput, error) {
	if s.uploadErr == nil {
		buf := &bytes.Buffer{}
		_, err := io.Copy(buf, input.Body)
		if err != nil {
			return nil, err
		}
		s.uploaded = buf.String()
		s.bucket = *input.Bucket
		s.address = *input.Key

	}
	// If there's an upload error, copy anyway to ensure that the resolver runs
	_, _ = io.Copy(io.Discard, input.Body)
	return s.upload, s.uploadErr
}

func (s *S3StorageServerSuite) TestNew(c *check.C) {
	svc := &fakeS3{}
	wn := &servertest.DummyWaiterNotifier{}
	server := NewStorageServer(StorageServerArgs{
		Bucket:    "test",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})
	c.Assert(server.(*StorageServer).move, check.NotNil)
	c.Assert(server.(*StorageServer).copy, check.NotNil)
	c.Assert(server.(*StorageServer).chunker, check.NotNil)
	server.(*StorageServer).move = nil
	server.(*StorageServer).copy = nil
	server.(*StorageServer).chunker = nil
	c.Check(server, check.DeepEquals, &StorageServer{
		bucket: "test",
		prefix: "prefix",
		svc:    svc,
	})

	c.Assert(server.Dir(), check.Equals, "s3:test")
	c.Assert(server.Type(), check.Equals, rsstorage.StorageTypeS3)
}

func (s *S3StorageServerSuite) TestCheck(c *check.C) {
	now := time.Now()
	svc := &fakeS3{
		head: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
	}
	server := &StorageServer{
		svc:    svc,
		prefix: "prefix",
	}

	ctx := context.Background()

	// Ok
	ok, chunked, sz, mod, err := server.Check(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(45))
	c.Assert(mod, servertest.TimeEquals, now)
	c.Assert(ok, check.Equals, true)
	c.Assert(svc.headRes, check.Equals, "prefix/dir/address")

	// Error
	svc.headErr = errors.New("head error")
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "head error")
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Missing
	svc.headErr = &types.NoSuchKey{}
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - error in HeadObject
	svc.headErr = nil
	svc.headMap = map[string]HeadResponse{
		"prefix/dir/address/info.json": {
			err:  errors.New("info.json head error"),
			head: &s3.HeadObjectOutput{},
		},
	}
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "info.json head error")
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - error getting info.json
	svc.headMap = map[string]HeadResponse{
		"prefix/dir/address/info.json": {
			head: &s3.HeadObjectOutput{},
		},
	}
	svc.getMap = map[string]GetResponse{
		"prefix/dir/address/info.json": {
			err: errors.New("info.json get error"),
			get: &s3.GetObjectOutput{},
		},
	}
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "info.json get error")
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - error decoding info.json
	output := &testReadCloser{bytes.NewBufferString("bad[{")}
	svc.getMap = map[string]GetResponse{
		"prefix/dir/address/info.json": {
			get: &s3.GetObjectOutput{
				Body: output,
			},
		},
	}
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "invalid character 'b' looking for beginning of value")
	c.Assert(chunked, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - ok
	nowbytes, err := now.MarshalJSON()
	info := []byte(fmt.Sprintf(`{"chunk_size":64,"file_size":3232,"num_chunks":15,"complete":true,"mod_time":%s}`, string(nowbytes)))
	output = &testReadCloser{bytes.NewBuffer(info)}
	svc.getMap = map[string]GetResponse{
		"prefix/dir/address/info.json": {
			get: &s3.GetObjectOutput{
				Body: output,
			},
		},
	}
	ok, chunked, sz, mod, err = server.Check(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(chunked, check.NotNil)
	c.Assert(sz, check.DeepEquals, int64(3232))
	c.Assert(ok, check.Equals, true)
}

func (s *S3StorageServerSuite) TestGet(c *check.C) {
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
	}
	server := &StorageServer{
		svc:    svc,
		prefix: "prefix",
	}
	ctx := context.Background()

	// Ok
	rs, ch, sz, mod, ok, err := server.Get(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(rs, check.FitsTypeOf, &testReadCloser{})
	c.Assert(ch, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(45))
	c.Assert(mod, servertest.TimeEquals, now)
	c.Assert(ok, check.Equals, true)
	c.Assert(svc.got, check.Equals, "prefix/dir/address")

	// Error
	svc.getErr = errors.New("get error")
	rs, _, sz, mod, ok, err = server.Get(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "get error")
	c.Assert(rs, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Missing
	svc.getErr = &types.NoSuchKey{}
	svc.headErr = &types.NoSuchKey{}
	rs, _, sz, mod, ok, err = server.Get(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(rs, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - error in HeadObject
	svc.headErr = nil
	svc.headMap = map[string]HeadResponse{
		"prefix/dir/address/info.json": {
			err:  errors.New("info.json head error"),
			head: &s3.HeadObjectOutput{},
		},
	}
	rs, _, sz, mod, ok, err = server.Get(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "info.json head error")
	c.Assert(rs, check.IsNil)
	c.Assert(sz, check.DeepEquals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - error reading chunks
	svc.headMap = map[string]HeadResponse{
		"prefix/dir/address/info.json": {
			head: &s3.HeadObjectOutput{},
		},
	}
	chunker := &servertest.DummyChunkUtils{
		Read: output,
		ReadCh: &rtypes.ChunksInfo{
			Complete: true,
		},
		ReadSz:  5454,
		ReadMod: now,
		ReadErr: errors.New("chunk read error"),
	}
	server.chunker = chunker
	rs, _, sz, mod, ok, err = server.Get(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "error reading chunked directory files for address: chunk read error")
	c.Assert(rs, check.IsNil)
	c.Assert(sz, check.Equals, int64(0))
	c.Assert(ok, check.Equals, false)

	// Chunked - ok
	chunker.ReadErr = nil
	rs, ch, sz, mod, ok, err = server.Get(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(rs, check.DeepEquals, output)
	c.Assert(ch, check.DeepEquals, &rtypes.ChunksInfo{
		Complete: true,
	})
	c.Assert(sz, check.DeepEquals, int64(5454))
	c.Assert(mod, check.DeepEquals, now)
	c.Assert(ok, check.Equals, true)
}

func (s *S3StorageServerSuite) TestPut(c *check.C) {
	defer leaktest.Check(c)

	input := bytes.NewBufferString("test input")
	resolver := func(w io.Writer) (string, string, error) {
		_, err := io.Copy(w, input)
		return "", "", err
	}
	svc := &fakeS3{
		upload: &s3.UploadPartOutput{},
	}
	server := &StorageServer{
		svc:    svc,
		bucket: "test-bucket",
		prefix: "prefix",
	}
	ctx := context.Background()

	// Ok
	d, a, err := server.Put(ctx, resolver, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(svc.address, check.Matches, "prefix/temp/.*")
	c.Assert(svc.bucket, check.Equals, "test-bucket")
	c.Assert(svc.uploaded, check.Equals, "test input")
	c.Check(d, check.Equals, "dir")
	c.Check(a, check.Equals, "address")

	// Error
	svc.uploadErr = errors.New("upload error")
	_, _, err = server.Put(ctx, resolver, "dir", "address")
	c.Assert(err, check.ErrorMatches, "upload error")

	// Resolve error
	svc.uploadErr = errors.New("upload error (should not be returned)")
	resolver = func(w io.Writer) (string, string, error) {
		return "", "", errors.New("resolver error")
	}
	_, _, err = server.Put(ctx, resolver, "dir", "address")
	c.Assert(err, check.ErrorMatches, "resolver error")
}

func (s *S3StorageServerSuite) TestPutDeferredAddress(c *check.C) {
	defer leaktest.Check(c)

	input := bytes.NewBufferString("test input")
	resolver := func(w io.Writer) (string, string, error) {
		_, err := io.Copy(w, input)
		return "mydir", "deferred_address", err
	}
	svc := &fakeS3{
		upload: &s3.UploadPartOutput{},
	}
	server := &StorageServer{
		svc:    svc,
		bucket: "test-bucket",
		prefix: "prefix",
	}
	ctx := context.Background()

	// Ok
	// the dir and address are obtained from the resolver
	//    and stored on the svc object
	d, a, err := server.Put(ctx, resolver, "", "")
	c.Assert(err, check.IsNil)
	c.Assert(svc.address, check.Not(check.Equals), "prefix/mydir/deferred_address")
	c.Assert(svc.bucket, check.Equals, "test-bucket")
	c.Assert(svc.uploaded, check.Equals, "test input")
	c.Assert(svc.moveTo, check.Equals, "prefix/mydir/deferred_address")
	c.Check(d, check.Equals, "mydir")
	c.Check(a, check.Equals, "deferred_address")

	// Error
	svc.moveError = errors.New("move error")
	_, _, err = server.Put(ctx, resolver, "", "")
	c.Assert(err, check.ErrorMatches, "move error")
}

func (s *S3StorageServerSuite) TestRemove(c *check.C) {
	svc := &fakeS3{
		headMap: map[string]HeadResponse{
			"dir/address": {
				err: errors.New("check error"),
			},
		},
	}
	server := &StorageServer{
		svc: svc,
	}
	ctx := context.Background()

	// Check error
	err := server.Remove(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "check error")
	c.Assert(svc.deleted, check.Equals, "")

	// Asset missing
	svc.headMap = map[string]HeadResponse{
		"dir/address": {
			head: &s3.HeadObjectOutput{
				LastModified:  aws.Time(time.Now()),
				ContentLength: aws.Int64(3232),
			},
		},
		"prefix/dir/address": {
			head: &s3.HeadObjectOutput{
				LastModified:  aws.Time(time.Now()),
				ContentLength: aws.Int64(3232),
			},
		},
	}
	err = server.Remove(ctx, "dir", "address_missing")
	c.Assert(err, check.IsNil)
	c.Assert(svc.deleted, check.Equals, "")

	// No prefix, ok
	err = server.Remove(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(svc.deleted, check.Equals, "dir/address")

	// Ok
	server.prefix = "prefix"
	svc.deleted = ""
	err = server.Remove(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(svc.deleted, check.Equals, "prefix/dir/address")

	// Delete Error
	svc.deleteErr = errors.New("delete error")
	err = server.Remove(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "delete error")

	// Delete Error (chunked)
	svc.headMap = map[string]HeadResponse{
		"prefix/dir/address/info.json": {
			head: &s3.HeadObjectOutput{
				LastModified:  aws.Time(time.Now()),
				ContentLength: aws.Int64(3232),
			},
		},
	}
	now := time.Now()
	nowbytes, err := now.MarshalJSON()
	info := []byte(fmt.Sprintf(`{"chunk_size":64,"file_size":3232,"num_chunks":15,"complete":true,"mod_time":%s}`, string(nowbytes)))
	output := &testReadCloser{bytes.NewBuffer(info)}
	svc.getMap = map[string]GetResponse{
		"prefix/dir/address/info.json": {
			get: &s3.GetObjectOutput{
				Body: output,
			},
		},
	}
	err = server.Remove(ctx, "dir", "address")
	c.Assert(err, check.ErrorMatches, "delete error")

	// Ok (chunked)
	svc.deleteErr = nil
	output = &testReadCloser{bytes.NewBuffer(info)}
	svc.getMap = map[string]GetResponse{
		"prefix/dir/address/info.json": {
			get: &s3.GetObjectOutput{
				Body: output,
			},
		},
	}
	err = server.Remove(ctx, "dir", "address")
	c.Assert(err, check.IsNil)
	c.Assert(svc.deleted, check.Equals, ""+
		"prefix/dir/address\n"+
		"prefix/dir/address/00000001\n"+
		"prefix/dir/address/00000002\n"+
		"prefix/dir/address/00000003\n"+
		"prefix/dir/address/00000004\n"+
		"prefix/dir/address/00000005\n"+
		"prefix/dir/address/00000006\n"+
		"prefix/dir/address/00000007\n"+
		"prefix/dir/address/00000008\n"+
		"prefix/dir/address/00000009\n"+
		"prefix/dir/address/00000010\n"+
		"prefix/dir/address/00000011\n"+
		"prefix/dir/address/00000012\n"+
		"prefix/dir/address/00000013\n"+
		"prefix/dir/address/00000014\n"+
		"prefix/dir/address/00000015\n"+
		"prefix/dir/address/info.json")
}

func (s *S3StorageServerSuite) TestEnumerateError(c *check.C) {
	svc := &fakeS3{
		listError: errors.New("list error"),
	}
	server := &StorageServer{
		svc: svc,
	}
	_, err := server.Enumerate(context.Background())
	c.Assert(err, check.ErrorMatches, "list error")
}

func (s *S3StorageServerSuite) TestEnumerateOk(c *check.C) {
	svc := &fakeS3{
		list: []string{
			"dir/address",
			"dir/address2",
			"nodir",
			"dir/2/address1",
			"dir/address3/00000001",
			"dir/address3/00000002",
			"dir/address3/00000003",
			"dir/address3/info.json",
			"somechunk/00000001",
			"somechunk/info.json",
			"somechunk/00000002",
			"somechunk/00000003",
			"somechunk/00000004",
		},
	}
	server := &StorageServer{
		svc: svc,
	}
	en, err := server.Enumerate(context.Background())
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []rtypes.StoredItem{
		{
			Dir:     "dir",
			Address: "address3",
			Chunked: true,
		},
		{
			Dir:     "",
			Address: "somechunk",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "address",
		},
		{
			Dir:     "dir",
			Address: "address2",
		},
		{
			Dir:     "",
			Address: "nodir",
		},
		{
			Dir:     "dir/2",
			Address: "address1",
		},
	})
}

type fakeMoveOrCopy struct {
	result error
	ops    []string
}

func (f *fakeMoveOrCopy) Operation(ctx context.Context, bucket, path, newBucket, newPath string) (*s3.CopyObjectOutput, error) {
	if f.ops != nil {
		f.ops = append(f.ops, fmt.Sprintf("%s-%s-%s-%s", bucket, path, newBucket, newPath))
	}
	return nil, f.result
}

func (s *S3StorageServerSuite) TestInternalMoveOrCopy(c *check.C) {
	sourceServer := &StorageServer{
		bucket: "bucketA",
		prefix: "prefixA",
		svc: &fakeS3{
			head: &s3.HeadObjectOutput{
				ContentLength: aws.Int64(4242),
				LastModified:  aws.Time(time.Now()),
			},
		},
	}
	destServer := &StorageServer{
		bucket: "bucketB",
		prefix: "prefixB",
	}
	op := &fakeMoveOrCopy{
		result: errors.New("copy error"),
		ops:    make([]string, 0),
	}
	ctx := context.Background()

	err := sourceServer.moveOrCopy(ctx, "dir", "address", destServer, op.Operation)
	c.Assert(err, check.ErrorMatches, "copy error")
	c.Assert(op.ops, check.DeepEquals, []string{"bucketA-prefixA/dir/address-bucketB-prefixB/dir/address"})

	op.ops = make([]string, 0)
	op.result = nil
	destServer.prefix = ""
	err = sourceServer.moveOrCopy(ctx, "", "address", destServer, op.Operation)
	c.Assert(err, check.IsNil)
	c.Assert(op.ops, check.DeepEquals, []string{"bucketA-prefixA/address-bucketB-address"})
}

// Test scenario where we copy from S3 -> S3. The S3-specific copy
// operation succeeds immediately.
func (s *S3StorageServerSuite) TestCopyViaS3(c *check.C) {
	opCopy := &fakeMoveOrCopy{
		ops: make([]string, 0),
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		copy:   opCopy.Operation,
		svc: &fakeS3{
			head: &s3.HeadObjectOutput{
				ContentLength: aws.Int64(4242),
				LastModified:  aws.Time(time.Now()),
			},
		},
	}
	destServer := &StorageServer{
		bucket: "b-test-bucket",
		prefix: "another-place",
	}
	err := sourceServer.Copy(context.Background(), "dir", "address", destServer)
	c.Assert(err, check.IsNil)
	c.Assert(
		opCopy.ops,
		check.DeepEquals,
		[]string{"a-test-bucket-some/prefix/dir/address-b-test-bucket-another-place/dir/address"},
	)
}

// Test scenario where we copy from S3 -> S3. The S3-specific copy
// operation fails, but we succeed by performing a slower `Get` + `Put`
// operation.
func (s *S3StorageServerSuite) TestCopyViaS3Fallback(c *check.C) {
	// The copy operation attempt will fail, but the fallback to `Get` + `Put`
	// should succeed.
	opCopy := &fakeMoveOrCopy{
		ops:    make([]string, 0),
		result: errors.New("no go"),
	}
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
		head: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(4242),
			LastModified:  aws.Time(time.Now()),
		},
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		copy:   opCopy.Operation,
		svc:    svc,
	}
	destServer := &StorageServer{
		bucket: "b-test-bucket",
		prefix: "another-place",
		svc:    svc,
	}
	err := sourceServer.Copy(context.Background(), "dir", "address", destServer)
	c.Assert(err, check.IsNil)
	c.Assert(
		opCopy.ops,
		check.DeepEquals,
		[]string{"a-test-bucket-some/prefix/dir/address-b-test-bucket-another-place/dir/address"},
	)
	c.Assert(svc.uploaded, check.Equals, "test output")
}

// Test scenario where we copy from S3 to a non-S3 storage system.
// This scenario always uses a `Get` + `Put` operation.
func (s *S3StorageServerSuite) TestCopyNoS3(c *check.C) {
	ctx := context.Background()
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		svc:    svc,
	}

	// The destination server is not of type `SetStorageServer`, so we won't attempt
	// an S3-specific copy operation.
	destServer := &rsstorage.DummyStorageServer{
		PutErr: errors.New("put error"),
	}

	// Asset does not exist
	svc.getErr = &types.NoSuchKey{}
	svc.headErr = &types.NoSuchKey{}
	err := sourceServer.Copy(ctx, "dir", "address", destServer)
	c.Assert(err, check.ErrorMatches, "the S3 object .* does not exist")

	// Error getting asset
	svc.getErr = errors.New("some other error")
	err = sourceServer.Copy(ctx, "dir", "address", destServer)
	c.Assert(err, check.ErrorMatches, "some other error")

	// Put error
	svc.getErr = nil
	err = sourceServer.Copy(ctx, "dir", "address", destServer)
	c.Assert(err, check.ErrorMatches, "put error")

	destServer.PutErr = nil
	err = sourceServer.Copy(ctx, "dir", "address", destServer)
	c.Assert(err, check.IsNil)
}

// Test scenario where we move from S3 -> S3. The S3-specific move
// operation succeeds.
func (s *S3StorageServerSuite) TestMoveViaS3(c *check.C) {
	opMove := &fakeMoveOrCopy{
		ops: make([]string, 0),
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		move:   opMove.Operation,
		svc: &fakeS3{
			head: &s3.HeadObjectOutput{
				ContentLength: aws.Int64(4242),
				LastModified:  aws.Time(time.Now()),
			},
		},
	}
	destServer := &StorageServer{
		bucket: "b-test-bucket",
		prefix: "another-place",
	}

	// Catch an error deleting the item a
	err := sourceServer.Move(context.Background(), "dir", "address", destServer)
	c.Assert(err, check.IsNil)
	c.Assert(
		opMove.ops,
		check.DeepEquals,
		[]string{"a-test-bucket-some/prefix/dir/address-b-test-bucket-another-place/dir/address"},
	)
}

// Test scenario where we move from S3 -> S3. The S3-specific move
// operation fails, but we succeed by performing a copy + delete, still
// using the S3-specific copy operation.
func (s *S3StorageServerSuite) TestMoveViaS3Fallback(c *check.C) {
	// The initial move will fail
	opMove := &fakeMoveOrCopy{
		ops:    make([]string, 0),
		result: errors.New("no go"),
	}
	// The initial copy on fallback will succeed.
	opCopy := &fakeMoveOrCopy{
		ops: make([]string, 0),
	}
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
		head: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		move:   opMove.Operation,
		copy:   opCopy.Operation,
		svc:    svc,
	}
	destServer := &StorageServer{
		bucket: "b-test-bucket",
		prefix: "another-place",
		svc:    svc,
	}
	err := sourceServer.Move(context.Background(), "dir", "address", destServer)
	c.Assert(err, check.IsNil)
	c.Assert(
		opMove.ops,
		check.DeepEquals,
		[]string{"a-test-bucket-some/prefix/dir/address-b-test-bucket-another-place/dir/address"},
	)
	c.Assert(svc.uploaded, check.Equals, "")
}

// Test scenario where we move from S3 -> S3. The S3-specific move
// operation fails. The fallback to copying fails when trying the
// S3-specific copy operation. Finally, we succeed by copying with
// a `Get` + `Put` operation followed by a `Remove`.
func (s *S3StorageServerSuite) TestMoveViaS3FallbackCopyFallback(c *check.C) {
	// The initial move will fail
	opMove := &fakeMoveOrCopy{
		ops:    make([]string, 0),
		result: errors.New("no go"),
	}
	// The initial copy on fallback will fail, as well, but the `Get` + `Put` operation
	// will still succeed.
	opCopy := &fakeMoveOrCopy{
		ops:    make([]string, 0),
		result: errors.New("no go"),
	}
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
		head: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		move:   opMove.Operation,
		copy:   opCopy.Operation,
		svc:    svc,
	}
	destServer := &StorageServer{
		bucket: "b-test-bucket",
		prefix: "another-place",
		svc:    svc,
	}
	err := sourceServer.Move(context.Background(), "dir", "address", destServer)
	c.Assert(err, check.IsNil)
	c.Assert(
		opMove.ops,
		check.DeepEquals,
		[]string{"a-test-bucket-some/prefix/dir/address-b-test-bucket-another-place/dir/address"},
	)
	c.Assert(svc.uploaded, check.Equals, "test output")
}

// Test scenario where we move from S3 to a non-S3 storage system.
// This scenario always uses a `Get` + `Put` operation followed by a
// `Remove`.
func (s *S3StorageServerSuite) TestMoveNoS3(c *check.C) {
	ctx := context.Background()
	output := &testReadCloser{bytes.NewBufferString("test output")}
	now := time.Now()
	svc := &fakeS3{
		get: &s3.GetObjectOutput{
			Body:          output,
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
		head: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(45),
			LastModified:  aws.Time(now),
		},
		deleteErr: errors.New("delete error"),
	}
	sourceServer := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
		svc:    svc,
	}
	// The destination server is not of type `SetStorageServer`, so we won't attempt
	// an S3-specific move operation.
	destServer := &rsstorage.DummyStorageServer{
		PutErr: errors.New("put error"),
	}
	err := sourceServer.Move(ctx, "dir", "address", destServer)
	c.Assert(err, check.ErrorMatches, "put error")

	destServer.PutErr = nil
	err = sourceServer.Move(ctx, "dir", "address", destServer)
	c.Assert(err, check.ErrorMatches, "delete error")

	svc.deleteErr = nil
	err = sourceServer.Move(ctx, "dir", "address", destServer)
	c.Assert(err, check.IsNil)
}

func (s *S3StorageServerSuite) TestLocate(c *check.C) {
	server := &StorageServer{
		bucket: "a-test-bucket",
		prefix: "some/prefix",
	}
	c.Check(server.Locate("dir", "address"), check.Equals, "s3://a-test-bucket/some/prefix/dir/address")
	c.Check(server.Locate("", "address"), check.Equals, "s3://a-test-bucket/some/prefix/address")

	server = &StorageServer{
		bucket: "a-test-bucket",
	}
	c.Check(server.Locate("dir", "address"), check.Equals, "s3://a-test-bucket/dir/address")
	c.Check(server.Locate("", "address"), check.Equals, "s3://a-test-bucket/address")
}

func (s *S3StorageServerSuite) TestUsage(c *check.C) {
	svc := &fakeS3{}
	wn := &servertest.DummyWaiterNotifier{}
	server := NewStorageServer(StorageServerArgs{
		Bucket:    "testbucket",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})

	usage, err := server.CalculateUsage()
	c.Assert(usage, check.DeepEquals, rtypes.Usage{})
	c.Assert(err, check.NotNil)
}

func (s *S3StorageServerSuite) TestValidate(c *check.C) {
	ctx := context.Background()
	uploadErr := errors.New("s3 upload op failed")
	headErr := errors.New("s3 head op failed")
	deleteErr := errors.New("s3 delete op failed")

	svc := &fakeS3{}
	wn := &servertest.DummyWaiterNotifier{}
	server := NewStorageServer(StorageServerArgs{
		Bucket:    "testbucket",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})

	s3Store, ok := server.(*StorageServer)
	c.Assert(ok, check.Equals, true)

	err := s3Store.Validate(ctx)
	c.Assert(err, check.IsNil)

	svc = &fakeS3{
		uploadErr: uploadErr,
	}
	server = NewStorageServer(StorageServerArgs{
		Bucket:    "testbucket",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})
	s3Store, ok = server.(*StorageServer)
	c.Assert(ok, check.Equals, true)
	err = s3Store.Validate(ctx)
	c.Check(err, check.Equals, uploadErr)

	svc = &fakeS3{
		headErr: headErr,
	}
	server = NewStorageServer(StorageServerArgs{
		Bucket:    "testbucket",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})
	s3Store, ok = server.(*StorageServer)
	c.Assert(ok, check.Equals, true)
	err = s3Store.Validate(ctx)
	c.Check(err, check.Equals, headErr)

	svc = &fakeS3{
		headErr: deleteErr,
	}
	server = NewStorageServer(StorageServerArgs{
		Bucket:    "testbucket",
		Prefix:    "prefix",
		Svc:       svc,
		ChunkSize: 4096,
		Waiter:    wn,
		Notifier:  wn,
	})
	s3Store, ok = server.(*StorageServer)
	c.Assert(ok, check.Equals, true)
	err = s3Store.Validate(ctx)
	c.Check(err, check.Equals, deleteErr)
}
