package s3server

// Copyright (C) 2026 by Posit Software, PBC

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage"
	rtypes "github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

var rangeModTime = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

type recordedGet struct {
	Key   string
	Range string // empty if the request had no Range
}

// rangeS3 is an in-memory S3 that honors Range the way S3 does, and records every GetObject.
type rangeS3 struct {
	fakeS3
	objects map[string][]byte
	gets    []recordedGet
	kms     bool
}

func (s *rangeS3) KmsEncrypted() bool { return s.kms }

func (s *rangeS3) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	data, ok := s.objects[*input.Key]
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(data))), LastModified: aws.Time(rangeModTime)}, nil
}

func (s *rangeS3) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	rec := recordedGet{Key: *input.Key, Range: aws.ToString(input.Range)}
	s.gets = append(s.gets, rec)
	data, ok := s.objects[*input.Key]
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}
	out := &s3.GetObjectOutput{LastModified: aws.Time(rangeModTime)}
	if rec.Range == "" {
		out.Body = io.NopCloser(bytes.NewReader(data))
		out.ContentLength = aws.Int64(int64(len(data)))
		return out, nil
	}
	var first, last int64
	if _, err := fmt.Sscanf(rec.Range, "bytes=%d-%d", &first, &last); err != nil {
		return nil, err
	}
	if first >= int64(len(data)) {
		return nil, &smithy.GenericAPIError{Code: "InvalidRange", Message: "The requested range is not satisfiable"}
	}
	last = min(last, int64(len(data))-1)
	out.Body = io.NopCloser(bytes.NewReader(data[first : last+1]))
	out.ContentLength = aws.Int64(last - first + 1)
	out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", first, last, len(data)))
	return out, nil
}

func (s *rangeS3) reset() { s.gets = nil }

func (s *rangeS3) keys() string {
	var out []string
	for _, g := range s.gets {
		out = append(out, g.Key+"|"+g.Range)
	}
	return strings.Join(out, "\n")
}

func s3RangeData() []byte {
	b := make([]byte, 95)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// newRangeServer stores a plain object at prefix/dir/plain and a chunked one
// (chunk size 10, short last chunk) at prefix/dir/chunked.
func newRangeServer(c *check.C) (*StorageServer, *rangeS3, []byte) {
	data := s3RangeData()
	svc := &rangeS3{objects: map[string][]byte{"prefix/dir/plain": data}}

	info, err := json.Marshal(rtypes.ChunksInfo{ChunkSize: 10, NumChunks: 10, FileSize: 95, Complete: true, ModTime: rangeModTime})
	c.Assert(err, check.IsNil)
	svc.objects["prefix/dir/chunked/info.json"] = info
	for i := 0; i < 10; i++ {
		svc.objects[fmt.Sprintf("prefix/dir/chunked/%08d", i+1)] = data[i*10 : min((i+1)*10, 95)]
	}
	return &StorageServer{bucket: "test", prefix: "prefix", svc: svc}, svc, data
}

func readRange(c *check.C, server *StorageServer, addr string, offset, length int64) ([]byte, int64, *rtypes.ChunksInfo) {
	r, info, size, mod, ok, err := server.GetRange(context.Background(), "dir", addr, offset, length)
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
	c.Check(mod.Equal(rangeModTime), check.Equals, true)
	got, err := io.ReadAll(r)
	c.Assert(err, check.IsNil)
	c.Check(r.Close(), check.IsNil)
	return got, size, info
}

func (s *S3StorageServerSuite) TestGetRangePlain(c *check.C) {
	server, svc, data := newRangeServer(c)
	var _ rsstorage.RangeGetter = server

	cases := []struct {
		name           string
		offset, length int64
		wantRange      string
		want           [2]int
	}{
		{"offset zero", 0, 7, "bytes=0-6", [2]int{0, 7}},
		{"middle", 12, 5, "bytes=12-16", [2]int{12, 17}},
		{"clamped at the end", 90, 1000, "bytes=90-1089", [2]int{90, 95}},
		{"last byte", 94, 1, "bytes=94-94", [2]int{94, 95}},
	}
	for _, tc := range cases {
		svc.reset()
		got, size, info := readRange(c, server, "plain", tc.offset, tc.length)
		c.Check(string(got), check.Equals, string(data[tc.want[0]:tc.want[1]]), check.Commentf(tc.name))
		c.Check(size, check.Equals, int64(95), check.Commentf(tc.name))
		c.Check(info, check.IsNil, check.Commentf(tc.name))
		// The decision: one GetObject, for the object, with the range. No full download.
		c.Check(svc.gets, check.DeepEquals, []recordedGet{{"prefix/dir/plain", tc.wantRange}}, check.Commentf(tc.name))
	}
}

func (s *S3StorageServerSuite) TestGetRangeNotSatisfiableAndInvalid(c *check.C) {
	server, _, _ := newRangeServer(c)
	ctx := context.Background()

	for _, addr := range []string{"plain", "chunked"} {
		for _, off := range []int64{95, 96, 5000} {
			_, _, _, _, _, err := server.GetRange(ctx, "dir", addr, off, 1)
			c.Check(errors.Is(err, rsstorage.ErrRangeNotSatisfiable), check.Equals, true, check.Commentf("%s %d", addr, off))
			var rerr *rsstorage.RangeNotSatisfiableError
			c.Assert(errors.As(err, &rerr), check.Equals, true, check.Commentf("%s %d", addr, off))
			c.Check(rerr.Size, check.Equals, int64(95))
		}
		for _, in := range [][2]int64{{-1, 5}, {0, 0}, {0, -3}} {
			_, _, _, _, _, err := server.GetRange(ctx, "dir", addr, in[0], in[1])
			c.Check(errors.Is(err, rsstorage.ErrInvalidRange), check.Equals, true, check.Commentf("%s %v", addr, in))
		}
	}

	_, _, _, _, ok, err := server.GetRange(ctx, "dir", "missing", 0, 1)
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
}

func (s *S3StorageServerSuite) TestGetRangeChunked(c *check.C) {
	server, svc, data := newRangeServer(c)

	cases := []struct {
		name           string
		offset, length int64
		want           [2]int
		wantGets       []string // chunk file|Range, after the info.json read
	}{
		{"offset zero", 0, 7, [2]int{0, 7}, []string{"00000001|bytes=0-6"}},
		{"inside one chunk", 12, 5, [2]int{12, 17}, []string{"00000002|bytes=2-6"}},
		{"across chunks", 8, 25, [2]int{8, 33}, []string{"00000001|bytes=8-9", "00000002|bytes=0-9", "00000003|bytes=0-9", "00000004|bytes=0-2"}},
		{"deep offset, clamped", 45, 1000, [2]int{45, 95}, []string{
			"00000005|bytes=5-9", "00000006|bytes=0-9", "00000007|bytes=0-9", "00000008|bytes=0-9", "00000009|bytes=0-9", "00000010|bytes=0-4"}},
		{"last chunk", 91, 4, [2]int{91, 95}, []string{"00000010|bytes=1-4"}},
	}

	for _, tc := range cases {
		svc.reset()
		got, size, info := readRange(c, server, "chunked", tc.offset, tc.length)
		c.Check(string(got), check.Equals, string(data[tc.want[0]:tc.want[1]]), check.Commentf(tc.name))
		c.Check(size, check.Equals, int64(95), check.Commentf(tc.name))
		c.Check(info != nil, check.Equals, true, check.Commentf(tc.name))

		// The decision: the plain-object probe, info.json, then only the chunks the range
		// touches, each with its own range. Nothing before the offset is requested.
		want := []string{
			"prefix/dir/chunked|bytes=" + strconv.FormatInt(tc.offset, 10) + "-" + strconv.FormatInt(tc.offset+tc.length-1, 10),
			"prefix/dir/chunked/info.json|",
		}
		for _, g := range tc.wantGets {
			want = append(want, "prefix/dir/chunked/"+g)
		}
		c.Check(svc.keys(), check.Equals, strings.Join(want, "\n"), check.Commentf(tc.name))
	}
}

func (s *S3StorageServerSuite) TestGetRangeKMSNotSupported(c *check.C) {
	server, svc, _ := newRangeServer(c)
	svc.kms = true
	for _, addr := range []string{"plain", "chunked"} {
		_, _, _, _, ok, err := server.GetRange(context.Background(), "dir", addr, 0, 5)
		c.Check(errors.Is(err, rsstorage.ErrRangeNotSupported), check.Equals, true)
		c.Check(ok, check.Equals, false)
	}
	// Nothing was fetched: no decrypt-and-skip fallback.
	c.Check(svc.gets, check.HasLen, 0)
}
