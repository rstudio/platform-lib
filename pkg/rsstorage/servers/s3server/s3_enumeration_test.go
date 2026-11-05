package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fortytw2/leaktest"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"
)

type MetaTestSuite struct{}

var _ = check.Suite(&MetaTestSuite{})

func (s *MetaTestSuite) TestBucketDirs(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/</Prefix>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes>
    <Prefix>bin/3.4-xenial/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>bin/3.5-xenial/</Prefix>
  </CommonPrefixes>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	dirs, err := awsOps.BucketDirs(ctx, "no-sync", "bin/")
	c.Assert(err, check.NotNil)
	c.Assert(dirs, check.IsNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)

	dirs, err = awsOps.BucketDirs(ctx, "sync", "bin/")
	c.Assert(err, check.IsNil)
	c.Check(dirs, check.DeepEquals, []string{"3.4-xenial", "3.5-xenial"})
}

func (s *MetaTestSuite) TestBucketObjects(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/3.5-xenial/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ABCDEFG.json</Key>
  </Contents>
  <Contents>
    <Key>HIJKLMN.tar.gz</Key>
  </Contents>
  <Contents>
    <Key>OPQRSTU.zip</Key>
  </Contents>
  <Contents>
    <Key>nothing</Key>
  </Contents>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	files, err := awsOps.BucketObjects(ctx, "no-sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
	c.Assert(files, check.IsNil)

	files, err = awsOps.BucketObjects(ctx, "sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"HIJKLMN", "OPQRSTU"})
}

func (s *MetaTestSuite) TestBucketObjectsMap(c *check.C) {
	client := http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  &client,
		},
	)
	awsOps := NewAwsOps(s3Client)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>sync</Name>
  <Prefix>bin/3.5-xenial/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ABCDEFG.json</Key>
    <ETag>123</ETag>
  </Contents>
  <Contents>
    <Key>HIJKLMN.tar.gz</Key>
    <ETag>456</ETag>
  </Contents>
  <Contents>
    <Key>OPQRSTU.zip</Key>
    <ETag>789</ETag>
  </Contents>
  <Contents>
    <Key>nothing</Key>
    <ETag>0</ETag>
  </Contents>
</ListBucketResult>`))
	httpmock.RegisterResponder("GET", `https://no-sync.s3.us-east-1.amazonaws.com/?delimiter=%2F&prefix=bin%2F3.5-xenial`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	ctx := context.Background()
	files, err := awsOps.BucketObjectsETagMap(ctx, "no-sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "StatusCode: 404"), check.Equals, true)
	c.Assert(files, check.IsNil)

	files, err = awsOps.BucketObjectsETagMap(ctx, "sync", "bin/3.5-xenial", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, map[string]string{
		"HIJKLMN": "456",
		"OPQRSTU": "789",
	})
}

// pagedResponder serves the pages in order. Each page after the first must be
// requested with the expected marker. It records each marker it receives.
type pagedResponder struct {
	pages   []string
	markers map[string]int
	seen    []string
}

func (p *pagedResponder) respond(req *http.Request) (*http.Response, error) {
	marker := req.URL.Query().Get("marker")
	p.seen = append(p.seen, marker)
	idx, ok := p.markers[marker]
	if !ok {
		return httpmock.NewStringResponse(http.StatusBadRequest, "unexpected marker "+marker), nil
	}
	return httpmock.NewStringResponse(http.StatusOK, p.pages[idx]), nil
}

func listPage(truncated bool, nextMarker string, keys ...string) string {
	var b strings.Builder
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>sync</Name>`)
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", truncated)
	if nextMarker != "" {
		fmt.Fprintf(&b, "<NextMarker>%s</NextMarker>", nextMarker)
	}
	for _, k := range keys {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><ETag>etag-%s</ETag></Contents>", k, k)
	}
	b.WriteString("</ListBucketResult>")
	return b.String()
}

func newMockedAwsOps() (*DefaultAwsOps, *http.Client) {
	client := &http.Client{}
	s3Client := s3.New(
		s3.Options{
			Region:      "us-east-1",
			Credentials: aws.AnonymousCredentials{},
			HTTPClient:  client,
		},
	)
	return NewAwsOps(s3Client), client
}

const syncBucketURL = "https://sync.s3.us-east-1.amazonaws.com/"

func (s *MetaTestSuite) TestBucketObjectsPaginationNextMarker(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// With a delimiter, S3 returns NextMarker on truncated pages.
	p := &pagedResponder{
		pages: []string{
			listPage(true, "m1", "a.tar.gz", "b.zip"),
			listPage(true, "m2", "c.tar.gz"),
			listPage(false, "", "d.zip", "e.json"),
		},
		markers: map[string]int{"": 0, "m1": 1, "m2": 2},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	files, err := awsOps.BucketObjects(context.Background(), "sync", "", 4, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"a", "b", "c", "d"})
	c.Check(p.seen, check.DeepEquals, []string{"", "m1", "m2"})
}

func (s *MetaTestSuite) TestBucketObjectsPaginationRecursiveLastKey(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// Without a delimiter, S3 does not return NextMarker. The last key of the
	// page must be used as the marker for the next page.
	p := &pagedResponder{
		pages: []string{
			listPage(true, "", "x/a.tar.gz", "x/b.zip"),
			listPage(false, "", "x/y/c.tar.gz"),
		},
		markers: map[string]int{"": 0, "x/b.zip": 1},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	files, err := awsOps.BucketObjectsETagMap(context.Background(), "sync", "x/", 1, true, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, map[string]string{
		"a":   "etag-x/a.tar.gz",
		"b":   "etag-x/b.zip",
		"y/c": "etag-x/y/c.tar.gz",
	})
	c.Check(p.seen, check.DeepEquals, []string{"", "x/b.zip"})
}

func (s *MetaTestSuite) TestBucketObjectsEmptyListing(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	p := &pagedResponder{
		pages:   []string{listPage(false, "")},
		markers: map[string]int{"": 0},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	files, err := awsOps.BucketObjects(context.Background(), "sync", "", 1, true, nil)
	c.Assert(err, check.IsNil)
	// An empty listing returns an empty slice, not nil.
	c.Check(files, check.NotNil)
	c.Check(files, check.HasLen, 0)
	c.Check(p.seen, check.DeepEquals, []string{""})
}

func (s *MetaTestSuite) TestBucketObjectsTruncatedWithoutMarker(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// A truncated page with no NextMarker and no contents cannot be continued.
	// The listing must fail rather than report a partial list as complete.
	p := &pagedResponder{
		pages:   []string{listPage(true, "")},
		markers: map[string]int{"": 0},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	files, err := awsOps.BucketObjects(context.Background(), "sync", "", 1, true, nil)
	c.Assert(err, check.ErrorMatches, "error listing objects: truncated page with no marker to continue from")
	c.Check(files, check.IsNil)
	c.Check(p.seen, check.DeepEquals, []string{""})
}

func (s *MetaTestSuite) TestBucketObjectsMarkerDoesNotAdvance(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// The second page returns the same marker it was asked for. Following it
	// would loop forever, so the listing must fail.
	p := &pagedResponder{
		pages: []string{
			listPage(true, "m1", "a.tar.gz"),
			listPage(true, "m1", "b.tar.gz"),
		},
		markers: map[string]int{"": 0, "m1": 1},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, func(req *http.Request) (*http.Response, error) {
		if len(p.seen) >= 5 {
			return httpmock.NewStringResponse(http.StatusBadRequest, "too many requests"), nil
		}
		return p.respond(req)
	})

	files, err := awsOps.BucketObjectsETagMap(context.Background(), "sync", "", 1, false, BinaryReg)
	c.Assert(err, check.ErrorMatches, `error listing objects: marker "m1" did not advance`)
	c.Check(files, check.IsNil)
	c.Check(p.seen, check.DeepEquals, []string{"", "m1"})
}

func (s *MetaTestSuite) TestBucketObjectsEmptyNextMarker(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// An empty NextMarker must be treated as missing, so the last key is used.
	// Using the empty marker would refetch the first page forever.
	page1 := strings.Replace(listPage(true, "", "a.tar.gz", "b.zip"),
		"<IsTruncated>", "<NextMarker></NextMarker><IsTruncated>", 1)
	p := &pagedResponder{
		pages:   []string{page1, listPage(false, "", "c.tar.gz")},
		markers: map[string]int{"": 0, "b.zip": 1},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, func(req *http.Request) (*http.Response, error) {
		if len(p.seen) >= 5 {
			return httpmock.NewStringResponse(http.StatusBadRequest, "too many requests"), nil
		}
		return p.respond(req)
	})

	files, err := awsOps.BucketObjects(context.Background(), "sync", "", 1, false, BinaryReg)
	c.Assert(err, check.IsNil)
	c.Check(files, check.DeepEquals, []string{"a", "b", "c"})
	c.Check(p.seen, check.DeepEquals, []string{"", "b.zip"})
}

func (s *MetaTestSuite) TestBucketObjectsErrorOnLaterPage(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	// The second page asks for marker "m1", which the responder rejects.
	p := &pagedResponder{
		pages:   []string{listPage(true, "m1", "a.tar.gz")},
		markers: map[string]int{"": 0},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	files, err := awsOps.BucketObjects(context.Background(), "sync", "", 1, false, BinaryReg)
	c.Assert(err, check.NotNil)
	c.Check(strings.Contains(err.Error(), "error listing objects"), check.Equals, true)
	c.Check(files, check.IsNil)
	c.Check(p.seen, check.DeepEquals, []string{"", "m1"})
}

func (s *MetaTestSuite) TestEnumerateBucketCancelInCallback(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	p := &pagedResponder{
		pages: []string{
			listPage(true, "m1", "a.tar.gz"),
			listPage(false, "", "b.tar.gz"),
		},
		markers: map[string]int{"": 0, "m1": 1},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	// Cancel after the first truncated page. The listing must stop and return
	// the context error without fetching the next page.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	err := awsOps.enumerateBucket(ctx, "sync", "", 1, false, func(*s3.ListObjectsOutput) {
		calls++
		cancel()
	})
	c.Assert(errors.Is(err, context.Canceled), check.Equals, true)
	c.Check(calls, check.Equals, 1)
	c.Check(p.seen, check.DeepEquals, []string{""})
}

func (s *MetaTestSuite) TestEnumerateBucketCancelNoGoroutineLeak(c *check.C) {
	defer leaktest.Check(c)()

	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	p := &pagedResponder{
		pages: []string{
			listPage(true, "m1", "a.tar.gz"),
			listPage(false, "", "b.tar.gz"),
		},
		markers: map[string]int{"": 0, "m1": 1},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	// Cancel in the callback after a truncated page. No goroutine may be
	// left behind. One run does not always hit the race that leaked in the
	// old worker pool, so repeat it.
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		err := awsOps.enumerateBucket(ctx, "sync", "", 1, false, func(*s3.ListObjectsOutput) {
			cancel()
		})
		cancel()
		c.Assert(errors.Is(err, context.Canceled), check.Equals, true)
	}
}

func (s *MetaTestSuite) TestEnumerateBucketCancelDuringRequest(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel while the request is in flight. The request must see the
	// cancellation, which only happens if the caller's ctx reaches
	// ListObjects.
	sawCancel := make(chan struct{})
	httpmock.RegisterResponder("GET", syncBucketURL, func(req *http.Request) (*http.Response, error) {
		cancel()
		select {
		case <-req.Context().Done():
			close(sawCancel)
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return httpmock.NewStringResponse(http.StatusOK, listPage(false, "", "a.tar.gz")), nil
		}
	})

	files, err := awsOps.BucketObjects(ctx, "sync", "", 1, false, BinaryReg)
	c.Assert(errors.Is(err, context.Canceled), check.Equals, true)
	c.Check(files, check.IsNil)

	// The HTTP client can return before the responder finishes, so wait.
	select {
	case <-sawCancel:
	case <-time.After(5 * time.Second):
		c.Fatal("the request did not see the cancellation")
	}
}

func (s *MetaTestSuite) TestEnumerateBucketCanceledContext(c *check.C) {
	awsOps, client := newMockedAwsOps()
	httpmock.ActivateNonDefault(client)
	defer httpmock.DeactivateAndReset()

	p := &pagedResponder{
		pages:   []string{listPage(false, "", "a.tar.gz")},
		markers: map[string]int{"": 0},
	}
	httpmock.RegisterResponder("GET", syncBucketURL, p.respond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	files, err := awsOps.BucketObjects(ctx, "sync", "", 1, false, BinaryReg)
	c.Assert(errors.Is(err, context.Canceled), check.Equals, true)
	c.Check(files, check.IsNil)
	c.Check(p.seen, check.HasLen, 0)
}
