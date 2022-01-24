package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"fmt"
	"log"
	"net/url"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/awstesting"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/check.v1"
)

// From https://github.com/aws/aws-sdk-go/pull/2653
//
// This implements multi-part copying to support copying files larger than
// 5GB on S3. This may eventually be included in the aws SDK; when that
// happens, we should remove `s3_copier.go`, `s3_copier_internal_test.go`,
// and `s3_copier_test.go`.

// TODO Watch https://github.com/aws/aws-sdk-go/pull/2653 and remove the
// TODO files listed above when this code is incorporated in the AWS SDK.

type S3CopierSuite struct{}

var _ = check.Suite(&S3CopierSuite{})

func ExampleNewCopierWithClient() {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	copier := NewCopierWithClient(
		svc, func(copier *Copier) {
			copier.LeavePartsOnError = true
		})
	_, _ = copier.CopyWithContext(
		aws.BackgroundContext(), &s3.CopyObjectInput{
			Bucket:     aws.String("dest-bucket"),
			Key:        aws.String("lorem/ipsum.txt"),
			CopySource: aws.String(url.QueryEscape("src-bucket/lorem/ipsum.txt?versionId=1")),
		})
}

func ExampleCopier_CopyWithContext() {
	var svc s3iface.S3API
	copier := NewCopierWithClient(svc)

	// Copy s3://src-bucket/lorem/ipsum.txt to s3://dest-bucket/lorem/ipsum.txt.
	// Version 1 of the source object will be copied.
	out, err := copier.Copy(
		&s3.CopyObjectInput{
			Bucket:     aws.String("dest-bucket"),
			Key:        aws.String("lorem/ipsum.txt"),
			CopySource: aws.String(url.QueryEscape("src-bucket/lorem/ipsum.txt?versionId=1")),
		},
		// Optional parameter for customization.
		func(c *Copier) {
			c.LeavePartsOnError = true
		})

	if err != nil {
		panic(err)
	}

	log.Printf("The destination object's ETag is: %s", *out.CopyObjectResult.ETag)
}

type copyTestCall struct {
	method string
	input  interface{}
}

type copyTestMock struct {
	s3iface.S3API
	calls            []copyTestCall
	srcContentLength int64
	mu               sync.Mutex
	partRanges       []string // form is "num:bytes=start-end"
	errorAtNthRange  int64
}

func (m *copyTestMock) appendCall(method string, input interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, copyTestCall{method, input})
}

func (m *copyTestMock) appendPartRange(num int64, partRange string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partRanges = append(m.partRanges, fmt.Sprintf("%d:%s", num, partRange))
}

func (m *copyTestMock) getCallOrder() []string {
	var out []string
	for _, call := range m.calls {
		out = append(out, call.method)
	}
	return out
}

func (m *copyTestMock) HeadObjectWithContext(
	ctx aws.Context,
	in *s3.HeadObjectInput,
	opts ...request.Option,
) (*s3.HeadObjectOutput, error) {
	m.appendCall("HeadObject", in)
	out := s3.HeadObjectOutput{
		ContentLength: aws.Int64(m.srcContentLength),
	}
	return &out, nil
}

func (m *copyTestMock) CopyObjectWithContext(
	ctx aws.Context,
	in *s3.CopyObjectInput,
	opts ...request.Option,
) (*s3.CopyObjectOutput, error) {
	m.appendCall("CopyObject", in)
	out := s3.CopyObjectOutput{}
	out.VersionId = aws.String("ETag-simple")
	return &out, nil
}

func (m *copyTestMock) CreateMultipartUploadWithContext(
	ctx aws.Context,
	in *s3.CreateMultipartUploadInput,
	opts ...request.Option,
) (*s3.CreateMultipartUploadOutput, error) {
	m.appendCall("CreateMultipartUpload", in)
	out := s3.CreateMultipartUploadOutput{}
	out.SetUploadId("Upload123")
	return &out, nil
}

func (m *copyTestMock) UploadPartCopyWithContext(
	ctx aws.Context,
	in *s3.UploadPartCopyInput,
	opts ...request.Option,
) (*s3.UploadPartCopyOutput, error) {
	if *in.PartNumber == m.errorAtNthRange {
		return nil, fmt.Errorf("intentional failure")
	}

	m.appendCall("UploadPartCopy", in)
	m.appendPartRange(*in.PartNumber, *in.CopySourceRange)
	out := s3.UploadPartCopyOutput{}
	out.CopyPartResult = &s3.CopyPartResult{
		ETag: aws.String(fmt.Sprintf("ETag-%d", *in.PartNumber)),
	}
	return &out, nil
}

func (m *copyTestMock) CompleteMultipartUploadWithContext(
	ctx aws.Context,
	in *s3.CompleteMultipartUploadInput,
	opts ...request.Option,
) (*s3.CompleteMultipartUploadOutput, error) {
	m.appendCall("CompleteMultipartUpload", in)
	out := s3.CompleteMultipartUploadOutput{}
	return &out, nil
}

func (m *copyTestMock) AbortMultipartUploadWithContext(
	ctx aws.Context,
	in *s3.AbortMultipartUploadInput,
	opts ...request.Option,
) (*s3.AbortMultipartUploadOutput, error) {
	m.appendCall("AbortMultipartUpload", in)
	out := s3.AbortMultipartUploadOutput{}
	return &out, nil
}

func assertEqual(c *check.C, expect, actual interface{}) {
	if !reflect.DeepEqual(expect, actual) {
		c.Fatalf(awstesting.SprintExpectActual(expect, actual))
	}
}

func assertNoError(c *check.C, err error) {
	if err != nil {
		c.Fatalf("expected no error; got %+v", err)
	}
}

func assertError(c *check.C, err error) {
	if err == nil {
		c.Fatalf("expected error")
	}
}

func assertStringIn(c *check.C, s string, slice []string) {
	for _, x := range slice {
		if x == s {
			return
		}
	}

	c.Fatalf("expected to find %s in %+v", s, slice)
}

func (s *S3CopierSuite) TestCopyWhenSizeBelowThreshold(c *check.C) {
	m := copyTestMock{
		srcContentLength: DefaultMultipartCopyThreshold - 1,
	}
	cc := NewCopierWithClient(&m)

	copySource := url.QueryEscape("bucket/prefix/file.txt?versionId=123")
	out, err := cc.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String("destbucket"),
		Key:        aws.String("dest/key.txt"),
		CopySource: &copySource,
	})
	assertNoError(c, err)

	ord := m.getCallOrder()
	assertEqual(c, []string{"HeadObject", "CopyObject"}, ord)

	{
		in := m.calls[0].input.(*s3.HeadObjectInput)
		assertEqual(c, *in.Bucket, "bucket")
		assertEqual(c, *in.Key, "prefix/file.txt")
		assertEqual(c, *in.VersionId, "123")
	}

	{
		in := m.calls[1].input.(*s3.CopyObjectInput)
		assertEqual(c, *in.Bucket, "destbucket")
		assertEqual(c, *in.Key, "dest/key.txt")
		assertEqual(c, *in.CopySource, copySource)
	}

	assertEqual(c, "ETag-simple", *out.VersionId)
}

func (s *S3CopierSuite) TestCopyWhenSizeAboveThreshold(c *check.C) {
	m := copyTestMock{}
	cc := NewCopierWithClient(&m)
	cc.MaxPartSize = s3manager.MinUploadPartSize
	cc.MultipartCopyThreshold = s3manager.MinUploadPartSize
	m.srcContentLength = 2*cc.MaxPartSize + 1

	copySource := url.QueryEscape("bucket/prefix/file.txt?versionId=123")
	_, err := cc.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String("destbucket"),
		Key:        aws.String("dest/key.txt"),
		CopySource: &copySource,
	})
	assertNoError(c, err)

	ord := m.getCallOrder()
	assertEqual(c, []string{
		"HeadObject", "CreateMultipartUpload",
		"UploadPartCopy", "UploadPartCopy", "UploadPartCopy",
		"CompleteMultipartUpload"},
		ord)

	assertEqual(c, 3, len(m.partRanges))
	assertStringIn(c, fmt.Sprintf("1:bytes=0-%d", s3manager.MinUploadPartSize-1), m.partRanges)
	assertStringIn(c, fmt.Sprintf("2:bytes=%d-%d", s3manager.MinUploadPartSize, s3manager.MinUploadPartSize*2-1), m.partRanges)
	assertStringIn(c, fmt.Sprintf("3:bytes=%d-%d", 2*s3manager.MinUploadPartSize, 2*s3manager.MinUploadPartSize), m.partRanges)

	{
		in := m.calls[1].input.(*s3.CreateMultipartUploadInput)
		assertEqual(c, *in.Bucket, "destbucket")
		assertEqual(c, *in.Key, "dest/key.txt")
	}

	for _, call := range m.calls[2:5] {
		in := call.input.(*s3.UploadPartCopyInput)
		assertEqual(c, *in.Bucket, "destbucket")
		assertEqual(c, *in.Key, "dest/key.txt")
		assertEqual(c, *in.UploadId, "Upload123")
	}

	{
		in := m.calls[5].input.(*s3.CompleteMultipartUploadInput)
		assertEqual(c, *in.Bucket, "destbucket")
		assertEqual(c, *in.Key, "dest/key.txt")
		assertEqual(c, *in.UploadId, "Upload123")
		assertEqual(c, 3, len(in.MultipartUpload.Parts))
	}
}

func (s *S3CopierSuite) TestCopyAbortWhenUploadPartFails(c *check.C) {
	m := copyTestMock{}
	cc := NewCopierWithClient(&m)
	cc.MaxPartSize = s3manager.MinUploadPartSize
	cc.MultipartCopyThreshold = s3manager.MinUploadPartSize
	cc.Concurrency = 1
	m.srcContentLength = 2*cc.MaxPartSize + 1
	m.errorAtNthRange = 2

	copySource := url.QueryEscape("bucket/prefix/file.txt?versionId=123")
	_, err := cc.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String("destbucket"),
		Key:        aws.String("dest/key.txt"),
		CopySource: &copySource,
	})
	assertError(c, err)

	ord := m.getCallOrder()
	assertEqual(c, []string{
		"HeadObject", "CreateMultipartUpload",
		"UploadPartCopy",
		"AbortMultipartUpload"},
		ord)

	{
		in := m.calls[3].input.(*s3.AbortMultipartUploadInput)
		assertEqual(c, *in.Bucket, "destbucket")
		assertEqual(c, *in.Key, "dest/key.txt")
		assertEqual(c, *in.UploadId, "Upload123")
	}
}

func (s *S3CopierSuite) TestCopyNoAbortWhenUploadPartFailsButLeavePartsIsSet(c *check.C) {
	m := copyTestMock{}
	cc := NewCopierWithClient(&m)
	cc.MaxPartSize = s3manager.MinUploadPartSize
	cc.MultipartCopyThreshold = s3manager.MinUploadPartSize
	cc.Concurrency = 1
	cc.LeavePartsOnError = true
	m.srcContentLength = 2*cc.MaxPartSize + 1
	m.errorAtNthRange = 2

	copySource := url.QueryEscape("bucket/prefix/file.txt?versionId=123")
	_, err := cc.Copy(&s3.CopyObjectInput{
		Bucket:     aws.String("destbucket"),
		Key:        aws.String("dest/key.txt"),
		CopySource: &copySource,
	})
	assertError(c, err)

	ord := m.getCallOrder()
	assertEqual(c, []string{
		"HeadObject", "CreateMultipartUpload", "UploadPartCopy"},
		ord)
}
