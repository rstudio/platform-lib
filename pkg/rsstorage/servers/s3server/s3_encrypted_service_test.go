package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsstorage"
)

type S3EncryptedServiceSuite struct{}

var _ = check.Suite(&S3EncryptedServiceSuite{})

const (
	// Raw AWS responses generated via aws.LogDebugWithHTTPBody using test objects
	kmsResponse = `{"CiphertextBlob":"AQIDAHjn6Sd1ah3Pq5ObkS0zZNMKPN158UNlAjJfcYmp3qOIJAGWPnUuTqUcLSVl0Sxk2OcOAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQME/hVJ7LNrJ0uLrKcAgEQgDs8iwgfz3Ml4D8zMjCXjkb7GRysOsam4yAM/EE5Ynl+fgrzwGu6CYXjT1IstlAO4weQR6+yAlw3C5xhXw==","KeyId":"arn:aws:kms:us-east-1:528395739535:key/7ddec34f-7c3e-4875-a348-de761fc28b4f","Plaintext":"VZrCXyYuBdlGvFsiN7ZRvobqh5VyJmc16aaAJ2/6dEI="}`
)

func (s *S3EncryptedServiceSuite) TestUpload(c *check.C) {
	ctx := context.Background()
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(
		ctx,
		rsstorage.ConfigS3{Region: "us-east-1"},
		"7ddec34f-7c3e-4875-a348-de761fc28b4f",
		&client,
	)
	c.Assert(err, check.IsNil)
	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodPost, `https://kms.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusOK, kmsResponse))

	httpmock.RegisterResponder(http.MethodPut, `https://tyler-s3-test.s3.amazonaws.com/test.txt`,
		httpmock.NewStringResponder(http.StatusOK, ""))

	bucket := "tyler-s3-test"
	key := "test.text"

	input := &s3.UploadPartInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   strings.NewReader("test"),
	}

	_, err = s3Service.Upload(ctx, input)
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestGetObject(c *check.C) {
	ctx := context.Background()
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(
		ctx,
		rsstorage.ConfigS3{Region: "us-east-1"},
		"7ddec34f-7c3e-4875-a348-de761fc28b4f",
		&client,
	)
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodPost, `https://kms.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusOK, kmsResponse))

	httpmock.RegisterResponder(http.MethodGet, `https://tyler-s3-test.s3.amazonaws.com/test.txt`,
		func(req *http.Request) (*http.Response, error) {
			b, err := os.ReadFile("./testdata/test.txt")
			c.Assert(err, check.IsNil)

			res := httpmock.NewBytesResponse(http.StatusOK, b)
			res.Header.Add("x-amz-meta-x-amz-tag-len", "128")
			res.Header.Add("x-amz-meta-x-amz-unencrypted-content-length", "4")
			res.Header.Add("x-amz-meta-x-amz-wrap-alg", "kms+context")
			res.Header.Add("x-amz-meta-x-amz-matdesc", `{"aws:x-amz-cek-alg":"AES/GCM/NoPadding"}`)
			res.Header.Add("x-amz-meta-x-amz-key-v2", "AQIDAHjn6Sd1ah3Pq5ObkS0zZNMKPN158UNlAjJfcYmp3qOIJAGWPnUuTqUcLSVl0Sxk2OcOAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQME/hVJ7LNrJ0uLrKcAgEQgDs8iwgfz3Ml4D8zMjCXjkb7GRysOsam4yAM/EE5Ynl+fgrzwGu6CYXjT1IstlAO4weQR6+yAlw3C5xhXw==")
			res.Header.Add("x-amz-meta-x-amz-cek-alg", "AES/GCM/NoPadding")
			res.Header.Add("x-amz-meta-x-amz-iv", "KxoyygmuPbQkCV7e")

			return res, nil
		},
	)

	bucket := "tyler-s3-test"
	key := "test.txt"

	input := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	out, err := s3Service.GetObject(ctx, input)
	c.Assert(err, check.IsNil)
	b, err := io.ReadAll(out.Body)
	c.Assert(err, check.IsNil)
	c.Check(string(b), check.Equals, "test")
}
