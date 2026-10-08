package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"

	encryptClient "github.com/aws/amazon-s3-encryption-client-go/v3/client"
	"github.com/aws/amazon-s3-encryption-client-go/v3/materials"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jarcoal/httpmock"
	"gopkg.in/check.v1"
)

type S3EncryptedServiceSuite struct{}

var _ = check.Suite(&S3EncryptedServiceSuite{})

const (
	// Raw AWS responses generated via aws.LogDebugWithHTTPBody using test objects
	kmsResponse = `{"CiphertextBlob":"AQIDAHjn6Sd1ah3Pq5ObkS0zZNMKPN158UNlAjJfcYmp3qOIJAGWPnUuTqUcLSVl0Sxk2OcOAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQME/hVJ7LNrJ0uLrKcAgEQgDs8iwgfz3Ml4D8zMjCXjkb7GRysOsam4yAM/EE5Ynl+fgrzwGu6CYXjT1IstlAO4weQR6+yAlw3C5xhXw==","KeyId":"arn:aws:kms:us-east-1:528395739535:key/7ddec34f-7c3e-4875-a348-de761fc28b4f","Plaintext":"VZrCXyYuBdlGvFsiN7ZRvobqh5VyJmc16aaAJ2/6dEI="}`

	testKeyID = "7ddec34f-7c3e-4875-a348-de761fc28b4f"
)

// newTestEncryptedClient builds an *encryptClient.S3EncryptionClientV3 wired
// through the supplied http.Client for both S3 and KMS so tests can stub
// responses with httpmock. This mirrors the construction work that callers
// now perform themselves before handing the client to NewEncryptedS3Wrapper.
func newTestEncryptedClient(c *check.C, httpClient *http.Client) *encryptClient.S3EncryptionClientV3 {
	s3Client := s3.New(s3.Options{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  httpClient,
	})
	kmsClient := kms.New(kms.Options{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  httpClient,
	})
	cmm, err := materials.NewCryptographicMaterialsManager(materials.NewKmsKeyring(kmsClient, testKeyID))
	c.Assert(err, check.IsNil)
	client, err := encryptClient.New(s3Client, cmm)
	c.Assert(err, check.IsNil)
	return client
}

func (s *S3EncryptedServiceSuite) TestUpload(c *check.C) {
	ctx := context.Background()
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)
	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodPost, `https://kms.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusOK, kmsResponse))

	httpmock.RegisterResponder(http.MethodPut, `https://tyler-s3-test.s3.us-east-1.amazonaws.com/test.text?partNumber=1&uploadId=1&x-id=UploadPart`,
		httpmock.NewStringResponder(http.StatusOK, ""))

	httpmock.RegisterResponder(http.MethodPut, `https://tyler-s3-test.s3.us-east-1.amazonaws.com/test.text?x-id=PutObject`,
		httpmock.NewStringResponder(http.StatusOK, ""))

	bucket := "tyler-s3-test"
	key := "test.text"

	input := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   strings.NewReader("test"),
	}

	_, err = s3Service.Upload(ctx, input)
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestGetObject(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodPost, `https://kms.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusOK, kmsResponse))

	httpmock.RegisterResponder(http.MethodGet, `https://tyler-s3-test.s3.us-east-1.amazonaws.com/test.txt`,
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

	out, err := s3Service.GetObject(context.Background(), input)
	c.Assert(err, check.IsNil)
	b, err := io.ReadAll(out.Body)
	c.Assert(err, check.IsNil)
	c.Check(string(b), check.Equals, "test")
}

func (s *S3EncryptedServiceSuite) TestNewEncryptedS3WrapperNilClient(c *check.C) {
	_, err := NewEncryptedS3Wrapper(nil)
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "unable to create S3 encrypted wrapper, S3 client is nil")
}

func (s *S3EncryptedServiceSuite) TestKmsEncrypted(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)
	c.Assert(s3Service.KmsEncrypted(), check.Equals, true)
}

func (s *S3EncryptedServiceSuite) TestCreateBucket(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodPut, `https://test-bucket.s3.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	bucket := "test-bucket"
	_, err = s3Service.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: &bucket})
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestDeleteBucket(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodDelete, `https://test-bucket.s3.us-east-1.amazonaws.com/`,
		httpmock.NewStringResponder(http.StatusNoContent, ``))

	bucket := "test-bucket"
	_, err = s3Service.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: &bucket})
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestHeadObject(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://test-bucket.s3.us-east-1.amazonaws.com/test-key`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	bucket := "test-bucket"
	key := "test-key"
	_, err = s3Service.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestDeleteObject(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodDelete, `https://test-bucket.s3.us-east-1.amazonaws.com/test-key?x-id=DeleteObject`,
		httpmock.NewStringResponder(http.StatusNoContent, ``))

	bucket := "test-bucket"
	key := "test-key"
	_, err = s3Service.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestCopyObject(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	httpmock.RegisterResponder(http.MethodPut, `https://dest-bucket.s3.us-east-1.amazonaws.com/dest-key?x-id=CopyObject`,
		httpmock.NewStringResponder(http.StatusOK, `<CopyObjectResult><ETag>"etag"</ETag></CopyObjectResult>`))

	_, err = s3Service.CopyObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestCopyObjectHeadError(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	_, err = s3Service.CopyObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Matches, ".*HEAD for an S3 object.*")
}

func (s *S3EncryptedServiceSuite) TestMoveObject(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	httpmock.RegisterResponder(http.MethodPut, `https://dest-bucket.s3.us-east-1.amazonaws.com/dest-key?x-id=CopyObject`,
		httpmock.NewStringResponder(http.StatusOK, `<CopyObjectResult><ETag>"etag"</ETag></CopyObjectResult>`))

	httpmock.RegisterResponder(http.MethodDelete, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key?x-id=DeleteObject`,
		httpmock.NewStringResponder(http.StatusNoContent, ``))

	_, err = s3Service.MoveObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.IsNil)
}

func (s *S3EncryptedServiceSuite) TestMoveObjectHeadError(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusNotFound, ``))

	_, err = s3Service.MoveObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Matches, ".*HEAD for an S3 object.*")
}

func (s *S3EncryptedServiceSuite) TestMoveObjectCopyError(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	httpmock.RegisterResponder(http.MethodPut, `https://dest-bucket.s3.us-east-1.amazonaws.com/dest-key?x-id=CopyObject`,
		httpmock.NewStringResponder(http.StatusForbidden, ``))

	_, err = s3Service.MoveObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Matches, ".*moving an S3 object.*")
}

func (s *S3EncryptedServiceSuite) TestMoveObjectDeleteError(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(http.MethodHead, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key`,
		httpmock.NewStringResponder(http.StatusOK, ``))

	httpmock.RegisterResponder(http.MethodPut, `https://dest-bucket.s3.us-east-1.amazonaws.com/dest-key?x-id=CopyObject`,
		httpmock.NewStringResponder(http.StatusOK, `<CopyObjectResult><ETag>"etag"</ETag></CopyObjectResult>`))

	httpmock.RegisterResponder(http.MethodDelete, `https://source-bucket.s3.us-east-1.amazonaws.com/source-key?x-id=DeleteObject`,
		httpmock.NewStringResponder(http.StatusForbidden, ``))

	_, err = s3Service.MoveObject(context.Background(), "source-bucket", "source-key", "dest-bucket", "dest-key")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Matches, ".*deleting source object after move.*")
}

func (s *S3EncryptedServiceSuite) TestListObjects(c *check.C) {
	client := http.Client{}
	s3Service, err := NewEncryptedS3Wrapper(newTestEncryptedClient(c, &client))
	c.Assert(err, check.IsNil)

	httpmock.ActivateNonDefault(&client)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `https://test-bucket.s3.us-east-1.amazonaws.com/?list-type=2&prefix=test-prefix`,
		httpmock.NewStringResponder(http.StatusOK, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>test-prefix</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>test-prefix/file1.txt</Key>
  </Contents>
  <Contents>
    <Key>test-prefix/file2.txt</Key>
  </Contents>
</ListBucketResult>`))

	bucket := "test-bucket"
	prefix := "test-prefix"
	result, err := s3Service.ListObjects(context.Background(), &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	c.Assert(err, check.IsNil)
	c.Assert(len(result.Contents), check.Equals, 2)
	c.Assert(*result.Contents[0].Key, check.Equals, "test-prefix/file1.txt")
	c.Assert(*result.Contents[1].Key, check.Equals, "test-prefix/file2.txt")
}
