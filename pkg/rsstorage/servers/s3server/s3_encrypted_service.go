package s3server

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3crypto"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type encryptedS3Service struct {
	defaultS3Wrapper
	keyID string
}

func (s *encryptedS3Service) encryptionClient() (*s3crypto.EncryptionClientV2, error) {
	kmsClient := kms.New(s.session)
	// Create the KeyProvider
	var matdesc s3crypto.MaterialDescription
	handler := s3crypto.NewKMSContextKeyGenerator(kmsClient, s.keyID, matdesc)

	// Create an encryption and decryption client
	// We need to pass the session here so S3 can use it. In addition, any decryption that
	// occurs will use the KMS client.
	svc, err := s3crypto.NewEncryptionClientV2(s.session, s3crypto.AESGCMContentCipherBuilderV2(handler))
	if err != nil {
		return nil, err
	}

	return svc, err
}

func (s *encryptedS3Service) decryptionClient() (*s3crypto.DecryptionClientV2, error) {
	kmsClient := kms.New(s.session)
	cr := s3crypto.NewCryptoRegistry()
	if err := s3crypto.RegisterAESGCMContentCipher(cr); err != nil {
		return nil, err
	}

	if err := s3crypto.RegisterKMSContextWrapWithAnyCMK(cr, kmsClient); err != nil {
		return nil, err
	}

	// Create a decryption client to decrypt artifacts
	svc, err := s3crypto.NewDecryptionClientV2(s.session, cr)
	if err != nil {
		return nil, err
	}

	return svc, err
}

// Upload takes the same input as the defaultS3Service *s3manager.UploadInput, and converts it to a PutObjectInput
// since the EncryptionClient does not have an equivalent to s3manager today. This means it will potentially upload
// content more slowly, and in an unoptimized format.
func (s *encryptedS3Service) Upload(input *s3manager.UploadInput, ctx context.Context, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	svc, err := s.encryptionClient()
	if err != nil {
		return nil, err
	}

	// The s3crypto V2 client requires an io.ReadSeeker for signing and content-length purposes, but we use an
	// io.PipeReader under the hood here. As a first pass for this functionality, we'll have to add two caveats to
	// the docs:
	// 1. Higher memory requirements and
	// 2. May be slower as the AWS SDK doesn't currently support the same optimizations for encrypted payloads
	b, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, fmt.Errorf("encryptedS3Service.Upload: Failed to read body %w", err)
	}

	putObjectInput := &s3.PutObjectInput{
		Key:    input.Key,
		Bucket: input.Bucket,
		Body:   bytes.NewReader(b),
	}

	output, err := svc.PutObjectWithContext(ctx, putObjectInput)
	if err != nil {
		return nil, fmt.Errorf("encryptedS3Service.Upload: Something went wrong uploading to S3. You may want to check your configuration, error: %s", err.Error())
	}

	// Pass the supported fields from the PutObjectOutput to the UploadOutput format.
	s3managerOutput := &s3manager.UploadOutput{
		VersionID: output.VersionId,
		ETag:      output.ETag,
	}

	return s3managerOutput, nil
}

// GetObject downloads an encrypted file from S3 and returns the plaintext value using client-side KMS decryption
func (s *encryptedS3Service) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	svc, err := s.decryptionClient()
	if err != nil {
		return nil, err
	}

	out, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, err
			} else {
				return nil, fmt.Errorf("encryptedS3Service.GetObject: Something went wrong getting an object from S3. You may want to check your configuration, error: %s", err.Error())
			}
		}
	}
	return out, err
}
