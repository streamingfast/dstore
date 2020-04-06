package dstore

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Store struct {
	bucket  string
	service *s3.S3
	context context.Context

	*commonStore
}

func NewS3Store(bucket, region, extension, compressionType string, overwrite bool) (*S3Store, error) {
	ctx := context.Background()

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, fmt.Errorf("error fetching AWS session info from env: %s", err)
	}

	s3Service := s3.New(sess)

	return &S3Store{
		bucket:  bucket,
		service: s3Service,
		context: ctx,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *S3Store) WriteObject(filename string, f io.ReadSeeker) (err error) {
	_, err = s.service.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    &filename,
		Body:   f,
	})
	return err
}
