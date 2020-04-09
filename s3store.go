package dstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Store struct {
	baseURL  *url.URL
	basePath string
	service  *s3.S3
	context  context.Context

	*commonStore
}

func NewS3Store(baseURL *url.URL, extension, compressionType string, overwrite bool) (*S3Store, error) {
	region := baseURL.Query().Get("region")
	if region == "" {
		return nil, fmt.Errorf("specify s3 bucket like: s3://bucket/path?region=us-east-1")
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, fmt.Errorf("error fetching AWS session info from env: %s", err)
	}

	s3Service := s3.New(sess)

	return &S3Store{
		baseURL: baseURL,
		service: s3Service,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *S3Store) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), s.pathWithExt(name))
}

func (s *S3Store) WriteObject(ctx context.Context, base string, f io.ReadSeeker) (err error) {
	path := s.ObjectPath(base)

	exists, err := s.FileExists(ctx, base)
	if err != nil {
		return err
	}

	if !s.overwrite && exists {
		// We silently ignore when we ask not to overwrite
		return nil
	}

	_, err = s.service.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
		Body:   f,
	})
	return err
}

func (s *S3Store) FileExists(ctx context.Context, base string) (bool, error) {
	path := s.ObjectPath(base)

	head, err := s.service.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
	})
	if err != nil {
		return false, err
	}

	// TODO: check if that,s the right way to figure out if something exists!

	if head.LastModified == nil {
		return false, nil
	}

	return true, nil
}
