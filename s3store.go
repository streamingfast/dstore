package dstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Store struct {
	baseURL  *url.URL
	basePath string
	service  *s3.S3
	uploader *s3manager.Uploader
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
	uploader := s3manager.NewUploader(sess)

	return &S3Store{
		baseURL:  baseURL,
		service:  s3Service,
		uploader: uploader,
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

func (s *S3Store) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	path := s.ObjectPath(base)

	exists, err := s.FileExists(ctx, base)
	if err != nil {
		return err
	}

	if !s.overwrite && exists {
		// We silently ignore when we ask not to overwrite
		return nil
	}

	pipeRead, pipeWrite := io.Pipe()
	writeDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer pipeWrite.Close()

		err := s.compressedCopy(f, pipeWrite)
		if err != nil {
			cancel()
		}
		writeDone <- err
	}()

	_, err = s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
		Body:   pipeRead,
	})
	if err != nil {
		if err2 := <-writeDone; err2 != nil {
			return fmt.Errorf("writing through pipe: %s", err2)
		}
		return fmt.Errorf("uploading to S3 through manager: %s", err)
	}

	return nil
}

func (s *S3Store) FileExists(ctx context.Context, base string) (bool, error) {
	path := s.ObjectPath(base)

	_, err := s.service.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (s *S3Store) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	reader, err := s.service.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
	})
	if err != nil {
		return nil, err
	}

	return s.uncompressedReader(reader.Body)
}

func (s *S3Store) Walk(ctx context.Context, prefix, _ string, f func(filename string) (err error)) error {
	targetPrefix := strings.TrimLeft(s.baseURL.Path, "/") + "/"
	if prefix != "" {
		targetPrefix = filepath.Join(targetPrefix, prefix)
		if prefix[len(prefix)-1:] == "/" {
			targetPrefix += "/"
		}
	}

	q := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.baseURL.Host),
		Prefix: &prefix,
	}

	var innerErr error
	err := s.service.ListObjectsV2PagesWithContext(ctx, q, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, el := range page.Contents {
			if err := f(s.toBaseName(*el.Key)); err != nil {
				if err == StopIteration {
					return false
				}
				innerErr = err
				return false
			}
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("listing objects: %s", err)
	}
	if innerErr != nil {
		return fmt.Errorf("processing object list: %s", innerErr)
	}

	return nil
}

func (s *S3Store) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), strings.TrimLeft(s.baseURL.Path, "/")+"/")
}

func (s *S3Store) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	_, err := s.service.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.baseURL.Host),
		Key:    &path,
	})
	return err
}

func (s *S3Store) PushLocalFile(ctx context.Context, localFile, toBaseName string) (err error) {
	return pushLocalFile(ctx, s, localFile, toBaseName)
}
func (s *S3Store) ListFiles(ctx context.Context, prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, ignoreSuffix, max)
}
