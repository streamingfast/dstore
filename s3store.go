package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

var retryS3PushLocalFilesDelay time.Duration
var s3ReadAttempts = 1
var bufferedS3Read bool

func init() {
	retry := os.Getenv("DSTORE_S3_RETRY_PUSH_DELAY")
	if retry != "" {
		retryS3PushLocalFilesDelay, _ = time.ParseDuration(retry)
	}
	if os.Getenv("DSTORE_S3_BUFFERED_READ") == "true" {
		bufferedS3Read = true
	}

	readAttempts := os.Getenv("DSTORE_S3_READ_ATTEMPTS")
	if readAttempts != "" {
		attempts, _ := strconv.ParseUint(readAttempts, 10, 64)
		if attempts > 0 {
			s3ReadAttempts = int(attempts)
		}
	}
	zlog.Info("S3 storage configured",
		zap.Bool("buffered_read", bufferedS3Read),
		zap.Int("read_attempts", s3ReadAttempts),
		zap.Duration("retry_push_local_files_delay", retryS3PushLocalFilesDelay),
	)

}

type S3Store struct {
	baseURL *url.URL

	bucket   string
	path     string
	service  *s3.S3
	uploader *s3manager.Uploader
	context  context.Context

	*commonStore
}

func NewS3Store(baseURL *url.URL, extension, compressionType string, overwrite bool) (*S3Store, error) {
	s := &S3Store{
		baseURL: baseURL,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}

	awsConfig, bucket, path, err := ParseS3URL(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid s3 url: %w", err)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("error fetching AWS session info from env: %w", err)
	}

	s.service = s3.New(sess)
	s.uploader = s3manager.NewUploader(sess)
	s.bucket = bucket
	s.path = path

	return s, nil
}

func (s *S3Store) SubStore(subFolder string) (Store, error) {
	url, err := url.Parse(s.baseURL.String())
	if err != nil {
		return nil, fmt.Errorf("s3 store parsing base url: %w", err)
	}
	url.Path = path.Join(url.Path, subFolder)
	return NewS3Store(url, s.extension, s.compressionType, s.overwrite)
}

func ParseS3URL(s3URL *url.URL) (config *aws.Config, bucket string, path string, err error) {
	region := s3URL.Query().Get("region")
	if region == "" {
		return nil, "", "", fmt.Errorf("specify s3 bucket like: s3://bucket/path?region=us-east-1")
	}

	awsConfig := &aws.Config{
		Region: &region,
	}

	hasEndpoint := hasCustomEndpoint(s3URL)
	if hasEndpoint {
		awsConfig.Endpoint = aws.String(s3URL.Host)
		awsConfig.S3ForcePathStyle = aws.Bool(true)

		if s3URL.Query().Get("insecure") != "" {
			awsConfig.Endpoint = aws.String("http://" + *awsConfig.Endpoint)
			awsConfig.DisableSSL = aws.Bool(true)
		}

		pathParts := strings.Split(strings.TrimLeft(s3URL.Path, "/"), "/")

		bucket = pathParts[0]
		path = strings.Replace(s3URL.Path, bucket, "", 1)
	} else {
		bucket = s3URL.Hostname()
		path = s3URL.Path
	}

	accessKeyID := s3URL.Query().Get("access_key_id")
	secretAccessKey := s3URL.Query().Get("secret_access_key")
	if accessKeyID != "" && secretAccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	}

	return awsConfig, bucket, strings.Trim(path, "/"), nil
}

func hasCustomEndpoint(s3URL *url.URL) bool {
	// As soon as there is a port in the url, we are sure that's it's the
	// hostname that should be configured, so move along
	if s3URL.Port() != "" {
		return true
	}

	// If there is no `.` in the hostname, we assume it's a bucket. It could still be
	// problematic for `localhost`, we are expecting people to use `:<port>` to go
	// in the condition above.
	host := s3URL.Hostname()
	if !strings.Contains(host, ".") {
		return false
	}

	// Otherwise, by default we assume it's an hostname followed by the bucket. If
	// operator really intent to use the bucket directly an it contains dot, the
	// query parameter `infer_aws_endpoint=true` can be used to tell the store
	// implementation that the hostname is the actual bucket
	inferEndpoint := s3URL.Query().Get("infer_aws_endpoint")
	if inferEndpoint != "" {
		return false
	}

	return true
}

func (s *S3Store) BaseURL() *url.URL {
	return s.baseURL
}

func (s *S3Store) ObjectPath(name string) string {
	return path.Join(s.path, s.pathWithExt(name))
}

func (s *S3Store) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(s.baseURL.String(), "/"), strings.TrimLeft(s.pathWithExt(name), "/"))
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
		err := s.compressedCopy(f, pipeWrite)
		writeDone <- err
		pipeWrite.Close() // required to allow the uploader to complete

		if err != nil {
			cancel()
		}
	}()

	_, err = s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    &path,
		Body:   pipeRead,
	})
	if err != nil {
		select {
		case err2 := <-writeDone:
			if err2 != nil {
				return fmt.Errorf("writing through pipe: %w", err2)
			}
		default:
			// error was generated in the Upload (s3 or context timeout), compressedCopy is not finished,
			// we make it fail. double closing is safe here
			pipeWrite.Close()
		}
		return fmt.Errorf("uploading to S3 through manager: %w", err)
	}

	return nil
}

func (s *S3Store) FileExists(ctx context.Context, base string) (bool, error) {
	path := s.ObjectPath(base)

	_, err := s.service.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
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

func (s *S3Store) OpenObject(ctx context.Context, name string) (io.ReadCloser, error) {
	path := s.ObjectPath(name)

	var err error
	for i := 0; i < s3ReadAttempts; i++ {
		if i > 0 { // small wait on retry
			zlog.Warn("got an error on s3 OpenObject, retrying",
				zap.Error(err),
				zap.Int("attempt", i),
				zap.Int("max_attempts", s3ReadAttempts),
				zap.String("name", name),
				zap.String("path", path),
			)
			time.Sleep(500 * time.Millisecond)
		}
		var reader *s3.GetObjectOutput
		reader, err = s.service.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    &path,
		})
		if err != nil {
			if err.Error() == "no such key" {
				err = ErrNotFound
			}
			continue
		}
		if bufferedS3Read {
			var data []byte
			data, err = ioutil.ReadAll(reader.Body)
			if err != nil {
				continue
			}
			if err = reader.Body.Close(); err != nil {
				continue
			}
			return s.uncompressedReader(ioutil.NopCloser(bytes.NewReader(data)))
		}
		return s.uncompressedReader(reader.Body)
	}
	return nil, fmt.Errorf("s3 open object (%d attempts, buffered_read: %v): %w", s3ReadAttempts, bufferedS3Read, err)
}

func (s *S3Store) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (s *S3Store) Walk(ctx context.Context, prefix, _ string, f func(filename string) (err error)) error {
	targetPrefix := s.path
	if targetPrefix != "" {
		targetPrefix += "/"
	}
	if prefix != "" {
		targetPrefix = filepath.Join(targetPrefix, prefix)
		if prefix[len(prefix)-1:] == "/" {
			targetPrefix += "/"
		}
	}

	if tracer.Enabled() {
		zlog.Debug("walking files", zap.String("bucket", s.bucket), zap.String("prefix", targetPrefix))
	}

	q := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: &targetPrefix,
	}

	var innerErr error
	err := s.service.ListObjectsV2PagesWithContext(ctx, q, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, el := range page.Contents {
			filename := s.toBaseName(*el.Key)
			if filename == "" {
				zlog.Warn("got an empty filename from s3 store, ignoring it", zap.String("key", *el.Key))
				continue
			}
			if err := f(filename); err != nil {
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
		return fmt.Errorf("listing objects: %w", err)
	}
	if innerErr != nil {
		return fmt.Errorf("processing object list: %w", innerErr)
	}

	return nil
}

func (s *S3Store) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), s.path+"/")
}

func (s *S3Store) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	_, err := s.service.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    &path,
	})
	return err
}

func (s *S3Store) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if retryS3PushLocalFilesDelay != 0 {
		time.Sleep(retryS3PushLocalFilesDelay)
		exists, err := s.FileExists(ctx, toBaseName)
		if err != nil {
			zlog.Warn("just pushed file to dstore, but cannot check if it is still there after 500 milliseconds and retryS3PushLocalFiles is set", zap.Error(err))
			return err
		}
		if !exists {
			zlog.Warn("just pushed file to dstore, but it disappeared. Pushing again because retryS3PushLocalFiles is set", zap.String("dest basename", toBaseName))
			rem, err := pushLocalFile(ctx, s, localFile, toBaseName)
			if err != nil {
				return err
			}
			return rem()
		}
	}

	if err != nil {
		return err
	}
	return remove()
}

func (s *S3Store) ListFiles(ctx context.Context, prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, ignoreSuffix, max)
}
