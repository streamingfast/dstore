package dstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

//
// Google Storage Store

type GSStore struct {
	baseURL     *url.URL
	client      *storage.Client
	userProject string
	*commonStore
}

func newGSStoreContext(ctx context.Context, baseURL *url.URL, extension, compressionType string, overwrite bool) (*GSStore, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	userProject := baseURL.Query().Get("project")

	client.SetRetry(storage.WithBackoff(gax.Backoff{}))

	return &GSStore{
		baseURL: baseURL,
		client:  client,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
		userProject: userProject,
	}, nil
}

func NewGSStore(baseURL *url.URL, extension, compressionType string, overwrite bool) (*GSStore, error) {
	ctx := context.Background()
	return newGSStoreContext(ctx, baseURL, extension, compressionType, overwrite)
}

func (s *GSStore) Clone(ctx context.Context) (Store, error) {
	return newGSStoreContext(ctx, s.baseURL, s.extension, s.compressionType, s.overwrite)
}

func (s *GSStore) SubStore(subFolder string) (Store, error) {
	url, err := url.Parse(s.baseURL.String())
	if err != nil {
		return nil, fmt.Errorf("gs store parsing base url: %w", err)
	}
	url.Path = path.Join(url.Path, subFolder)

	return &GSStore{
		baseURL:     url,
		client:      s.client,
		commonStore: s.commonStore,
		userProject: s.userProject,
	}, nil
}

func (s *GSStore) bucket() *storage.BucketHandle {
	if s.userProject != "" {
		return s.client.Bucket(s.baseURL.Host).UserProject(s.userProject)
	}
	return s.client.Bucket(s.baseURL.Host)
}

func (s *GSStore) BaseURL() *url.URL {
	return s.baseURL
}

func (s *GSStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), s.pathWithExt(name))
}

func (s *GSStore) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(s.baseURL.String(), "/"), strings.TrimLeft(s.pathWithExt(name), "/"))
}

func (s *GSStore) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), strings.TrimLeft(s.baseURL.Path, "/")+"/")
}

func (s *GSStore) CopyObject(ctx context.Context, src, dest string) error {
	srcPath := s.ObjectPath(src)
	srcObj := s.bucket().Object(srcPath)

	destPath := s.ObjectPath(dest)
	_, err := s.bucket().Object(destPath).CopierFrom(srcObj).Run(ctx)
	return err
}

func (s *GSStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	path := s.ObjectPath(base)

	object := s.bucket().Object(path)

	if !s.overwrite {
		object = object.If(storage.Conditions{DoesNotExist: true})
	}
	w := object.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	if err := s.compressedCopy(w, f); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		if s.overwrite {
			return err
		}
		return silencePreconditionError(err)
	}

	return nil
}

func silencePreconditionError(err error) error {
	if e, ok := err.(*googleapi.Error); ok {
		if e.Code == http.StatusPreconditionFailed {
			return nil
		}
	}
	return err
}

func (s *GSStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	if tracer.Enabled() {
		zlog.Debug("opening dstore file", zap.String("path", s.pathWithExt(name)))
	}
	reader, err := s.bucket().Object(path).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, ErrNotFound
		}

		return nil, err
	}

	out, err = s.uncompressedReader(reader)
	if tracer.Enabled() {
		out = wrapReadCloser(out, func() {
			zlog.Debug("closing dstore file", zap.String("path", s.pathWithExt(name)))
		})
	}
	return
}

func (s *GSStore) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	err := s.bucket().Object(path).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *GSStore) FileExists(ctx context.Context, base string) (bool, error) {
	path := s.ObjectPath(base)

	_, err := s.bucket().Object(path).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}

		return false, err
	}
	return true, nil
}

func (s *GSStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	path := s.ObjectPath(base)

	attrs, err := s.bucket().Object(path).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return &ObjectAttributes{
		LastModified: attrs.Updated,
		Size:         attrs.Size,
	}, nil
}

func (s *GSStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (s *GSStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, max)
}

func (s *GSStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {
	return s.WalkFrom(ctx, prefix, "", f)
}

func (s *GSStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	q := &storage.Query{}
	q.Prefix = strings.TrimLeft(s.baseURL.Path, "/") + "/"
	if prefix != "" {
		q.Prefix = filepath.Join(q.Prefix, prefix)
		// join cleans the string and will remove the trailing / in the prefix if present.
		// adding it back to prevent false positive matches
		if prefix[len(prefix)-1:] == "/" {
			q.Prefix = q.Prefix + "/"
		}
	}

	if startingPoint != "" {
		if !strings.HasPrefix(startingPoint, prefix) {
			return fmt.Errorf("starting point %q must start with prefix %q", startingPoint, prefix)
		}

		// "startingPoint" is known to start with "prefix" (checked when entering function), but our the prefix received do
		// not contain the "baseURL" which is required because it contains the "path" of the store. So we remove the
		// "original prefix" from the "startingPoint" and append it to the real "final" prefix instead.
		relativeStartingPoint := strings.TrimPrefix(startingPoint, prefix)

		q.StartOffset = filepath.Join(q.Prefix, relativeStartingPoint)
	}

	if tracer.Enabled() {
		zlog.Info("walking files from", zap.String("original_prefix", prefix), zap.String("prefix", q.Prefix), zap.String("start_offset", q.StartOffset))
	}

	it := s.bucket().Objects(ctx, q)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := f(s.toBaseName(attrs.Name)); err != nil {
			if errors.Is(err, StopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}
