package dstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type SimpleGStore struct {
	baseURL          *url.URL
	client           *storage.Client
	context          context.Context
	operationTimeout time.Duration
	userProject      string
}

func NewSimpleGStore(baseURL string) (*SimpleGStore, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	if base.Scheme != "gs" {
		return nil, fmt.Errorf("scheme doesn't start with `gs://`")
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	userProject := base.Query().Get("project")

	return &SimpleGStore{
		baseURL:     base,
		client:      client,
		context:     ctx,
		userProject: userProject,
	}, nil

}

func (s *SimpleGStore) Context() (ctx context.Context, cancel func()) {
	if s.operationTimeout == 0 {
		ctx, cancel = context.WithCancel(s.context)
	} else {
		ctx, cancel = context.WithTimeout(s.context, s.operationTimeout)
	}
	return
}

func (s *SimpleGStore) bucket() *storage.BucketHandle {
	if s.userProject != "" {
		return s.client.Bucket(s.baseURL.Host).UserProject(s.userProject)
	}
	return s.client.Bucket(s.baseURL.Host)
}

func (s *SimpleGStore) SetOperationTimeout(d time.Duration) {
	s.operationTimeout = d
}

func (s *SimpleGStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), name)
}

func (s *SimpleGStore) WriteObject(base string, f io.Reader) (err error) {
	path := s.ObjectPath(base)

	ctx, cancel := s.Context()
	defer cancel()
	w := s.bucket().Object(path).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

func (s *SimpleGStore) OpenObject(name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	r, err := s.bucket().Object(path).NewReader(s.context)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *SimpleGStore) ListFiles(prefix string, max int) (out []string, err error) {
	path := s.ObjectPath(prefix)
	ctx, cancel := s.Context()
	defer cancel()
	it := s.bucket().Objects(ctx, &storage.Query{Prefix: path})

	count := 0
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		count++

		if count > max {
			break
		}

		out = append(out, attrs.Name)
	}

	return
}

func (s *SimpleGStore) FileExists(base string) (bool, error) {
	path := s.ObjectPath(base)

	ctx, cancel := s.Context()
	defer cancel()
	_, err := s.bucket().Object(path).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
