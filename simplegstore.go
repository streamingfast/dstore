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

	return &SimpleGStore{
		baseURL: base,
		client:  client,
		context: ctx,
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
	w := s.client.Bucket(s.baseURL.Host).Object(path).NewWriter(ctx)
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

	r, err := s.client.Bucket(s.baseURL.Host).Object(path).NewReader(s.context)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *SimpleGStore) ListFiles(prefix string, max int) (out []string, err error) {
	path := s.ObjectPath(prefix)
	ctx, cancel := s.Context()
	defer cancel()
	it := s.client.Bucket(s.baseURL.Host).Objects(ctx, &storage.Query{Prefix: path})

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
	_, err := s.client.Bucket(s.baseURL.Host).Object(path).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
