package dstore

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type Store interface {
	OpenObject(name string) (out io.ReadCloser, err error)
	WriteObject(base string, f io.Reader) (err error)
	ObjectPath(base string) string
	FileExists(base string) (bool, error)
	PushLocalFile(localFile, toBaseName string) (err error)
	Overwrite() bool
	SetOverwrite(enabled bool)
	SetOperationTimeout(timeout time.Duration)
	Walk(prefix, ignoreSuffix string, f func(filename string) (err error)) error
	ListFiles(prefix, ignoreSuffix string, max int) ([]string, error)

	DeleteObject(base string) error
}

var StopIteration = errors.New("stop iteration")

func NewDBinStore(baseURL string) (Store, error) {
	return NewStore(baseURL, "dbin.zst", "zstd", false)
}

func NewJSONLStore(baseURL string) (Store, error) {
	// Replaces NewSimpleArchiveStore() from before
	return NewStore(baseURL, "jsonl.gz", "gzip", false)
}

func NewSimpleStore(baseURL string) (Store, error) {
	// Replaces NewSimpleGStore, and supports local store.
	return NewStore(baseURL, "", "", true)
}

// NewStore creates a new Store instance. The baseURL is always a directory, and does not end with a `/`.
func NewStore(baseURL, extension, compressionType string, overwrite bool) (Store, error) {
	if strings.HasSuffix(baseURL, "/") {
		return nil, fmt.Errorf("baseURL shouldn't end with a /")
	}

	// WARN: if you were passing `jsonl` as an extension, you should now add `.gz` if you intend
	// to enable compression.
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	if base.Scheme == "gs" {
		return NewGSStore(base, extension, compressionType, overwrite)
	}

	if base.Scheme == "file" {
		return NewLocalStore(base.Path, extension, compressionType, overwrite)
	}

	// If scheme is empty, let's assume baseURL was a absolute/relative path without being an actual URL
	if base.Scheme == "" {
		return NewLocalStore(baseURL, extension, compressionType, overwrite)
	}

	return nil, fmt.Errorf("archive store only supports, file://, gs:// or local path")
}

//
// Common Archive Store
//

type commonStore struct {
	extension       string
	compressionType string
	overwrite       bool
}

func (c *commonStore) Overwrite() bool      { return c.overwrite }
func (c *commonStore) SetOverwrite(in bool) { c.overwrite = in }

func (c *commonStore) pathWithExt(base string) string {
	if c.extension != "" {
		return base + "." + c.extension
	}
	return base
}

func pushLocalFile(store Store, localFile, toBaseName string) (err error) {
	f, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("opening local file %q: %s", localFile, err)
	}
	defer f.Close()

	objPath := store.ObjectPath(toBaseName)

	// The file doesn't exist, let's continue.
	err = store.WriteObject(toBaseName, f)
	if err != nil {
		return fmt.Errorf("writing %q to storage %q: %s", localFile, objPath, err)
	}

	if err = os.Remove(localFile); err != nil {
		return fmt.Errorf("error removing local file %q: %s", localFile, err)
	}

	return nil
}

//
// Google Storage Store

type GSStore struct {
	baseURL *url.URL
	client  *storage.Client
	context context.Context
	*commonStore
	operationTimeout time.Duration
}

func NewGSStore(baseURL *url.URL, extension, compressionType string, overwrite bool) (*GSStore, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GSStore{
		baseURL: baseURL,
		client:  client,
		context: ctx,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *GSStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), s.pathWithExt(name))
}

func (s *GSStore) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), strings.TrimLeft(s.baseURL.Path, "/")+"/")
}

func (s *GSStore) WriteObject(base string, f io.Reader) (err error) {
	path := s.ObjectPath(base)

	ctx, cancel := s.Context()
	defer cancel()

	object := s.client.Bucket(s.baseURL.Host).Object(path)

	if !s.overwrite {
		object = object.If(storage.Conditions{DoesNotExist: true})
	}
	w := object.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	switch s.compressionType {
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err := io.Copy(gw, f); err != nil {
			return err
		}
		if err := gw.Close(); err != nil {
			return err
		}
	case "zstd":
		zstdEncoder, err := zstd.NewWriter(w)
		if err != nil {
			return err
		}
		if _, err := io.Copy(zstdEncoder, f); err != nil {
			return err
		}
		if err := zstdEncoder.Close(); err != nil {
			return err
		}
	default:
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
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

func (s *GSStore) Context() (ctx context.Context, cancel func()) {
	if s.operationTimeout == 0 {
		ctx, cancel = context.WithCancel(s.context)
	} else {
		ctx, cancel = context.WithTimeout(s.context, s.operationTimeout)
	}
	return
}

func (s *GSStore) OpenObject(name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	reader, err := s.client.Bucket(s.baseURL.Host).Object(path).NewReader(s.context)
	if err != nil {
		return nil, err
	}

	switch s.compressionType {
	case "gzip":
		gzipReader, err := NewGZipReadCloser(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %s", err)
		}

		return gzipReader, nil
	case "zstd":
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create zstd reader: %s", err)
		}

		return zstdReader.IOReadCloser(), nil
	default:
		return reader, nil
	}
}

func (s *GSStore) DeleteObject(base string) error {
	path := s.ObjectPath(base)
	ctx, cancel := s.Context()
	defer cancel()
	return s.client.Bucket(s.baseURL.Host).Object(path).Delete(ctx)
}

func (s *GSStore) FileExists(base string) (bool, error) {
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

func (s *GSStore) PushLocalFile(localFile, toBaseName string) (err error) {
	return pushLocalFile(s, localFile, toBaseName)
}

func listFiles(store Store, prefix, ignoreSuffix string, max int) (out []string, err error) {
	var count int
	err = store.Walk(prefix, ignoreSuffix, func(filename string) error {
		count++
		if count > max {
			return StopIteration
		}

		out = append(out, filename)

		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}

func (s *GSStore) ListFiles(prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(s, prefix, ignoreSuffix, max)
}

func (s *GSStore) SetOperationTimeout(d time.Duration) {
	s.operationTimeout = d
}

func (s *GSStore) Walk(prefix, _ string, f func(filename string) (err error)) error {

	ctx, cancel := s.Context()
	defer cancel()
	q := &storage.Query{}
	q.Prefix = strings.TrimLeft(s.baseURL.Path, "/") + "/"
	if prefix != "" {

		q.Prefix = filepath.Join(q.Prefix, prefix)
		// join cleans the string and will remove the trailing / in the prefix is present.
		// adding it back to prevent false positive matches
		if prefix[len(prefix)-1:] == "/" {
			q.Prefix = q.Prefix + "/"
		}
	}
	it := s.client.Bucket(s.baseURL.Host).Objects(ctx, q)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := f(s.toBaseName(attrs.Name)); err != nil {
			if err == StopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

//
// Local Storage Store
//

type LocalStore struct {
	basePath string
	*commonStore
}

func NewLocalStore(basePath, extension, compressionType string, overwrite bool) (*LocalStore, error) {
	info, err := os.Stat(basePath)
	if err != nil {
		if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("unable to create base path %q: %s", basePath, err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("received base path is a file, expecting it to be a directory")
	}

	return &LocalStore{
		basePath: basePath,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *LocalStore) SetOperationTimeout(_ time.Duration) {
	zlog.Debug("setting operation timeout on localstore is a NOOP (not implemented)")
}

func (s *LocalStore) ListFiles(prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(s, prefix, ignoreSuffix, max)
}

func (s *LocalStore) Walk(prefix, ignoreSuffix string, f func(filename string) (err error)) error {
	fullPath := s.basePath + "/"
	if prefix != "" {
		fullPath += prefix
	}

	walkPath := fullPath
	if !strings.HasSuffix(fullPath, "/") {
		// /my/path/0000 -> will walk /my/path, in case `0000` is the prefix of some files within
		walkPath = filepath.Dir(fullPath)
	}

	err := filepath.Walk(walkPath, func(path string, info os.FileInfo, err error) error {
		if ignoreSuffix != "" && strings.HasSuffix(path, ignoreSuffix) {
			// Early exist to avoid races with half-written `.tmp`
			// files, that would error out with the `err != nil` check
			// below.  Only for local ones, as Google Storage-based
			// are atomic, they exist or they don't exist.
			return nil
		}
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		if info.IsDir() {
			if len(path) >= len(fullPath) && !strings.HasPrefix(path, fullPath) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(path, fullPath) {
			return nil
		}

		if err := f(s.toBaseName(info.Name())); err != nil {
			if err == StopIteration {
				return nil
			}
			return err
		}

		return nil
	})
	return err
}

func (s *LocalStore) WriteObject(base string, reader io.Reader) (err error) {
	destPath := s.ObjectPath(base)

	tempPath := destPath + ".tmp"

	targetDir := filepath.Dir(tempPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("ensuring directory exists (mkdir -p) %q: %s", targetDir, err)
	}

	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("unable to create file %q: %s", tempPath, err)
	}

	err = func() (err error) {
		defer func() {
			err = file.Close()
		}()

		switch s.compressionType {
		case "gzip":
			gzWriter := gzip.NewWriter(file)

			if _, err := io.Copy(gzWriter, reader); err != nil {
				return err
			}

			if err := gzWriter.Close(); err != nil {
				return err
			}
		case "zstd":
			zstdWriter, err := zstd.NewWriter(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(zstdWriter, reader); err != nil {
				return err
			}

			if err := zstdWriter.Close(); err != nil {
				return err
			}
		default:
			if _, err := io.Copy(file, reader); err != nil {
				return err
			}
		}

		return
	}()
	if err != nil {
		return err
	}

	if err := os.Rename(tempPath, destPath); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	return nil
}

func (s *LocalStore) OpenObject(name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := NewBufferedFileReadCloser(file)
	switch s.compressionType {
	case "gzip":
		gzipReader, err := NewGZipReadCloser(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %s", err)
		}

		return gzipReader, nil
	case "zstd":
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %s", err)
		}

		return zstdReader.IOReadCloser(), nil
	default:
		return reader, nil
	}
}

func (s *LocalStore) toBaseName(filename string) string {
	return strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), s.basePath)
}

func (s *LocalStore) ObjectPath(name string) string {
	return path.Join(s.basePath, s.pathWithExt(name))
}

func (s *LocalStore) DeleteObject(base string) error {
	path := s.ObjectPath(base)
	return os.Remove(path)
}

func (s *LocalStore) FileExists(base string) (bool, error) {
	path := s.ObjectPath(base)

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (s *LocalStore) PushLocalFile(localFile, toBaseName string) (err error) {
	return pushLocalFile(s, localFile, toBaseName)
}

//
// Buffered ReadCloser
//

type BufferedFileReadCloser struct {
	file   *os.File
	reader io.Reader
}

func NewBufferedFileReadCloser(file *os.File) *BufferedFileReadCloser {
	reader := bufio.NewReader(file)
	return &BufferedFileReadCloser{
		file:   file,
		reader: reader,
	}
}

func (readCloser *BufferedFileReadCloser) Read(p []byte) (n int, err error) {
	return readCloser.reader.Read(p)
}

func (readCloser *BufferedFileReadCloser) Close() error {
	return readCloser.file.Close()
}
