package dstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
)

//
// Local Storage Store
//

type LocalStore struct {
	baseURL  *url.URL
	basePath string
	*commonStore
}

func NewLocalStore(baseURL *url.URL, extension, compressionType string, overwrite bool) (*LocalStore, error) {
	rand.Seed(time.Now().UnixNano())
	basePath := filepath.Clean(baseURL.Path)
	zlog.Debug("sanitized base path", zap.String("original_base_path", baseURL.Path), zap.String("sanitized_base_path", basePath))

	myBaseURL := *baseURL
	myBaseURL.Scheme = "file"

	info, err := os.Stat(basePath)
	if err != nil {
		if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("unable to create base path %q: %w", basePath, err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("received base path is a file, expecting it to be a directory")
	}

	return &LocalStore{
		basePath: basePath,
		baseURL:  &myBaseURL,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *LocalStore) Clone(ctx context.Context) (Store, error) {
	return NewLocalStore(s.baseURL, s.extension, s.compressionType, s.overwrite)
}

func (s *LocalStore) SubStore(subFolder string) (Store, error) {
	basePath := s.baseURL.Path
	newPath := path.Join(basePath, subFolder)
	url, err := url.Parse(newPath)
	if err != nil {
		return nil, fmt.Errorf("local store parsing base url: %w", err)
	}

	ls, err := NewLocalStore(url, s.extension, s.compressionType, s.overwrite)
	if err != nil {
		return nil, err
	}

	ls.meter = s.meter
	return ls, nil
}

func (s *LocalStore) BaseURL() *url.URL {
	return s.baseURL
}

func (s *LocalStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, max)
}

func (s *LocalStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (s *LocalStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {
	fullPath := s.basePath + string(os.PathSeparator)
	if prefix != "" {
		fullPath += prefix
	}

	walkPath := fullPath
	if !strings.HasSuffix(fullPath, string(os.PathSeparator)) {
		// /my/path/0000 -> will walk /my/path, in case `0000` is the prefix of some files within
		walkPath = filepath.Dir(fullPath)
	}

	if tracer.Enabled() {
		zlog.Debug("walking files", zap.String("walk_path", walkPath))
	}

	err := filepath.Walk(walkPath, func(infoPath string, info os.FileInfo, err error) error {
		if strings.HasSuffix(infoPath, ".tmp") {
			// Early exits to avoid races with half-written `.tmp`
			// files, that would error out with the `err != nil` check
			// below.  Only for local ones, as other stores are atomic.
			return nil
		}
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		if info.IsDir() {
			if len(infoPath) >= len(fullPath) && !strings.HasPrefix(infoPath, fullPath) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(infoPath, fullPath) {
			return nil
		}

		if err := f(s.toBaseName(infoPath)); err != nil {
			if errors.Is(err, StopIteration) {
				return nil
			}
			return err
		}

		return nil
	})
	return err
}

func (s *LocalStore) WriteObject(ctx context.Context, base string, reader io.Reader) (err error) {
	destPath := s.ObjectPath(base)

	tempPath := destPath + "." + randomString(8) + ".tmp"

	targetDir := filepath.Dir(tempPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("ensuring directory exists (mkdir -p) %q: %w", targetDir, err)
	}

	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("unable to create file %q: %w", tempPath, err)
	}

	if err := s.compressedCopy(file, reader); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, destPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func (s *LocalStore) CopyObject(ctx context.Context, src, dest string) error {
	reader, err := s.OpenObject(ctx, src)
	if err != nil {
		return err
	}
	defer reader.Close()

	return s.WriteObject(ctx, dest, reader)
}

func (s *LocalStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	if tracer.Enabled() {
		zlog.Debug("opening dstore file", zap.String("path", s.pathWithExt(name)))
	}

	file, err := os.Open(path)
	if err != nil {
		if strings.ContainsAny(err.Error(), "no such file or directory") {
			return nil, ErrNotFound
		}
		return nil, err
	}

	reader := NewBufferedFileReadCloser(file)
	out, err = s.uncompressedReader(reader)
	if tracer.Enabled() {
		out = wrapReadCloser(out, func() {
			zlog.Debug("closing dstore file", zap.String("path", s.pathWithExt(name)))
		})
	}
	return
}

func (s *LocalStore) toBaseName(filename string) string {
	baseName := strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), s.basePath)
	baseName = strings.TrimPrefix(strings.TrimPrefix(baseName, "\\"), "/")

	return baseName
}

func (s *LocalStore) ObjectPath(name string) string {
	return path.Join(s.basePath, s.pathWithExt(name))
}

func (s *LocalStore) ObjectURL(name string) string {
	// This is **not** converted to use os.PathSeparator, we assume all ObjectURL should be the same accross
	// all dstore implementations.
	return fmt.Sprintf("%s/%s", trimPathSeparatorSuffix(s.baseURL.String()), trimPathSeparatorPrefix(s.pathWithExt(name)))
}

func (s *LocalStore) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return ErrNotFound
	}
	return err
}

func (s *LocalStore) FileExists(ctx context.Context, base string) (bool, error) {
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

func (s *LocalStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	path := s.ObjectPath(base)

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &ObjectAttributes{
		LastModified: info.ModTime(),
		Size:         info.Size(),
	}, nil
}

func (s *LocalStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

var pathSeparatorCutset = "\\" + "/"

func trimPathSeparatorPrefix(in string) string {
	return strings.TrimLeft(in, pathSeparatorCutset)
}

func trimPathSeparatorSuffix(in string) string {
	return strings.TrimRight(in, pathSeparatorCutset)
}
