package dstore

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

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
	basePath := baseURL.Path
	if !strings.HasPrefix(basePath, "file://") {
		originalBasePath := basePath
		basePath = filepath.Clean(basePath)
		zlog.Info("sanitized base path", zap.String("original_base_path", originalBasePath), zap.String("sanitized_base_path", basePath))
	}

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
		baseURL:  baseURL,
		commonStore: &commonStore{
			compressionType: compressionType,
			extension:       extension,
			overwrite:       overwrite,
		},
	}, nil
}

func (s *LocalStore) BaseURL() *url.URL {
	return s.baseURL
}

func (s *LocalStore) ListFiles(ctx context.Context, prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, ignoreSuffix, max)
}

func (s *LocalStore) Walk(ctx context.Context, prefix, ignoreSuffix string, f func(filename string) (err error)) error {
	fullPath := s.basePath + "/"
	if prefix != "" {
		fullPath += prefix
	}

	walkPath := fullPath
	if !strings.HasSuffix(fullPath, "/") {
		// /my/path/0000 -> will walk /my/path, in case `0000` is the prefix of some files within
		walkPath = filepath.Dir(fullPath)
	}

	if traceEnabled {
		zlog.Debug("walking files", zap.String("walk_path", walkPath))
	}

	err := filepath.Walk(walkPath, func(infoPath string, info os.FileInfo, err error) error {
		if ignoreSuffix != "" && strings.HasSuffix(infoPath, ignoreSuffix) {
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
			if len(infoPath) >= len(fullPath) && !strings.HasPrefix(infoPath, fullPath) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(infoPath, fullPath) {
			return nil
		}

		if err := f(s.toBaseName(infoPath)); err != nil {
			if err == StopIteration {
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

		return s.compressedCopy(reader, file)
	}()
	if err != nil {
		return err
	}

	if err := os.Rename(tempPath, destPath); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	return nil
}

func (s *LocalStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	path := s.ObjectPath(name)

	file, err := os.Open(path)
	if err != nil {
		if strings.ContainsAny(err.Error(), "no such file or directory") {
			return nil, ErrNotFound
		}
		return nil, err
	}

	reader := NewBufferedFileReadCloser(file)
	return s.uncompressedReader(reader)
}

func (s *LocalStore) toBaseName(filename string) string {
	baseName := strings.TrimPrefix(strings.TrimSuffix(filename, s.pathWithExt("")), s.basePath)
	baseName = strings.TrimPrefix(baseName, "/")

	return baseName
}

func (s *LocalStore) ObjectPath(name string) string {
	return path.Join(s.basePath, s.pathWithExt(name))
}

func (s *LocalStore) DeleteObject(ctx context.Context, base string) error {
	path := s.ObjectPath(base)
	return os.Remove(path)
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

func (s *LocalStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) error {
	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}
