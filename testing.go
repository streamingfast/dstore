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
	"sort"
	"strings"

	"go.uber.org/zap"
)

type MockStore struct {
	files             map[string][]byte
	shouldOverwrite   bool
	OpenObjectFunc    func(ctx context.Context, name string) (out io.ReadCloser, err error)
	WriteObjectFunc   func(ctx context.Context, base string, f io.Reader) error
	CopyObjectFunc    func(ctx context.Context, src, dest string) error
	DeleteObjectFunc  func(ctx context.Context, base string) error
	FileExistsFunc    func(ctx context.Context, base string) (bool, error)
	ListFilesFunc     func(ctx context.Context, prefix string, max int) ([]string, error)
	WalkFunc          func(ctx context.Context, prefix string, f func(filename string) error) error
	PushLocalFileFunc func(ctx context.Context, localFile string, toBaseName string) (err error)
}

func NewMockStore(writeFunc func(base string, f io.Reader) (err error)) *MockStore {
	store := &MockStore{files: make(map[string][]byte)}
	if writeFunc != nil {
		store.WriteObjectFunc = func(ctx context.Context, base string, f io.Reader) error {
			return writeFunc(base, f)
		}
	}

	return store
}

func (s *MockStore) SubStore(subFolder string) (Store, error) {
	newFiles := map[string][]byte{}
	for k, v := range s.files {
		newFiles[subFolder+"/"+k] = v
	}

	return &MockStore{
		files:             newFiles,
		shouldOverwrite:   s.shouldOverwrite,
		OpenObjectFunc:    s.OpenObjectFunc,
		WriteObjectFunc:   s.WriteObjectFunc,
		CopyObjectFunc:    s.CopyObjectFunc,
		DeleteObjectFunc:  s.DeleteObjectFunc,
		FileExistsFunc:    s.FileExistsFunc,
		ListFilesFunc:     s.ListFilesFunc,
		WalkFunc:          s.WalkFunc,
		PushLocalFileFunc: s.PushLocalFileFunc,
	}, nil
}

func (s *MockStore) BaseURL() *url.URL {
	return &url.URL{Scheme: "mock", Path: "/mock"}
}

// WriteFiles dumps currently know file
func (s *MockStore) WriteFiles(toDirectory string) error {
	for name, content := range s.files {
		if err := ioutil.WriteFile(path.Join(toDirectory, name), content, os.ModePerm); err != nil {
			return fmt.Errorf("writing file %q: %w", name, err)
		}
	}

	return nil
}

// SetFile sets the content of a file. Set the value "err" to trigger
// an error when reading this file.
func (s *MockStore) SetFile(name string, content []byte) {
	isError := string(content) == "err"
	zlog.Debug("adding file", zap.String("name", name), zap.Int("content_length", len(content)), zap.Bool("is_error", isError))

	s.files[name] = content
}

func (s *MockStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	if s.OpenObjectFunc != nil {
		return s.OpenObjectFunc(ctx, name)
	}

	zlog.Debug("opening object", zap.String("name", name))

	content, exists := s.files[name]
	if !exists {
		zlog.Debug("opening object not found", zap.String("name", name))
		return nil, io.EOF
	}

	if string(content) == "err" {
		zlog.Debug("opening object error", zap.String("name", name))
		return nil, fmt.Errorf("%s errored", name)
	}

	zlog.Debug("opened object", zap.String("name", name), zap.Int("content_length", len(content)))
	return ioutil.NopCloser(bytes.NewReader(content)), nil

}

func (s *MockStore) CopyObject(ctx context.Context, src, dest string) error {
	if s.CopyObjectFunc != nil {
		return s.CopyObjectFunc(ctx, src, dest)
	}
	reader, err := s.OpenObject(ctx, src)
	if err != nil {
		return err
	}
	defer reader.Close()

	return s.WriteObject(ctx, dest, reader)
}

func (s *MockStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	if s.WriteObjectFunc != nil {
		return s.WriteObjectFunc(ctx, base, f)
	}

	zlog.Debug("writing object", zap.String("name", base))
	content, exists := s.files[base]
	if !exists {
		zlog.Debug("writing object not found, creating new one", zap.String("name", base))
	} else {
		if !s.shouldOverwrite {
			zlog.Debug("writing object not allowing overwrite", zap.String("name", base))
			return nil
		}

		zlog.Debug("writing object found, resetting it due to overwrite true", zap.String("name", base), zap.Int("content_length", len(content)))
	}

	buffer := bytes.NewBuffer(nil)
	_, err = io.Copy(buffer, f)
	if err != nil {
		return fmt.Errorf("copy object to mock storage: %w", err)
	}

	s.files[base] = buffer.Bytes()

	zlog.Debug("wrote object", zap.String("name", base), zap.Int("content_length", len(s.files[base])))
	return nil
}

func (s *MockStore) ObjectPath(base string) string {
	return base
}

func (s *MockStore) ObjectURL(base string) string {
	return base
}

func (s *MockStore) DeleteObject(ctx context.Context, base string) error {
	if s.DeleteObjectFunc != nil {
		return s.DeleteObjectFunc(ctx, base)
	}

	zlog.Debug("deleting object", zap.String("name", base))
	delete(s.files, base)
	return nil
}

func (s *MockStore) FileExists(ctx context.Context, base string) (bool, error) {
	if s.FileExistsFunc != nil {
		return s.FileExistsFunc(ctx, base)
	}

	zlog.Debug("checking if file exists", zap.String("name", base))

	content, exists := s.files[base]
	if !exists {
		return false, nil
	}

	scnt := string(content)
	if scnt == "err" {
		return false, fmt.Errorf("%q errored", base)
	}
	return scnt != "err", nil
}

func (s *MockStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	if s.ListFilesFunc != nil {
		return s.ListFilesFunc(ctx, prefix, max)
	}

	return listFiles(ctx, s, prefix, max)
}

func (s *MockStore) SetOverwrite(in bool) {
	s.shouldOverwrite = in
	return
}

func (s *MockStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (s *MockStore) Walk(ctx context.Context, prefix string, f func(filename string) error) error {
	if s.WalkFunc != nil {
		return s.WalkFunc(ctx, prefix, f)
	}

	zlog.Debug("walking files", zap.String("prefix", prefix))
	sortedFiles := s.sortedFiles()

	for _, file := range sortedFiles {
		zlog.Debug("walking file", zap.String("file", file), zap.Bool("has_prefix", strings.HasPrefix(file, prefix)))
		if strings.Contains(file, "err") {
			return fmt.Errorf("mock err, %s", file)
		}
		if strings.HasPrefix(file, prefix) {
			if err := f(file); err != nil {
				if err == StopIteration {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (s *MockStore) sortedFiles() []string {
	sortedFiles := make([]string, len(s.files))

	i := 0
	for file := range s.files {
		sortedFiles[i] = file
		i++
	}

	sort.Sort(sort.StringSlice(sortedFiles))
	return sortedFiles
}

func (s *MockStore) PushLocalFile(ctx context.Context, localFile string, toBaseName string) (err error) {
	if s.PushLocalFileFunc != nil {
		return s.PushLocalFileFunc(ctx, localFile, toBaseName)
	}

	remove, err := pushLocalFile(ctx, s, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (s *MockStore) Overwrite() bool {
	return s.shouldOverwrite
}
