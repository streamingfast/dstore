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
	files           map[string][]byte
	shouldOverwrite bool

	// Add other methods as usage grows...
	PushLocalFileFunc func(ctx context.Context, localFile string, toBaseName string) (err error)
	WriteObjectFunc   func(ctx context.Context, base string, f io.Reader) error
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

func (s *MockStore) BaseURL() *url.URL {
	return &url.URL{Scheme: "mock", Path: "/mock"}
}

// WriteFiles dumps currently know file
func (m *MockStore) WriteFiles(toDirectory string) error {
	for name, content := range m.files {
		if err := ioutil.WriteFile(path.Join(toDirectory, name), content, os.ModePerm); err != nil {
			return fmt.Errorf("writing file %q: %w", name, err)
		}
	}

	return nil
}

// SetFile sets the content of a file. Set the value "err" to trigger
// an error when reading this file.
func (m *MockStore) SetFile(name string, content []byte) {
	isError := string(content) == "err"
	zlog.Debug("adding file", zap.String("name", name), zap.Int("content_length", len(content)), zap.Bool("is_error", isError))

	m.files[name] = content
}

func (m *MockStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	zlog.Debug("opening object", zap.String("name", name))

	content, exists := m.files[name]
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

func (m *MockStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	if m.WriteObjectFunc != nil {
		return m.WriteObjectFunc(ctx, base, f)
	}

	zlog.Debug("writing object", zap.String("name", base))
	content, exists := m.files[base]
	if !exists {
		zlog.Debug("writing object not found, creating new one", zap.String("name", base))
	} else {
		if !m.shouldOverwrite {
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

	m.files[base] = buffer.Bytes()

	zlog.Debug("wrote object", zap.String("name", base), zap.Int("content_length", len(m.files[base])))
	return nil
}

func (m *MockStore) ObjectPath(base string) string {
	return base
}

func (m *MockStore) ObjectURL(base string) string {
	return base
}

func (m *MockStore) DeleteObject(ctx context.Context, base string) error {
	zlog.Debug("deleting object", zap.String("name", base))
	delete(m.files, base)
	return nil
}

func (m *MockStore) FileExists(ctx context.Context, base string) (bool, error) {
	zlog.Debug("checking if file exists", zap.String("name", base))

	content, exists := m.files[base]
	if !exists {
		return false, nil
	}

	scnt := string(content)
	if scnt == "err" {
		return false, fmt.Errorf("%q errored", base)
	}
	return scnt != "err", nil
}

func (s *MockStore) ListFiles(ctx context.Context, prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(ctx, s, prefix, ignoreSuffix, max)
}

func (s *MockStore) SetOverwrite(in bool) {
	s.shouldOverwrite = in
	return
}

func (s *MockStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (m *MockStore) Walk(ctx context.Context, prefix, _ string, f func(filename string) error) error {
	zlog.Debug("walking files", zap.String("prefix", prefix))
	sortedFiles := m.sortedFiles()

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

func (m *MockStore) sortedFiles() []string {
	sortedFiles := make([]string, len(m.files))

	i := 0
	for file := range m.files {
		sortedFiles[i] = file
		i++
	}

	sort.Sort(sort.StringSlice(sortedFiles))
	return sortedFiles
}

func (m *MockStore) PushLocalFile(ctx context.Context, localFile string, toBaseName string) (err error) {
	if m.PushLocalFileFunc != nil {
		return m.PushLocalFileFunc(ctx, localFile, toBaseName)
	}

	remove, err := pushLocalFile(ctx, m, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (m *MockStore) Overwrite() bool {
	return m.shouldOverwrite
}
