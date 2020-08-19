package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	"go.uber.org/zap"
)

type MockStore struct {
	files     map[string]*bytes.Buffer
	writeFunc func(base string, f io.Reader) (err error)

	shouldOverwrite bool
}

func NewMockStore(writeFunc func(base string, f io.Reader) (err error)) *MockStore {
	return &MockStore{
		files:     make(map[string]*bytes.Buffer),
		writeFunc: writeFunc,
	}
}

// WriteFiles dumps currently know file
func (m *MockStore) WriteFiles(toDirectory string) error {
	for name, buffer := range m.files {
		content, err := ioutil.ReadAll(buffer)
		if err != nil {
			return fmt.Errorf("read content of %q: %w", name, err)
		}

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

	m.files[name] = bytes.NewBuffer(content)
}

func (m *MockStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	zlog.Debug("opening object", zap.String("name", name))

	buffer := m.files[name]
	if buffer == nil {
		zlog.Debug("opening object not found", zap.String("name", name))
		return nil, io.EOF
	}

	if string(buffer.Bytes()) == "err" {
		zlog.Debug("opening object error", zap.String("name", name))
		return nil, fmt.Errorf("%s errored", name)
	}

	zlog.Debug("opened object", zap.String("name", name), zap.Int("content_length", buffer.Len()))
	return ioutil.NopCloser(buffer), nil
}

func (m *MockStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	if m.writeFunc != nil {
		return m.writeFunc(base, f)
	}

	zlog.Debug("writing object", zap.String("name", base))
	buffer := m.files[base]
	if buffer == nil {
		zlog.Debug("writing object not found, creating new one", zap.String("name", base))
		buffer = bytes.NewBuffer(nil)
		m.files[base] = buffer
	} else {
		if !m.shouldOverwrite {
			zlog.Debug("writing object not allowing overwrite", zap.String("name", base))
			return nil
		}

		zlog.Debug("writing object found, resetting it due to overwrite true", zap.String("name", base))
		buffer.Reset()
	}

	_, err = io.Copy(buffer, f)
	if err != nil {
		return fmt.Errorf("copy object to mock storage: %w", err)
	}

	zlog.Debug("wrote object", zap.String("name", base))
	return nil
}

func (m *MockStore) ObjectPath(base string) string {
	return base
}

func (m *MockStore) DeleteObject(ctx context.Context, base string) error {
	zlog.Debug("deleting object", zap.String("name", base))
	delete(m.files, base)
	return nil
}

func (m *MockStore) FileExists(ctx context.Context, base string) (bool, error) {
	zlog.Debug("checking if file exists", zap.String("name", base))

	buffer := m.files[base]
	if buffer == nil {
		return false, nil
	}

	scnt := string(buffer.Bytes())
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

func (m *MockStore) Walk(ctx context.Context, prefix, _ string, f func(filename string) error) error {
	zlog.Debug("walking files", zap.String("prefix", prefix))
	sortedFiles := m.sortedFiles()

	for _, file := range sortedFiles {
		zlog.Debug("walking file", zap.String("file", file), zap.Bool("has_prefix", strings.HasPrefix(file, prefix)))
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
	return nil
}

func (m *MockStore) Overwrite() bool {
	return m.shouldOverwrite
}
