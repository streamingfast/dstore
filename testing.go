package dstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
)

type MockStore struct {
	OpenObjectFunc       func(ctx context.Context, name string) (out io.ReadCloser, err error)
	WriteObjectFunc      func(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) error
	CopyObjectFunc       func(ctx context.Context, src, dest string) error
	DeleteObjectFunc     func(ctx context.Context, base string) error
	FileExistsFunc       func(ctx context.Context, base string) (bool, error)
	ObjectAttributesFunc func(ctx context.Context, base string) (*ObjectAttributes, error)
	SetMetadataFunc      func(ctx context.Context, base string, metadata map[string]string) error
	ListFilesFunc        func(ctx context.Context, prefix string, max int) ([]string, error)
	WalkFunc             func(ctx context.Context, prefix string, f func(filename string) error) error
	WalkFromFunc         func(ctx context.Context, prefix, startingPoint string, f func(filename string) error) error
	WalkFromToFunc       func(ctx context.Context, prefix, startingPoint, exclusiveEndPoint string, f func(filename string) error) error
	PushLocalFileFunc    func(ctx context.Context, localFile string, toBaseName string) (err error)

	Files           map[string][]byte
	Metadata        map[string]map[string]string
	shouldOverwrite bool
}

func NewMockStore(writeFunc func(base string, f io.Reader) (err error)) *MockStore {
	store := &MockStore{
		Files:    make(map[string][]byte),
		Metadata: make(map[string]map[string]string),
	}
	if writeFunc != nil {
		store.WriteObjectFunc = func(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) error {
			return writeFunc(base, f)
		}
	}

	return store
}

func (s *MockStore) SubStore(subFolder string) (Store, error) {
	newFiles := map[string][]byte{}
	for k, v := range s.Files {
		prefix := filepath.Join(subFolder, "") + string(filepath.Separator)
		if strings.HasPrefix(k, prefix) {
			newFiles[strings.TrimPrefix(k, prefix)] = v
		}
	}

	return &MockStore{
		Files:             newFiles,
		Metadata:          make(map[string]map[string]string),
		shouldOverwrite:   s.shouldOverwrite,
		OpenObjectFunc:    s.OpenObjectFunc,
		WriteObjectFunc:   s.WriteObjectFunc,
		CopyObjectFunc:    s.CopyObjectFunc,
		DeleteObjectFunc:  s.DeleteObjectFunc,
		FileExistsFunc:    s.FileExistsFunc,
		SetMetadataFunc:   s.SetMetadataFunc,
		ListFilesFunc:     s.ListFilesFunc,
		WalkFunc:          s.WalkFunc,
		WalkFromFunc:      s.WalkFromFunc,
		WalkFromToFunc:    s.WalkFromToFunc,
		PushLocalFileFunc: s.PushLocalFileFunc,
	}, nil
}

func (s *MockStore) BaseURL() *url.URL {
	return &url.URL{Scheme: "mock", Path: "/mock"}
}

// WriteFiles dumps currently know file
func (s *MockStore) WriteFiles(toDirectory string) error {
	for name, content := range s.Files {
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

	s.Files[name] = content
}

func (s *MockStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	if s.OpenObjectFunc != nil {
		return s.OpenObjectFunc(ctx, name)
	}

	zlog.Debug("opening object", zap.String("name", name))

	content, exists := s.Files[name]
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

func (s *MockStore) WriteObject(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) (err error) {
	if s.WriteObjectFunc != nil {
		return s.WriteObjectFunc(ctx, base, f, metadataKeyValues...)
	}

	// Parse metadataKeyValues array
	if len(metadataKeyValues)%2 != 0 {
		return fmt.Errorf("metadataKeyValues must have an even number of strings (key-value pairs), got %d", len(metadataKeyValues))
	}

	zlog.Debug("writing object", zap.String("name", base))
	content, exists := s.Files[base]
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

	s.Files[base] = buffer.Bytes()

	// Set metadata if provided
	if len(metadataKeyValues) > 0 {
		metadata := make(map[string]string)
		for i := 0; i < len(metadataKeyValues); i += 2 {
			key := metadataKeyValues[i]
			value := metadataKeyValues[i+1]
			metadata[key] = value
		}
		s.Metadata[base] = metadata
	}

	zlog.Debug("wrote object", zap.String("name", base), zap.Int("content_length", len(s.Files[base])))
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
	delete(s.Files, base)
	delete(s.Metadata, base)
	return nil
}

func (s *MockStore) FileExists(ctx context.Context, base string) (bool, error) {
	if s.FileExistsFunc != nil {
		return s.FileExistsFunc(ctx, base)
	}

	zlog.Debug("checking if file exists", zap.String("name", base))

	content, exists := s.Files[base]
	if !exists {
		return false, nil
	}

	scnt := string(content)
	if scnt == "err" {
		return false, fmt.Errorf("%q errored", base)
	}
	return scnt != "err", nil
}

func (s *MockStore) ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error) {
	if s.ObjectAttributesFunc != nil {
		return s.ObjectAttributesFunc(ctx, base)
	}

	content, exists := s.Files[base]
	if !exists {
		return nil, ErrNotFound
	}

	return &ObjectAttributes{
		Size:     int64(len(content)),
		Metadata: s.Metadata[base],
	}, nil
}

func (s *MockStore) SetMetadata(ctx context.Context, base string, metadata map[string]string) error {
	if s.SetMetadataFunc != nil {
		return s.SetMetadataFunc(ctx, base, metadata)
	}

	_, exists := s.Files[base]
	if !exists {
		return ErrNotFound
	}

	s.Metadata[base] = metadata
	return nil
}

func (s *MockStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	if s.ListFilesFunc != nil {
		return s.ListFilesFunc(ctx, prefix, max)
	}

	return listFiles(ctx, s, prefix, max)
}

func (s *MockStore) SetOverwrite(in bool) {
	s.shouldOverwrite = in
}

func (s *MockStore) WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	if s.WalkFromFunc != nil {
		return s.WalkFromFunc(ctx, prefix, startingPoint, f)
	}
	return commonWalkFrom(s, ctx, prefix, startingPoint, f)
}

func (s *MockStore) WalkFromTo(ctx context.Context, prefix, startingPoint, exclusiveEndPoint string, f func(filename string) (err error)) error {
	if s.WalkFromToFunc != nil {
		return s.WalkFromToFunc(ctx, prefix, startingPoint, exclusiveEndPoint, f)
	}
	return commonWalkFromTo(s, ctx, prefix, startingPoint, exclusiveEndPoint, f)
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
				if errors.Is(err, StopIteration) {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (s *MockStore) sortedFiles() []string {
	sortedFiles := make([]string, len(s.Files))

	i := 0
	for file := range s.Files {
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
