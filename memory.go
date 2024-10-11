package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

// MemoryStore is a store that keeps all data in memory. Useful for unit testing where a store is required.
// Work in progress, not all methods are implemented
type MemoryStore struct {
	*commonStore

	baseURL *url.URL

	data     map[string][]byte
	modified map[string]time.Time

	lock sync.RWMutex
}

func (m *MemoryStore) OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _, ok := m.data[name]; !ok {
		return nil, ErrNotFound
	}

	reader := io.NopCloser(bytes.NewReader(m.data[name]))
	out, err = m.uncompressedReader(ctx, reader)
	return
}

func (m *MemoryStore) WriteObject(ctx context.Context, base string, f io.Reader) (err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, exists := m.data[base]; !m.overwrite && exists {
		return nil
	}

	w := bytes.NewBuffer(nil)
	if err := m.compressedCopy(ctx, w, f); err != nil {
		return err
	}

	m.data[base] = w.Bytes()
	m.modified[base] = time.Now()

	return nil
}

func (m *MemoryStore) FileExists(_ context.Context, base string) (bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	_, exists := m.data[base]
	return exists, nil
}

func (m *MemoryStore) ObjectPath(name string) string {
	return path.Join(strings.TrimLeft(m.baseURL.Path, "/"), m.pathWithExt(name))
}

func (m *MemoryStore) ObjectURL(name string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(m.baseURL.String(), "/"), strings.TrimLeft(m.pathWithExt(name), "/"))
}

func (m *MemoryStore) ObjectAttributes(_ context.Context, base string) (*ObjectAttributes, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if !m.modified[base].IsZero() {
		return &ObjectAttributes{
			LastModified: m.modified[base],
			Size:         int64(len(m.data[base])),
		}, nil
	}

	return nil, ErrNotFound
}

func (m *MemoryStore) PushLocalFile(ctx context.Context, localFile, toBaseName string) (err error) {
	remove, err := pushLocalFile(ctx, m, localFile, toBaseName)
	if err != nil {
		return err
	}
	return remove()
}

func (m *MemoryStore) CopyObject(_ context.Context, src, dest string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.data[src]; !ok {
		return ErrNotFound
	}

	m.data[dest] = m.data[src]
	return nil
}

func (m *MemoryStore) WalkFrom(_ context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	panic("not yet supported for this store type")
}

func (m *MemoryStore) Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error {
	panic("not yet supported for this store type")
}

func (m *MemoryStore) ListFiles(ctx context.Context, prefix string, max int) ([]string, error) {
	panic("not yet supported for this store type")
}

func (m *MemoryStore) DeleteObject(ctx context.Context, base string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.data, base)
	delete(m.modified, base)
	return nil
}

func (m *MemoryStore) BaseURL() *url.URL {
	return &url.URL{}
}

func (m *MemoryStore) SubStore(subFolder string) (Store, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	newFiles := map[string][]byte{}
	newModified := map[string]time.Time{}

	for k, v := range m.data {
		if !strings.HasPrefix(k, subFolder) {
			continue
		}

		newFiles[strings.TrimPrefix(k, subFolder)] = v
		newModified[strings.TrimPrefix(k, subFolder)] = m.modified[k]
	}

	return &MemoryStore{
		commonStore: m.commonStore,
		baseURL:     m.baseURL,
		data:        newFiles,
		modified:    newModified,
	}, nil
}

func (m *MemoryStore) Clone(ctx context.Context, opts ...Option) (Store, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	ms, err := newMemoryStoreContext(ctx, m.baseURL, m.extension, m.compressionType, m.overwrite, opts...)
	if err != nil {
		return nil, err
	}

	ms.data = m.data
	ms.modified = m.modified

	return ms, nil
}

func NewMemoryStore(baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*MemoryStore, error) {
	return newMemoryStoreContext(context.Background(), baseURL, extension, compressionType, overwrite, opts...)
}

func newMemoryStoreContext(_ context.Context, baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*MemoryStore, error) {
	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	common := &commonStore{
		compressionType:           compressionType,
		extension:                 extension,
		overwrite:                 overwrite,
		uncompressedReadCallback:  conf.uncompressedReadCallback,
		compressedReadCallback:    conf.compressedReadCallback,
		uncompressedWriteCallback: conf.uncompressedWriteCallback,
		compressedWriteCallback:   conf.compressedWriteCallback,
	}

	return &MemoryStore{
		commonStore: common,
		baseURL:     baseURL,
		data:        map[string][]byte{},
		modified:    map[string]time.Time{},
	}, nil
}
