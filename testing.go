package dstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

type MockStore struct {
	files     map[string][]byte
	writeFunc func(base string, f io.Reader) (err error)
}

func NewMockStore(writeFunc func(base string, f io.Reader) (err error)) *MockStore {
	return &MockStore{
		files:     make(map[string][]byte),
		writeFunc: writeFunc,
	}
}

// SetFile sets the content of a file. Set the value "err" to trigger
// an error when reading this file.
func (m *MockStore) SetFile(name string, content []byte) {
	m.files[name] = content
}

func (m *MockStore) OpenObject(name string) (out io.ReadCloser, err error) {
	cnt := m.files[name]
	if string(cnt) == "err" {
		return nil, fmt.Errorf("%s errored", name)
	}

	return ioutil.NopCloser(bytes.NewBuffer(cnt)), nil
}

func (m *MockStore) WriteObject(base string, f io.Reader) (err error) {
	if m.writeFunc != nil {
		return m.writeFunc(base, f)
	}
	return nil
}

func (m *MockStore) ObjectPath(base string) string {
	return base
}

func (m *MockStore) DeleteObject(base string) error {
	delete(m.files, base)
	return nil
}

func (m *MockStore) FileExists(base string) (bool, error) {
	cnt := m.files[base]
	scnt := string(cnt)
	if scnt == "err" {
		return false, fmt.Errorf("%q errored", base)
	}
	return scnt != "err" && scnt != "", nil
}

func (s *MockStore) ListFiles(prefix, ignoreSuffix string, max int) ([]string, error) {
	return listFiles(s, prefix, ignoreSuffix, max)
}

func (s *MockStore) SetOverwrite(in bool) {
	return
}

func (m *MockStore) SetOperationTimeout(_ time.Duration) {
}

func (m *MockStore) Walk(prefix, _ string, f func(filename string) error) error {
	for i := range m.files {
		if strings.HasPrefix(i, prefix) {
			if err := f(i); err != nil {
				if err == StopIteration {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (m *MockStore) PushLocalFile(localFile string, toBaseName string) (err error) {
	return nil
}

func (m *MockStore) Overwrite() bool {
	return false
}
