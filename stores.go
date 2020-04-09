package dstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

type Store interface {
	OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error)
	FileExists(ctx context.Context, base string) (bool, error)
	ObjectPath(base string) string

	WriteObject(ctx context.Context, base string, f io.Reader) (err error)
	PushLocalFile(ctx context.Context, localFile, toBaseName string) (err error)

	Overwrite() bool
	SetOverwrite(enabled bool)

	Walk(ctx context.Context, prefix, ignoreSuffix string, f func(filename string) (err error)) error
	ListFiles(ctx context.Context, prefix, ignoreSuffix string, max int) ([]string, error)

	DeleteObject(ctx context.Context, base string) error
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

	switch base.Scheme {
	case "gs":
		return NewGSStore(base, extension, compressionType, overwrite)
	case "s3":
		return NewS3Store(base, extension, compressionType, overwrite)
	case "file":
		return NewLocalStore(base.Path, extension, compressionType, overwrite)
	case "":
		// If scheme is empty, let's assume baseURL was a absolute/relative path without being an actual URL
		return NewLocalStore(baseURL, extension, compressionType, overwrite)
	}

	return nil, fmt.Errorf("archive store only supports, file://, gs:// or local path")
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
