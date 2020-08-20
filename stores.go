package dstore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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

// Proposal: New interfaces for dstore

type walkQuery struct {
	prefix       string
	ignoreSuffix string
	max          int
}

type WalkOption interface {
	apply(query *walkQuery)
}

type PrefixedWith string

func (o PrefixedWith) apply(query *walkQuery) {
	query.prefix = string(o)
}

type IgnoreSuffixedWith string

func (o IgnoreSuffixedWith) apply(query *walkQuery) {
	query.ignoreSuffix = string(o)
}

type Limit int

func (o Limit) apply(query *walkQuery) {
	query.max = int(o)
}

type Store2 interface {
	Exists(ctx context.Context, name string) (bool, error)
	Delete(ctx context.Context, name string) error

	Reader(ctx context.Context, name string) (out io.ReadCloser, err error)
	Writer(ctx context.Context, name string) (out io.WriteCloser, err error)

	Walk(ctx context.Context, f func(filename string) (err error), options ...WalkOption) error
}

// Now helper functions (or still straight in the interface and we exposed a "BaseStore" that every implementation
// can embedded to reduce duplication). I kind of prefer the embedding for easier discovery, for example completion
// on a store instance will list all available methods straight. But this clutter the interface at the same time could
// be benefical for some specific implementation so they can provided "fast" path.
func ListFiles(ctx context.Context, store Store2, options ...WalkOption) ([]string, error) {
	panic("not implemented")
}

func ReadBytes(ctx context.Context, store Store2, name string) ([]byte, error) {
	reader, err := store.Reader(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

func Write(ctx context.Context, store Store2, name string, reader io.Reader) error {
	writer, err := store.Writer(ctx, name)
	if err != nil {
		return fmt.Errorf("writer: %w", err)
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	return err
}

func WriteBytes(ctx context.Context, store Store2, name string, content []byte) error {
	return Write(ctx, store, name, bytes.NewBuffer(content))
}

// End Proposals: New interfaces for dstore

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
	case "az":
		return NewAzureStore(base, extension, compressionType, overwrite)
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
