package dstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

var ErrNotFound = errors.New("not found")

type Store interface {
	OpenObject(ctx context.Context, name string) (out io.ReadCloser, err error)
	FileExists(ctx context.Context, base string) (bool, error)

	ObjectPath(base string) string
	ObjectURL(base string) string
	ObjectAttributes(ctx context.Context, base string) (*ObjectAttributes, error)

	WriteObject(ctx context.Context, base string, f io.Reader) (err error)
	PushLocalFile(ctx context.Context, localFile, toBaseName string) (err error)

	CopyObject(ctx context.Context, src, dest string) error
	Overwrite() bool
	SetOverwrite(enabled bool)

	WalkFrom(ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error

	// Walk recursively all files starting with the given prefix within this store. The `f` callback is invoked
	// for each file found.
	//
	// If you return `dstore.StopIteration` from your callback, iteration stops right away and `nil` will
	// returned by the `Walk` function. If your callback returns any error, iteration stops right away and
	// callback returned error is return by the `Walk` function.
	Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error
	ListFiles(ctx context.Context, prefix string, max int) ([]string, error)

	DeleteObject(ctx context.Context, base string) error

	// Used to retrieve original query parameters, allowing further
	// configurability of the consumers of this store.
	BaseURL() *url.URL
	SubStore(subFolder string) (Store, error)

	// Deprecated: Use the Options to add callbacks to inject metering from the upstream code instead
	SetMeter(meter Meter)
}

type Clonable interface {
	Clone(ctx context.Context, opts ...Option) (Store, error)
}

var StopIteration = errors.New("stop iteration")

func NewDBinStore(baseURL string, opts ...Option) (Store, error) {
	return NewStore(baseURL, "dbin.zst", "zstd", false, opts...)
}

func NewJSONLStore(baseURL string, opts ...Option) (Store, error) {
	// Replaces NewSimpleArchiveStore() from before
	return NewStore(baseURL, "jsonl.gz", "gzip", false, opts...)
}

func NewSimpleStore(baseURL string, opts ...Option) (Store, error) {
	// Replaces NewSimpleGStore, and supports local store.
	return NewStore(baseURL, "", "", true, opts...)
}

// NewStore creates a new Store instance. The baseURL is always a directory, and does not end with a `/`.
func NewStore(baseURL, extension, compressionType string, overwrite bool, opts ...Option) (Store, error) {
	if strings.HasSuffix(baseURL, "/") {
		return nil, fmt.Errorf("baseURL shouldn't end with a /")
	}

	// WARN: if you were passing `jsonl` as an extension, you should now add `.gz` if you intend
	// to enable compression.
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	config := config{}
	for _, opt := range opts {
		opt.apply(&config)
	}

	if config.compression != "" {
		compressionType = config.compression
	}

	// file://superbob
	switch base.Scheme {
	case "gs":
		return NewGSStore(base, extension, compressionType, overwrite, opts...)
	case "az":
		return NewAzureStore(base, extension, compressionType, overwrite, opts...)
	case "s3":
		return NewS3Store(base, extension, compressionType, overwrite, opts...)
	case "file":
		return NewLocalStore(base, extension, compressionType, overwrite, opts...)
	case "":
		// If scheme is empty, let's assume baseURL was a absolute/relative path without being an actual URL
		return NewLocalStore(base, extension, compressionType, overwrite, opts...)
	}

	return nil, fmt.Errorf("archive store only supports, file://, gs:// or local path")
}

type config struct {
	compression string
	overwrite   bool

	compressedWriteCallback   func(ctx context.Context, size int)
	compressedReadCallback    func(ctx context.Context, size int)
	uncompressedWriteCallback func(ctx context.Context, size int)
	uncompressedReadCallback  func(ctx context.Context, size int)
}

type Option interface {
	apply(config *config)
}

type optionFunc func(config *config)

func (f optionFunc) apply(config *config) {
	f(config)
}

// Compression defines which kind of compression to use when creating the store
// instance.
//
// Valid `compressionType` values:
// - <empty>       No compression
// - zstd          Use ZSTD compression
// - gzip          Use GZIP compression
func Compression(compressionType string) Option {
	return optionFunc(func(config *config) {
		config.compression = compressionType
	})
}

// AllowOverwrite allow files to be overwritten when already exist at a given
// location.
func AllowOverwrite() Option {
	return optionFunc(func(config *config) {
		config.overwrite = true
	})
}

// WithCompressedReadCallback allows you to set a callback function that is invoked
// when a compressed read operation is performed.
func WithCompressedReadCallback(cb func(context.Context, int)) Option {
	return optionFunc(func(config *config) {
		config.compressedReadCallback = cb
	})
}

// WithUncompressedReadCallback allows you to set a callback function that is invoked
// when an uncompressed read operation is performed.
func WithUncompressedReadCallback(cb func(context.Context, int)) Option {
	return optionFunc(func(config *config) {
		config.uncompressedReadCallback = cb
	})
}

// WithCompressedWriteCallback allows you to set a callback function that is invoked
// when a compressed write operation is performed.
func WithCompressedWriteCallback(cb func(context.Context, int)) Option {
	return optionFunc(func(config *config) {
		config.compressedWriteCallback = cb
	})
}

// WithUncompressedWriteCallback allows you to set a callback function that is invoked
// when an uncompressed write operation is performed.
func WithUncompressedWriteCallback(cb func(context.Context, int)) Option {
	return optionFunc(func(config *config) {
		config.uncompressedWriteCallback = cb
	})
}

// Deprecated: Use NewStoreFromFileURL
var NewStoreFromURL = NewStoreFromFileURL

// NewStoreFromFileURL works against a full file URL to derive the store from it as well as
// the filename it points to. Use this method **only and only if** the input points to a file directly,
// if your input is to build a store, use NewStore instead.
//
// This is a shortcut helper function that make it simpler to get store from a single file
// url.
func NewStoreFromFileURL(fileURL string, opts ...Option) (store Store, filename string, err error) {
	var storeURL string
	if _, err := os.Stat(fileURL); !os.IsNotExist(err) {
		sanitizedURL := filepath.Clean(fileURL)
		filename = filepath.Base(sanitizedURL)
		storeURL = filepath.Dir(sanitizedURL)
	} else {
		url, err := url.Parse(fileURL)
		if err != nil {
			return store, "", fmt.Errorf("parse file url: %w", err)
		}

		filename = filepath.Base(url.Path)
		url.Path = strings.TrimSuffix(filepath.Dir(url.Path), "/")
		storeURL = url.String()
	}

	config := config{}
	for _, opt := range opts {
		opt.apply(&config)
	}

	store, err = NewStore(storeURL, "", config.compression, config.overwrite, opts...)
	if err != nil {
		return nil, filename, fmt.Errorf("open store: %w", err)
	}

	return store, filename, nil
}

// OpenObject directly opens the giving file URL by parsing the file url, extracting the
// path and the filename from it, creating the store interface, opening the object directly
// and returning all this.
//
// This is a shortcut helper function that make it simpler to get store from a single file
// url.
func OpenObject(ctx context.Context, fileURL string, opts ...Option) (out io.ReadCloser, store Store, filename string, err error) {
	store, filename, err = NewStoreFromFileURL(fileURL, opts...)
	if err != nil {
		err = fmt.Errorf("new store: %w", err)
		return
	}

	out, err = store.OpenObject(ctx, filename)
	return
}

// ReadObject directly reads the giving file URL by parsing the file url, extracting the
// path and the filename from it, creating the store interface, opening the object directly
// and returning all this.
//
// This is a shortcut helper function that make it simpler to get store from a single file
// url.
func ReadObject(ctx context.Context, fileURL string, opts ...Option) ([]byte, error) {
	reader, _, _, err := OpenObject(ctx, fileURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("open object: %w", err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
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
