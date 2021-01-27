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

	"go.uber.org/zap"
)

var ErrNotFound = errors.New("not found")

type Store interface {
	// Used to retrieve original query parameters, allowing further
	// configurability of the consumers of this store.
	BaseURL() *url.URL

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
	case "az":
		return NewAzureStore(base, extension, compressionType, overwrite)
	case "s3":
		return NewS3Store(base, extension, compressionType, overwrite)
	case "file":
		return NewLocalStore(base, extension, compressionType, overwrite)
	case "":
		// If scheme is empty, let's assume baseURL was a absolute/relative path without being an actual URL
		return NewLocalStore(base, extension, compressionType, overwrite)
	}

	return nil, fmt.Errorf("archive store only supports, file://, gs:// or local path")
}

type config struct {
	compression string
	overwrite   bool
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

// NewStoreFromURL is similar from `NewStore` but infer the store URL path from the URL directly
// extracting the filename along the way. The store's path is always the directory containing the file
// itself.
//
// This is a shortcut helper function that make it simpler to get store from a single file
// url.
func NewStoreFromURL(fileURL string, opts ...Option) (store Store, filename string, err error) {
	var storeURL string
	if _, err := os.Stat(fileURL); !os.IsNotExist(err) {
		zlog.Info("file url is a local existing file")
		sanitizedURL := filepath.Clean(fileURL)
		filename = filepath.Base(sanitizedURL)
		storeURL = filepath.Dir(sanitizedURL)
	} else {
		zlog.Info("file url assumed to be a store url with a scheme")
		url, err := url.Parse(fileURL)
		if err != nil {
			return store, "", fmt.Errorf("parse file url: %w", err)
		}

		filename = filepath.Base(url.Path)
		url.Path = strings.TrimSuffix(filepath.Dir(url.Path), "/")
		storeURL = url.String()
	}

	zlog.Info("creating store", zap.String("store_url", storeURL), zap.String("filename", filename))
	config := config{}
	for _, opt := range opts {
		opt.apply(&config)
	}

	store, err = NewStore(storeURL, "", config.compression, config.overwrite)
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
	store, filename, err = NewStoreFromURL(fileURL, opts...)
	if err != nil {
		err = fmt.Errorf("new store: %w", err)
		return
	}

	out, err = store.OpenObject(ctx, filename)
	return
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
