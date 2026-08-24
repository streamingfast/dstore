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
	SetMetadata(ctx context.Context, base string, metadata map[string]string) error

	WriteObject(ctx context.Context, base string, f io.Reader, metadataKeyValues ...string) (err error)
	PushLocalFile(ctx context.Context, localFile, toBaseName string) (err error)

	CopyObject(ctx context.Context, src, dest string) error
	Overwrite() bool
	SetOverwrite(enabled bool)

	// Walk recursively all files starting with the given prefix within this store. The `f` callback is invoked
	// for each file found.
	//
	// If you return `dstore.StopIteration` from your callback, iteration stops right away and `nil` will
	// returned by the `Walk` function. If your callback returns any error, iteration stops right away and
	// callback returned error is return by the `Walk` function.
	Walk(ctx context.Context, prefix string, f func(filename string) (err error)) error
	WalkFrom(ctx context.Context, prefix, inclusiveFrom string, f func(filename string) (err error)) error
	WalkFromTo(ctx context.Context, prefix, inclusiveFrom, exclusiveTo string, f func(filename string) (err error)) error

	// WalkAttributes walks like Walk, but yields the size and modification time the listing
	// already carried instead of the name alone. It obeys the same rules as Walk, including
	// StopIteration.
	WalkAttributes(ctx context.Context, prefix string, f func(entry ObjectEntry) error) error

	ListFiles(ctx context.Context, prefix string, max int) ([]string, error)

	// ListFolders returns the immediate sub-folders of prefix, each relative to the store and
	// ending with a "/", without reporting anything nested deeper. A negative max means
	// unlimited. prefix is the empty string for the root of the store, and otherwise a folder
	// path with or without its trailing "/".
	//
	// An object store has no folders of its own, only the prefixes its objects imply, so it
	// reports exactly the folders that hold at least one object. LocalStore has real
	// directories and reports them all, empty ones included.
	ListFolders(ctx context.Context, prefix string, max int) ([]string, error)

	// ListFoldersFromTo is ListFolders restricted to a slice of the key space: it returns only
	// the folders whose path is at or after inclusiveFrom and strictly before exclusiveTo, both
	// of which must start with prefix and either of which may be empty for "unbounded".
	//
	// A single folder listing is paged one round trip at a time however few folders come back,
	// so a caller holding tens of thousands of them can split the key space and list the slices
	// concurrently instead. Object stores push the bounds down to the service; the others
	// filter what they listed.
	ListFoldersFromTo(ctx context.Context, prefix, inclusiveFrom, exclusiveTo string, max int) ([]string, error)

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

	switch base.Scheme {
	case "gs":
		return NewGSStore(base, extension, compressionType, overwrite, opts...)
	case "az":
		return NewAzureStore(base, extension, compressionType, overwrite, opts...)
	case "s3":
		return NewS3Store(base, extension, compressionType, overwrite, opts...)
	case "file":
		return NewLocalStore(base, extension, compressionType, overwrite, opts...)
	case "memory":
		return NewMemoryStore(base, extension, compressionType, overwrite, opts...)
	case "":
		// If scheme is empty, let's assume baseURL was a absolute/relative path without being an actual URL
		return NewLocalStore(base, extension, compressionType, overwrite, opts...)
	}

	return nil, fmt.Errorf("store URL must begin with file:// (or local path without a scheme), gs://, az://, s3:// or memory://, received scheme %q", base.Scheme)
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
