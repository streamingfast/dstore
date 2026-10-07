package dstore

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// StoreCompression is how a store compresses its objects and names its files.
type StoreCompression struct {
	// Type is "zstd", "gzip" or empty for no compression.
	Type string

	// Extension is appended to every object name, after a dot.
	Extension string

	// ZstdOptions are the encoder options of a zstd store, from compression_config.
	ZstdOptions []zstd.EOption
}

// ResolveCompression returns the compression and extension of a store created
// at baseURL with the given default extension and compression type.
//
// The default compression type is overridden by the Compression option, which
// is itself overridden by the `compression` query parameter of baseURL (`zstd`,
// `gzip` or `none`).
//
// The `extension` query parameter overrides the default extension, without its
// leading dot (`extension=dbin.gz`). The extension never follows the
// compression by itself: a store overriding one usually overrides both.
//
// The `compression_config` query parameter sets the zstd encoder level and
// window as `<level>` or `<level>/<window MiB>` (`best`, `better/32`,
// `best/64`). It is an error on a store that does not compress with zstd.
func ResolveCompression(baseURL *url.URL, extension, compressionType string, opts ...Option) (*StoreCompression, error) {
	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	resolved := compressionType
	if conf.compression != "" {
		resolved = conf.compression
	}

	query := baseURL.Query()
	switch value := query.Get("compression"); value {
	case "":
	case "zstd", "gzip":
		resolved = value
	case "none":
		resolved = ""
	default:
		return nil, fmt.Errorf("invalid compression %q: expected zstd, gzip or none", value)
	}

	if value := query.Get("extension"); value != "" {
		if strings.HasPrefix(value, ".") {
			return nil, fmt.Errorf("invalid extension %q: must not start with a dot", value)
		}
		extension = value
	}

	out := &StoreCompression{
		Type:      resolved,
		Extension: extension,
	}

	if spec := query.Get("compression_config"); spec != "" {
		if resolved != "zstd" {
			return nil, fmt.Errorf("compression_config %q requires zstd compression, store compression is %q", spec, compressionName(resolved))
		}

		zstdOptions, err := parseZstdConfig(spec)
		if err != nil {
			return nil, err
		}
		out.ZstdOptions = zstdOptions
	}

	return out, nil
}

func compressionName(compressionType string) string {
	if compressionType == "" {
		return "none"
	}
	return compressionType
}

// newCommonStore resolves the compression of a store and applies opts.
func newCommonStore(baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*commonStore, error) {
	compression, err := ResolveCompression(baseURL, extension, compressionType, opts...)
	if err != nil {
		return nil, err
	}

	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	return &commonStore{
		compressionType:           compression.Type,
		extension:                 compression.Extension,
		zstdOptions:               compression.ZstdOptions,
		overwrite:                 overwrite,
		uncompressedReadCallback:  conf.uncompressedReadCallback,
		compressedReadCallback:    conf.compressedReadCallback,
		uncompressedWriteCallback: conf.uncompressedWriteCallback,
		compressedWriteCallback:   conf.compressedWriteCallback,
	}, nil
}
