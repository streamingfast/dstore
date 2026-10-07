package dstore

import (
	"compress/gzip"
	"fmt"
	"net/url"
	"strconv"
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

	// GzipLevel is the compression level of a gzip store, from compression_config,
	// gzip.DefaultCompression when not set.
	GzipLevel int
}

// ResolveCompression returns the compression and extension of a store created
// at baseURL with the given default extension and compression type.
//
// The default compression type is overridden by the Compression option, which
// is itself overridden by the `compression` query parameter of baseURL (`zstd`,
// `gzip`, or `none` which an empty value also means).
//
// The `extension` query parameter overrides the default extension, without its
// leading dot (`extension=dbin.gz`); an empty value removes it. The extension
// never follows the compression by itself: a store overriding one usually
// overrides both.
//
// The `compression_config` query parameter tunes the encoder of the resolved
// compression. For zstd, it is the level and window as `<level>` or
// `<level>/<window MiB>` (`best`, `better/32`, `best/64`). For gzip, it is an
// integer level from -2 to 9 as defined by compress/gzip (`1` fastest, `9`
// smallest). It is an error on a store without compression, or when the value
// is not valid for the resolved compression.
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
	if query.Has("compression") {
		switch value := query.Get("compression"); value {
		case "zstd", "gzip":
			resolved = value
		case "none", "":
			resolved = ""
		default:
			return nil, fmt.Errorf("invalid compression %q: expected zstd, gzip or none", value)
		}
	}

	if query.Has("extension") {
		value := query.Get("extension")
		if strings.HasPrefix(value, ".") {
			return nil, fmt.Errorf("invalid extension %q: must not start with a dot", value)
		}
		extension = value
	}

	out := &StoreCompression{
		Type:      resolved,
		Extension: extension,
		GzipLevel: gzip.DefaultCompression,
	}

	if spec := query.Get("compression_config"); spec != "" {
		switch resolved {
		case "zstd":
			zstdOptions, err := parseZstdConfig(spec)
			if err != nil {
				return nil, err
			}
			out.ZstdOptions = zstdOptions
		case "gzip":
			level, err := parseGzipConfig(spec)
			if err != nil {
				return nil, err
			}
			out.GzipLevel = level
		default:
			return nil, fmt.Errorf("compression_config %q requires zstd or gzip compression, store compression is %q", spec, compressionName(resolved))
		}
	}

	return out, nil
}

// parseGzipConfig reads a gzip compression level, an integer from
// gzip.HuffmanOnly (-2) to gzip.BestCompression (9).
func parseGzipConfig(spec string) (int, error) {
	level, err := strconv.Atoi(spec)
	if err != nil || level < gzip.HuffmanOnly || level > gzip.BestCompression {
		return 0, fmt.Errorf("invalid compression_config %q for gzip: expected an integer level from %d to %d", spec, gzip.HuffmanOnly, gzip.BestCompression)
	}
	return level, nil
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
		gzipLevel:                 &compression.GzipLevel,
		overwrite:                 overwrite,
		uncompressedReadCallback:  conf.uncompressedReadCallback,
		compressedReadCallback:    conf.compressedReadCallback,
		uncompressedWriteCallback: conf.uncompressedWriteCallback,
		compressedWriteCallback:   conf.compressedWriteCallback,
	}, nil
}
