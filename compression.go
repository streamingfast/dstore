package dstore

import (
	"compress/gzip"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// newCommonStore builds the common part of a store from its constructor
// arguments and opts.
//
// The compression type is the Compression option when given, compressionType
// otherwise. The `compression_config` query parameter of baseURL tunes it: for
// zstd, the encoder level and window as `<level>` or `<level>/<window MiB>`
// (`best`, `better/32`, `best/64`), optionally followed by the decoder settings
// `lowmem=<bool>` and `pool=<name>` (`best/32,lowmem=false,pool=blocks`, see
// parseZstdConfig); for gzip, an integer level from -2 to 9 as defined by
// compress/gzip (`1` fastest, `9` smallest). It is an error on a store without
// compression, or when the value is not valid for its compression.
func newCommonStore(baseURL *url.URL, extension, compressionType string, overwrite bool, opts ...Option) (*commonStore, error) {
	conf := config{}
	for _, opt := range opts {
		opt.apply(&conf)
	}

	if conf.compression != "" {
		compressionType = conf.compression
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

	spec := baseURL.Query().Get("compression_config")
	if spec == "" {
		return common, nil
	}

	switch compressionType {
	case "zstd":
		conf, err := parseZstdConfig(spec)
		if err != nil {
			return nil, err
		}
		common.zstdConfig = conf
		common.zstdDecoders = zstdDecoderPoolFor(conf)
	case "gzip":
		if strings.ContainsAny(spec, ",=") {
			return nil, fmt.Errorf("invalid compression_config %q for gzip: lowmem and pool apply to zstd stores only, expected an integer level from %d to %d", spec, gzip.HuffmanOnly, gzip.BestCompression)
		}
		level, err := parseGzipConfig(spec)
		if err != nil {
			return nil, err
		}
		common.gzipLevel = &level
	default:
		return nil, fmt.Errorf("compression_config %q requires a zstd or gzip store, this store has no compression", spec)
	}

	return common, nil
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
