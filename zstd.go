package dstore

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// zstdEncoderOptions are the options every zstd store encodes with, read once
// from DSTORE_ZSTD_CONFIG. zstdConfigErr is reported by NewStore for zstd stores.
var zstdEncoderOptions, zstdConfigErr = parseZstdConfig(os.Getenv("DSTORE_ZSTD_CONFIG"))

func init() {
	if spec := os.Getenv("DSTORE_ZSTD_CONFIG"); spec != "" && zstdConfigErr == nil {
		zlog.Info("zstd encoder configured", zap.String("config", spec))
	}
}

// parseZstdConfig reads an encoder configuration of the form `<level>` or
// `<level>/<window MiB>`, for example `best`, `better/32` or `best/64`.
//
// The level is one of `fastest`, `default`, `better` or `best`. The window is a
// power of two in MiB; left out, the library default for that level is kept.
// Decoders read the window from the frame header and need no configuration, but
// the zstd command line tool refuses windows above 128 MiB unless told otherwise.
//
// An empty spec keeps the library defaults.
func parseZstdConfig(spec string) ([]zstd.EOption, error) {
	if spec == "" {
		return nil, nil
	}

	levelName, window, hasWindow := strings.Cut(spec, "/")
	ok, level := zstd.EncoderLevelFromString(levelName)
	if !ok {
		return nil, fmt.Errorf("invalid DSTORE_ZSTD_CONFIG %q: unknown level %q, expected fastest, default, better or best", spec, levelName)
	}

	opts := []zstd.EOption{zstd.WithEncoderLevel(level)}
	if hasWindow {
		mib, err := strconv.Atoi(window)
		if err != nil || mib <= 0 {
			return nil, fmt.Errorf("invalid DSTORE_ZSTD_CONFIG %q: window %q must be a positive number of MiB", spec, window)
		}
		opts = append(opts, zstd.WithWindowSize(mib<<20))
	}

	// The encoder validates the window (power of two, at most zstd.MaxWindowSize).
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid DSTORE_ZSTD_CONFIG %q: %w", spec, err)
	}
	enc.Close()

	return opts, nil
}
