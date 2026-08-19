package dstore

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// ZstdLevel selects a klauspost encoder speed/ratio preset without exposing
// third-party types in the public API.
type ZstdLevel int

const (
	ZstdLevelDefault ZstdLevel = iota // klauspost SpeedDefault
	ZstdLevelBetter                   // SpeedBetterCompression
	ZstdLevelBest                     // SpeedBestCompression
)

func (l ZstdLevel) String() string {
	switch l {
	case ZstdLevelDefault:
		return "default"
	case ZstdLevelBetter:
		return "better"
	case ZstdLevelBest:
		return "best"
	default:
		return fmt.Sprintf("ZstdLevel(%d)", int(l))
	}
}

// ZstdConfig tunes the zstd encoder. WindowSize 0 keeps the library default.
type ZstdConfig struct {
	Level      ZstdLevel
	WindowSize int // bytes; power of two in [1<<10, 128<<20]; 0 = library default
}

const (
	zstdMinWindowSize = 1 << 10   // 1KiB
	zstdMaxWindowSize = 128 << 20 // 128MiB; zstd CLI refuses larger frames by default
)

func (c ZstdConfig) validate() error {
	switch c.Level {
	case ZstdLevelDefault, ZstdLevelBetter, ZstdLevelBest:
	default:
		return fmt.Errorf("invalid zstd level %d", c.Level)
	}
	if c.WindowSize == 0 {
		return nil
	}
	if c.WindowSize < zstdMinWindowSize || c.WindowSize > zstdMaxWindowSize {
		return fmt.Errorf("zstd window size %d is outside [1KiB, 128MiB]; values above 128MiB cannot be decoded by the zstd CLI and most other language bindings without extra flags", c.WindowSize)
	}
	if c.WindowSize&(c.WindowSize-1) != 0 {
		return fmt.Errorf("zstd window size %d is not a power of two", c.WindowSize)
	}
	return nil
}

func (l ZstdLevel) encoderLevel() (zstd.EncoderLevel, error) {
	switch l {
	case ZstdLevelDefault:
		return zstd.SpeedDefault, nil
	case ZstdLevelBetter:
		return zstd.SpeedBetterCompression, nil
	case ZstdLevelBest:
		return zstd.SpeedBestCompression, nil
	default:
		return 0, fmt.Errorf("invalid zstd level %d", l)
	}
}

// zstdPools reuses klauspost encoders/decoders for a single store.
//
// There is no hard cap: a burst of concurrent reads/writes creates one codec
// per in-flight operation. Idle entries are not pinned — sync.Pool drops
// unused items across garbage collections, so a quiet period after a burst
// lets the window-sized buffers be reclaimed.
type zstdPools struct {
	encoders sync.Pool
	decoders sync.Pool
	encOpts  []zstd.EOption
}

func (c *commonStore) getPools() *zstdPools {
	if p := c.pools.Load(); p != nil {
		return p
	}
	p := newZstdPools(c.zstd)
	if !c.pools.CompareAndSwap(nil, p) {
		return c.pools.Load()
	}
	return p
}

func newZstdPools(cfg *ZstdConfig) *zstdPools {
	return &zstdPools{encOpts: zstdEncoderOptions(cfg)}
}

func (p *zstdPools) GetEncoder() (*zstd.Encoder, error) {
	if v := p.encoders.Get(); v != nil {
		return v.(*zstd.Encoder), nil
	}
	// NewWriter only fails when an option is invalid. Concurrency is a
	// constant, and level/window were rejected at store construction, so a
	// failure here would be a klauspost change, not a caller mistake.
	return zstd.NewWriter(nil, p.encOpts...)
}

func (p *zstdPools) PutEncoder(enc *zstd.Encoder) {
	enc.Reset(nil)
	p.encoders.Put(enc)
}

func (p *zstdPools) GetDecoder() (*zstd.Decoder, error) {
	if v := p.decoders.Get(); v != nil {
		return v.(*zstd.Decoder), nil
	}
	// WithDecoderConcurrency(1) is a valid constant; NewReader does not fail
	// on a nil input (Reset is used to attach the next stream).
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
}

func (p *zstdPools) PutDecoder(dec *zstd.Decoder) {
	p.decoders.Put(dec)
}

func zstdEncoderOptions(cfg *ZstdConfig) []zstd.EOption {
	opts := []zstd.EOption{zstd.WithEncoderConcurrency(1)}
	if cfg == nil {
		return opts
	}
	if lvl, err := cfg.Level.encoderLevel(); err == nil {
		opts = append(opts, zstd.WithEncoderLevel(lvl))
	}
	if cfg.WindowSize > 0 {
		opts = append(opts, zstd.WithWindowSize(cfg.WindowSize))
	}
	return opts
}

// zstdReadCloser wraps a pooled decoder. Close must never call Decoder.Close
// on the success path — that is terminal and would poison the pool.
type zstdReadCloser struct {
	dec      *zstd.Decoder
	src      io.ReadCloser
	pools    *zstdPools
	readErr  error
	once     sync.Once
	closeErr error
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	n, err := z.dec.Read(p)
	if err != nil && err != io.EOF {
		z.readErr = err
	}
	return n, err
}

func (z *zstdReadCloser) Close() error {
	z.once.Do(func() {
		// Reset cancels the previous stream before we close the source reader,
		// so a concurrent decoder read cannot race with src.Close.
		resetErr := z.dec.Reset(nil)
		srcErr := z.src.Close()

		if z.readErr != nil || resetErr != nil {
			z.dec.Close()
		} else {
			z.pools.PutDecoder(z.dec)
		}

		if srcErr != nil {
			z.closeErr = srcErr
			return
		}
		z.closeErr = resetErr
	})
	return z.closeErr
}

func applyZstdConfig(code *ZstdConfig, u *url.URL) (*ZstdConfig, error) {
	fromQuery, err := zstdConfigFromQuery(u)
	if err != nil {
		return nil, err
	}
	if code == nil && fromQuery == nil {
		return nil, nil
	}

	var out ZstdConfig
	if code != nil {
		out = *code
	}

	if fromQuery != nil {
		q := u.Query()
		if q.Get("zstd_level") != "" {
			if code != nil {
				zlog.Warn("query parameter 'zstd_level' overrides code-set ZstdCompression.Level",
					zap.Stringer("code_set", code.Level),
					zap.Stringer("query", fromQuery.Level),
				)
			}
			out.Level = fromQuery.Level
		}
		if q.Get("zstd_window") != "" {
			if code != nil {
				zlog.Warn("query parameter 'zstd_window' overrides code-set ZstdCompression.WindowSize",
					zap.Int("code_set", code.WindowSize),
					zap.Int("query", fromQuery.WindowSize),
				)
			}
			out.WindowSize = fromQuery.WindowSize
		}
	}

	if err := out.validate(); err != nil {
		return nil, err
	}
	return &out, nil
}

func zstdConfigFromQuery(u *url.URL) (*ZstdConfig, error) {
	if u == nil {
		return nil, nil
	}

	q := u.Query()
	var out *ZstdConfig
	if v := q.Get("zstd_level"); v != "" {
		level, err := parseZstdLevel(v)
		if err != nil {
			return nil, err
		}
		out = &ZstdConfig{Level: level}
	}
	if v := q.Get("zstd_window"); v != "" {
		window, err := parseZstdWindow(v)
		if err != nil {
			return nil, fmt.Errorf("zstd_window: %w", err)
		}
		if out == nil {
			out = &ZstdConfig{}
		}
		out.WindowSize = window
	}
	return out, nil
}

func parseZstdLevel(s string) (ZstdLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "default":
		return ZstdLevelDefault, nil
	case "better":
		return ZstdLevelBetter, nil
	case "best":
		return ZstdLevelBest, nil
	default:
		return 0, fmt.Errorf("invalid zstd_level %q, want default, better, or best", s)
	}
}

func parseZstdWindow(s string) (int, error) {
	raw := strings.ToLower(strings.TrimSpace(s))
	mult := 1
	switch {
	case strings.HasSuffix(raw, "kib"):
		mult = 1 << 10
		raw = strings.TrimSpace(raw[:len(raw)-3])
	case strings.HasSuffix(raw, "mib"):
		mult = 1 << 20
		raw = strings.TrimSpace(raw[:len(raw)-3])
	case strings.HasSuffix(raw, "gib"):
		mult = 1 << 30
		raw = strings.TrimSpace(raw[:len(raw)-3])
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid zstd window size %q", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid zstd window size %q", s)
	}
	if mult > 1 && n > (int64(^uint(0)>>1))/int64(mult) {
		return 0, fmt.Errorf("zstd window size %q overflows", s)
	}
	return int(n) * mult, nil
}
