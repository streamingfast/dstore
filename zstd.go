package dstore

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdConfig is the zstd part of a store's compression_config.
type zstdConfig struct {
	// encoderOptions configure the files the store writes, nil for the library
	// defaults.
	encoderOptions []zstd.EOption

	// lowmem is the decoder's low-memory mode, nil to keep the library default
	// (on).
	lowmem *bool

	// pool names the decoder pool the store reads with, empty for none.
	pool string
}

var zstdPoolNameRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// parseZstdConfig reads a zstd compression_config: an optional encoder setting
// followed by comma separated decoder settings,
// `[<level>[/<window MiB>]][,lowmem=<bool>][,pool=<name>]`, for example `best`,
// `better/32`, `best/32,lowmem=false,pool=blocks` or `pool=cache`.
//
// The encoder setting only changes the files the store writes. The level is one
// of `fastest`, `default`, `better` or `best`. The window is a power of two in
// MiB; left out, the library default for that level is kept. Decoders read the
// window from the frame header and need no configuration, but the zstd command
// line tool refuses windows above 128 MiB unless told otherwise.
//
// `lowmem=false` gives each decoder a history buffer of twice the window instead
// of the window plus 1 MiB. Objects larger than the window then decode with far
// less memory copying, at the cost of twice the window in memory per decoder.
//
// `pool=<name>` reads with decoders kept in a process-wide pool of that name and
// reused across objects and stores. Pooled decoders decode on the calling
// goroutine (concurrency 1).
//
// An empty spec keeps the library defaults.
func parseZstdConfig(spec string) (*zstdConfig, error) {
	conf := &zstdConfig{}
	if spec == "" {
		return conf, nil
	}

	for i, setting := range strings.Split(spec, ",") {
		key, value, isKeyValue := strings.Cut(setting, "=")
		if !isKeyValue {
			if i != 0 {
				return nil, fmt.Errorf("invalid compression_config %q for zstd: %q must come first or be written as <key>=<value>", spec, setting)
			}
			opts, err := parseZstdEncoderConfig(spec, setting)
			if err != nil {
				return nil, err
			}
			conf.encoderOptions = opts
			continue
		}

		switch key {
		case "lowmem":
			lowmem, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("invalid compression_config %q for zstd: lowmem %q must be true or false", spec, value)
			}
			conf.lowmem = &lowmem
		case "pool":
			if !zstdPoolNameRegexp.MatchString(value) {
				return nil, fmt.Errorf("invalid compression_config %q for zstd: pool %q must be 1 to 64 letters, digits, '-' or '_'", spec, value)
			}
			conf.pool = value
		default:
			return nil, fmt.Errorf("invalid compression_config %q for zstd: unknown setting %q, expected lowmem or pool", spec, key)
		}
	}

	return conf, nil
}

func parseZstdEncoderConfig(spec, setting string) ([]zstd.EOption, error) {
	levelName, window, hasWindow := strings.Cut(setting, "/")
	ok, level := zstd.EncoderLevelFromString(levelName)
	if !ok {
		return nil, fmt.Errorf("invalid compression_config %q for zstd: unknown level %q, expected <level> or <level>/<window MiB> with level fastest, default, better or best", spec, levelName)
	}

	opts := []zstd.EOption{zstd.WithEncoderLevel(level)}
	if hasWindow {
		mib, err := strconv.Atoi(window)
		if err != nil || mib <= 0 {
			return nil, fmt.Errorf("invalid compression_config %q for zstd: window %q must be a positive number of MiB", spec, window)
		}
		opts = append(opts, zstd.WithWindowSize(mib<<20))
	}

	// The encoder validates the window (power of two, at most zstd.MaxWindowSize).
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid compression_config %q for zstd: %w", spec, err)
	}
	enc.Close()

	return opts, nil
}

// decoderOptions are the options of the decoders of a store that does not pool
// them.
func (c *zstdConfig) decoderOptions() []zstd.DOption {
	if c.lowmem == nil {
		return nil
	}
	return []zstd.DOption{zstd.WithDecoderLowmem(*c.lowmem)}
}

// zstdDecoderPools holds the named decoder pools of the process, keyed by name
// and low-memory mode so a store never gets a decoder built with other options.
var zstdDecoderPools sync.Map // zstdPoolKey -> *zstdDecoderPool

type zstdPoolKey struct {
	name   string
	lowmem bool
}

// zstdDecoderPool keeps idle decoders for reuse. It is a sync.Pool, so idle
// decoders are dropped by garbage collection when they stop being used. That is
// only safe because pooled decoders run with concurrency 1: such a decoder owns
// no goroutine and needs no Close to be released.
type zstdDecoderPool struct {
	pool sync.Pool
}

func zstdDecoderPoolFor(conf *zstdConfig) *zstdDecoderPool {
	if conf.pool == "" {
		return nil
	}

	key := zstdPoolKey{name: conf.pool, lowmem: true}
	if conf.lowmem != nil {
		key.lowmem = *conf.lowmem
	}
	if p, ok := zstdDecoderPools.Load(key); ok {
		return p.(*zstdDecoderPool)
	}

	p := &zstdDecoderPool{}
	p.pool.New = func() any {
		// Options are constants and valid, so construction cannot fail.
		dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(key.lowmem))
		return dec
	}
	actual, _ := zstdDecoderPools.LoadOrStore(key, p)
	return actual.(*zstdDecoderPool)
}

// reader returns a reader decompressing body with a pooled decoder. Closing it
// closes body and gives the decoder back to the pool.
func (p *zstdDecoderPool) reader(body io.ReadCloser) (io.ReadCloser, error) {
	dec := p.pool.Get().(*zstd.Decoder)
	if err := dec.Reset(body); err != nil {
		dec.Close()
		return nil, fmt.Errorf("unable to reset pooled zstd decoder: %w", err)
	}
	return &pooledZstdReadCloser{dec: dec, body: body, pool: p}, nil
}

func (p *zstdDecoderPool) put(dec *zstd.Decoder) {
	// Reset drops the decoder's reference to the body, which must not be kept
	// alive by the pool.
	if err := dec.Reset(nil); err != nil {
		dec.Close()
		return
	}
	p.pool.Put(dec)
}

var errReadOnClosedReader = errors.New("read on a closed zstd reader")

// pooledZstdReadCloser hands its decoder back to the pool on Close. Once
// closed, it never touches the decoder again: another object may already be
// decoding with it.
type pooledZstdReadCloser struct {
	mu   sync.Mutex
	dec  *zstd.Decoder
	body io.ReadCloser
	pool *zstdDecoderPool
	once sync.Once
}

func (r *pooledZstdReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dec == nil {
		return 0, errReadOnClosedReader
	}
	return r.dec.Read(p)
}

// Close closes the body first, which unblocks a Read waiting on it in another
// goroutine, then waits for that Read to return before releasing the decoder.
func (r *pooledZstdReadCloser) Close() (err error) {
	r.once.Do(func() {
		err = r.body.Close()

		r.mu.Lock()
		dec := r.dec
		r.dec = nil
		r.mu.Unlock()

		r.pool.put(dec)
	})
	return err
}
