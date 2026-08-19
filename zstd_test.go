package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/streamingfast/dstore/internal/dummyblock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZstdCompressionOption(t *testing.T) {
	conf := &config{}
	opt := ZstdCompression(ZstdConfig{Level: ZstdLevelBetter, WindowSize: 1 << 20})
	opt.apply(conf)

	require.NotNil(t, conf.zstd)
	assert.Equal(t, ZstdLevelBetter, conf.zstd.Level)
	assert.Equal(t, 1<<20, conf.zstd.WindowSize)
}

func TestZstdConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ZstdConfig
		wantErr string
	}{
		{name: "defaults", cfg: ZstdConfig{}},
		{name: "min window", cfg: ZstdConfig{WindowSize: 1 << 10}},
		{name: "1MiB", cfg: ZstdConfig{WindowSize: 1 << 20}},
		{name: "128MiB cap", cfg: ZstdConfig{WindowSize: 128 << 20}},
		{name: "better level", cfg: ZstdConfig{Level: ZstdLevelBetter}},
		{name: "best level", cfg: ZstdConfig{Level: ZstdLevelBest}},
		{
			name:    "not power of two",
			cfg:     ZstdConfig{WindowSize: 3 << 20},
			wantErr: "power of two",
		},
		{
			name:    "below minimum",
			cfg:     ZstdConfig{WindowSize: 1 << 9},
			wantErr: "1KiB",
		},
		{
			name:    "above 128MiB cap",
			cfg:     ZstdConfig{WindowSize: 256 << 20},
			wantErr: "128MiB",
		},
		{
			name:    "klauspost max is still capped",
			cfg:     ZstdConfig{WindowSize: 1 << 29},
			wantErr: "128MiB",
		},
		{
			name:    "invalid level",
			cfg:     ZstdConfig{Level: ZstdLevel(99)},
			wantErr: "level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStore("memory://zstd-validate", "", "zstd", true, ZstdCompression(tt.cfg))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestZstdRoundTripLevels(t *testing.T) {
	payload := compressiblePayload(256 << 10)

	for _, level := range []ZstdLevel{ZstdLevelDefault, ZstdLevelBetter, ZstdLevelBest} {
		t.Run(level.String(), func(t *testing.T) {
			c := commonStore{
				compressionType: "zstd",
				zstd:            &ZstdConfig{Level: level},
			}
			roundTripZstd(t, &c, payload)
		})
	}
}

func TestZstdRoundTripWindowSizes(t *testing.T) {
	// Larger than klauspost's 128KiB block size so the frame header is written
	// with a window descriptor instead of a single-segment frame.
	payload := compressiblePayload(256 << 10)

	for _, window := range []int{1 << 10, 1 << 16, 1 << 20, 8 << 20, 32 << 20} {
		t.Run(fmt.Sprintf("%d", window), func(t *testing.T) {
			c := commonStore{
				compressionType: "zstd",
				zstd:            &ZstdConfig{WindowSize: window},
			}
			compressed := roundTripZstd(t, &c, payload)
			assert.Equal(t, uint64(window), frameWindowSize(t, compressed))
		})
	}
}

func TestZstdDefaultWindowUnchanged(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	compressed := roundTripZstd(t, &c, compressiblePayload(256<<10))
	assert.Equal(t, uint64(8<<20), frameWindowSize(t, compressed), "nil ZstdConfig must keep the klauspost default 8MiB window")
}

func TestZstdDecoderPoolReuse(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	payload := bytes.Repeat([]byte("pool-reuse-payload-"), 256)

	for i := 0; i < 50; i++ {
		roundTripZstd(t, &c, payload)
	}
}

func TestZstdPartialReadThenClose(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	payload := bytes.Repeat([]byte("partial-read-then-close-"), 128)

	var compressed bytes.Buffer
	require.NoError(t, c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))

	r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
	require.NoError(t, err)

	buf := make([]byte, 16)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 16, n)
	require.NoError(t, r.Close())

	roundTripZstd(t, &c, payload)
}

func TestZstdPartialCloseDoesNotLeakGoroutines(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	payload := compressiblePayload(256 << 10)

	var compressed bytes.Buffer
	require.NoError(t, c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))

	abandon := func() {
		t.Helper()
		r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
		require.NoError(t, err)
		buf := make([]byte, 32)
		n, err := r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 32, n)
		require.NoError(t, r.Close())
	}

	for range 5 {
		abandon()
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	for range 200 {
		abandon()
	}

	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before+5
	}, 2*time.Second, 50*time.Millisecond, "goroutine leak after partial-read Close: before=%d now=%d", before, runtime.NumGoroutine())

	roundTripZstd(t, &c, payload)
}

func TestZstdDoubleClose(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	payload := []byte("double-close")

	var compressed bytes.Buffer
	require.NoError(t, c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))

	r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

func TestZstdErrorDoesNotPoisonPool(t *testing.T) {
	c := commonStore{compressionType: "zstd"}
	payload := bytes.Repeat([]byte("do-not-poison-"), 64)

	var compressed bytes.Buffer
	require.NoError(t, c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))

	corrupt := bytes.Clone(compressed.Bytes())
	require.Greater(t, len(corrupt), 16)
	corrupt[len(corrupt)/2] ^= 0xff

	r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(corrupt)))
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	require.Error(t, err)
	require.NoError(t, r.Close())

	roundTripZstd(t, &c, payload)
}

func TestZstdDefaultReaderReadsLargeWindow(t *testing.T) {
	payload := compressiblePayload(256 << 10)
	window := 32 << 20

	writer := commonStore{
		compressionType: "zstd",
		zstd:            &ZstdConfig{WindowSize: window},
	}
	reader := commonStore{compressionType: "zstd"}

	var compressed bytes.Buffer
	require.NoError(t, writer.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))
	assert.Equal(t, uint64(window), frameWindowSize(t, compressed.Bytes()))

	r, err := reader.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestZstdQueryParams(t *testing.T) {
	store, err := NewStore("memory://zstd-query?zstd_level=better&zstd_window=1MiB", "", "zstd", true)
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("query-param-round-trip-"), 128)
	require.NoError(t, store.WriteObject(context.Background(), "obj", bytes.NewReader(payload)))

	r, err := store.OpenObject(context.Background(), "obj")
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	ms := store.(*MemoryStore)
	require.NotNil(t, ms.zstd)
	assert.Equal(t, ZstdLevelBetter, ms.zstd.Level)
	assert.Equal(t, 1<<20, ms.zstd.WindowSize)
}

func TestZstdQueryParamsOverrideCodeSet(t *testing.T) {
	store, err := NewStore(
		"memory://zstd-override?zstd_level=best&zstd_window=2MiB",
		"",
		"zstd",
		true,
		ZstdCompression(ZstdConfig{Level: ZstdLevelBetter, WindowSize: 1 << 20}),
	)
	require.NoError(t, err)

	ms := store.(*MemoryStore)
	require.NotNil(t, ms.zstd)
	assert.Equal(t, ZstdLevelBest, ms.zstd.Level)
	assert.Equal(t, 2<<20, ms.zstd.WindowSize)
}

func TestZstdQueryParamsInvalid(t *testing.T) {
	_, err := NewStore("memory://zstd-bad-level?zstd_level=ultra", "", "zstd", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zstd_level")

	_, err = NewStore("memory://zstd-bad-window?zstd_window=3MiB", "", "zstd", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "power of two")
}

func TestApplyZstdConfig(t *testing.T) {
	t.Run("nil code and no query", func(t *testing.T) {
		u, err := url.Parse("memory://x")
		require.NoError(t, err)
		cfg, err := applyZstdConfig(nil, u)
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("query window only", func(t *testing.T) {
		u, err := url.Parse("memory://x?zstd_window=1MiB")
		require.NoError(t, err)
		cfg, err := applyZstdConfig(nil, u)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, 1<<20, cfg.WindowSize)
		assert.Equal(t, ZstdLevelDefault, cfg.Level)
	})

	t.Run("query overrides code per field", func(t *testing.T) {
		code := &ZstdConfig{Level: ZstdLevelBetter, WindowSize: 1 << 20}
		u, err := url.Parse("memory://x?zstd_window=2MiB")
		require.NoError(t, err)
		cfg, err := applyZstdConfig(code, u)
		require.NoError(t, err)
		assert.Equal(t, 1<<20, code.WindowSize)
		assert.Equal(t, 2<<20, cfg.WindowSize)
		assert.Equal(t, ZstdLevelBetter, cfg.Level)
		assert.NotSame(t, code, cfg)
	})
}

func TestParseZstdWindow(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: "1024", want: 1024},
		{in: "1KiB", want: 1 << 10},
		{in: "1MiB", want: 1 << 20},
		{in: "32MiB", want: 32 << 20},
		{in: " 2MiB ", want: 2 << 20},
		{in: "1mib", want: 1 << 20},
		{in: "notanumber", wantErr: true},
		{in: "-1MiB", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseZstdWindow(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// BenchmarkZstdDecoderConcurrency isolates WithDecoderConcurrency(1) vs klauspost
// defaults (min(4, GOMAXPROCS) async block pipeline). Sequential is one object
// stream; parallel is many concurrent objects, which is the dstore read shape.
func BenchmarkZstdDecoderConcurrency(b *testing.B) {
	sizes := []int{1 << 20, 8 << 20, 32 << 20}
	modes := []struct {
		name string
		opts []zstd.DOption
	}{
		{name: "conc=1", opts: []zstd.DOption{zstd.WithDecoderConcurrency(1)}},
		{name: "conc=default", opts: nil},
	}

	for _, size := range sizes {
		payload := dummyblock.Proto(size)
		var compressed bytes.Buffer
		enc, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(enc, bytes.NewReader(payload)); err != nil {
			b.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
		src := compressed.Bytes()

		for _, mode := range modes {
			b.Run(fmt.Sprintf("seq/%s/%dMiB", mode.name, size>>20), func(b *testing.B) {
				dec, err := zstd.NewReader(nil, mode.opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer dec.Close()
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := dec.Reset(bytes.NewReader(src)); err != nil {
						b.Fatal(err)
					}
					n, err := io.Copy(io.Discard, dec)
					if err != nil {
						b.Fatal(err)
					}
					if n != int64(len(payload)) {
						b.Fatalf("decoded %d bytes, want %d", n, len(payload))
					}
				}
			})

			b.Run(fmt.Sprintf("par/%s/%dMiB", mode.name, size>>20), func(b *testing.B) {
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					dec, err := zstd.NewReader(nil, mode.opts...)
					if err != nil {
						b.Fatal(err)
					}
					defer dec.Close()
					for pb.Next() {
						if err := dec.Reset(bytes.NewReader(src)); err != nil {
							b.Fatal(err)
						}
						if _, err := io.Copy(io.Discard, dec); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

func BenchmarkZstdRoundTrip(b *testing.B) {
	payload := bytes.Repeat([]byte("benchmark-payload-"), 4096) // 72KiB
	b.Run("pooled", func(b *testing.B) {
		c := commonStore{compressionType: "zstd"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var compressed bytes.Buffer
			if err := c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)); err != nil {
				b.Fatal(err)
			}
			r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				b.Fatal(err)
			}
			if err := r.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unpooled", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var compressed bytes.Buffer
			enc, err := zstd.NewWriter(&compressed)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(enc, bytes.NewReader(payload)); err != nil {
				b.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
			dec, err := zstd.NewReader(bytes.NewReader(compressed.Bytes()))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, dec); err != nil {
				b.Fatal(err)
			}
			dec.Close()
		}
	})
}

func compressiblePayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}

func roundTripZstd(t *testing.T, c *commonStore, payload []byte) []byte {
	t.Helper()

	var compressed bytes.Buffer
	require.NoError(t, c.compressedCopy(context.Background(), &compressed, bytes.NewReader(payload)))

	r, err := c.uncompressedReader(context.Background(), io.NopCloser(bytes.NewReader(compressed.Bytes())))
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	return compressed.Bytes()
}

func frameWindowSize(t *testing.T, compressed []byte) uint64 {
	t.Helper()
	var h zstd.Header
	require.NoError(t, h.Decode(compressed))
	require.False(t, h.SingleSegment, "streamed frames should carry a window descriptor")
	return h.WindowSize
}

func TestZstdLevelString(t *testing.T) {
	assert.Equal(t, "default", ZstdLevelDefault.String())
	assert.Equal(t, "better", ZstdLevelBetter.String())
	assert.Equal(t, "best", ZstdLevelBest.String())
	assert.True(t, strings.Contains(ZstdLevel(42).String(), "42"))
}
