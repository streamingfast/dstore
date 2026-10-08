package dstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseZstdConfig(t *testing.T) {
	level := zstd.WithEncoderLevel
	window := func(mib int) zstd.EOption { return zstd.WithWindowSize(mib << 20) }

	tests := []struct {
		spec    string
		want    []zstd.EOption
		wantErr string
	}{
		{spec: "", want: nil},
		{spec: "fastest", want: []zstd.EOption{level(zstd.SpeedFastest)}},
		{spec: "default", want: []zstd.EOption{level(zstd.SpeedDefault)}},
		{spec: "better", want: []zstd.EOption{level(zstd.SpeedBetterCompression)}},
		{spec: "best", want: []zstd.EOption{level(zstd.SpeedBestCompression)}},
		{spec: "BEST", want: []zstd.EOption{level(zstd.SpeedBestCompression)}},
		{spec: "better/32", want: []zstd.EOption{level(zstd.SpeedBetterCompression), window(32)}},
		{spec: "best/64", want: []zstd.EOption{level(zstd.SpeedBestCompression), window(64)}},
		{spec: "fast", wantErr: `unknown level "fast"`},
		{spec: "best/", wantErr: `window "" must be a positive number of MiB`},
		{spec: "best/0", wantErr: `window "0" must be a positive number of MiB`},
		{spec: "best/64MB", wantErr: `window "64MB" must be a positive number of MiB`},
		{spec: "best/48", wantErr: "window size must be a power of 2"},
		{spec: "best/1024", wantErr: "window size must be at most"},
	}

	sample := make([]byte, 0, 1<<20)
	for i := 0; len(sample) < cap(sample); i++ {
		sample = strconv.AppendInt(sample, int64(i*i%7919), 10)
		sample = append(sample, ' ')
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			conf, err := parseZstdConfig(tt.spec)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, encodeWith(t, tt.want, sample), encodeWith(t, conf.encoderOptions, sample))
		})
	}
}

func encodeWith(t *testing.T, opts []zstd.EOption, in []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil, opts...)
	require.NoError(t, err)
	defer enc.Close()
	return enc.EncodeAll(in, nil)
}

func TestCompressedCopyZstdWithConfig(t *testing.T) {
	conf, err := parseZstdConfig("best/64")
	require.NoError(t, err)

	// Larger than the 8 MiB default window, so the frame advertises the configured one.
	raw := bytes.Repeat([]byte("dstore zstd config "), (16<<20)/19)

	c := commonStore{compressionType: "zstd", zstdConfig: conf}
	compressed := bytes.NewBuffer(nil)
	require.NoError(t, c.compressedCopy(context.Background(), compressed, bytes.NewReader(raw)))

	var header zstd.Header
	require.NoError(t, header.Decode(compressed.Bytes()))
	assert.Equal(t, uint64(64<<20), header.WindowSize)

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	out, err := dec.DecodeAll(compressed.Bytes(), nil)
	require.NoError(t, err)
	assert.Equal(t, raw, out)
}

func TestParseZstdDecoderSettings(t *testing.T) {
	tests := []struct {
		spec        string
		wantEncoder bool
		wantLowmem  *bool
		wantPool    string
		wantErr     string
	}{
		{spec: "best/32,lowmem=false,pool=blocks", wantEncoder: true, wantLowmem: boolPtr(false), wantPool: "blocks"},
		{spec: "better,pool=cache", wantEncoder: true, wantPool: "cache"},
		{spec: "pool=cache,lowmem=true", wantLowmem: boolPtr(true), wantPool: "cache"},
		{spec: "", wantPool: "default"},
		{spec: "better", wantEncoder: true, wantPool: "default"},
		{spec: "lowmem=0", wantLowmem: boolPtr(false), wantPool: "default"},
		{spec: "best/32,pool=none", wantEncoder: true},
		{spec: "pool=default", wantPool: "default"},
		{spec: "pool=my_pool-2", wantPool: "my_pool-2"},
		{spec: "lowmem=nope", wantErr: `lowmem "nope" must be true or false`},
		{spec: "pool=", wantErr: `pool "" must be none or 1 to 64 letters, digits, '-' or '_'`},
		{spec: "pool=a/b", wantErr: `pool "a/b" must be none or 1 to 64 letters`},
		{spec: "pool=" + strings.Repeat("a", 65), wantErr: "must be none or 1 to 64 letters"},
		{spec: "pool=blocks,best", wantErr: `"best" must come first or be written as <key>=<value>`},
		{spec: "best,", wantErr: `"" must come first or be written as <key>=<value>`},
		{spec: "best,window=32", wantErr: `unknown setting "window", expected lowmem or pool`},
		{spec: "fast,pool=blocks", wantErr: `unknown level "fast"`},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			conf, err := parseZstdConfig(tt.spec)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEncoder, conf.encoderOptions != nil)
			assert.Equal(t, tt.wantLowmem, conf.lowmem)
			assert.Equal(t, tt.wantPool, conf.pool)
		})
	}
}

func TestZstdDecoderPoolIsSharedByNameAndLowmem(t *testing.T) {
	poolOf := func(rawURL string) *zstdDecoderPool {
		t.Helper()
		store, err := NewDBinStore(rawURL)
		require.NoError(t, err)
		return store.(*MemoryStore).zstdDecoders
	}

	blocks := poolOf("memory://a?compression_config=best/32,lowmem=false,pool=shared-test")
	require.NotNil(t, blocks)

	assert.Same(t, blocks, poolOf("memory://b?compression_config=lowmem=false,pool=shared-test"), "same name and lowmem share the pool")
	assert.NotSame(t, blocks, poolOf("memory://c?compression_config=pool=shared-test"), "another lowmem gets its own pool")
	assert.NotSame(t, blocks, poolOf("memory://d?compression_config=lowmem=false,pool=other-test"), "another name gets its own pool")
	defaultPool := poolOf("memory://e")
	require.NotNil(t, defaultPool, "a store naming no pool reads with the default one")
	assert.Same(t, defaultPool, poolOf("memory://f?compression_config=best/32"))
	assert.Same(t, defaultPool, poolOf("memory://g?compression_config=pool=default"))
	assert.NotSame(t, defaultPool, poolOf("memory://h?compression_config=lowmem=false"), "another lowmem gets its own default pool")
	assert.Nil(t, poolOf("memory://i?compression_config=pool=none"))
	assert.Nil(t, poolOf("memory://j?compression_config=lowmem=false,pool=none"))
}

func TestZstdDecoderSettingsRoundTrip(t *testing.T) {
	ctx := context.Background()
	// Larger than the 1 MiB window plus its 1 MiB low-memory margin, so the
	// history buffer wraps whatever lowmem is.
	payloads := make([][]byte, 3)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte(fmt.Sprintf("dstore zstd decoder %d ", i)), (4<<20)/22)
	}

	for _, spec := range []string{
		"default/1",
		"default/1,lowmem=false",
		"default/1,pool=none",
		"default/1,lowmem=false,pool=none",
		"default/1,pool=roundtrip-test",
		"default/1,lowmem=false,pool=roundtrip-test",
	} {
		t.Run(spec, func(t *testing.T) {
			store, err := NewDBinStore("memory://test?compression_config=" + spec)
			require.NoError(t, err)

			for i, payload := range payloads {
				require.NoError(t, store.WriteObject(ctx, strconv.Itoa(i), bytes.NewReader(payload)))
			}
			// Each object read twice, so pooled decoders are reused across objects.
			for round := 0; round < 2; round++ {
				for i, payload := range payloads {
					assertReadBack(t, store, strconv.Itoa(i), payload)
				}
			}
		})
	}
}

func TestPooledZstdReaderConcurrentReads(t *testing.T) {
	ctx := context.Background()
	store, err := NewDBinStore("memory://test?compression_config=default/1,lowmem=false,pool=concurrent-test")
	require.NoError(t, err)

	payloads := make([][]byte, 8)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte(fmt.Sprintf("object %d ", i)), (3<<20)/9)
		require.NoError(t, store.WriteObject(ctx, strconv.Itoa(i), bytes.NewReader(payloads[i])))
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 16; i++ {
				n := (worker + i) % len(payloads)
				assertReadBack(t, store, strconv.Itoa(n), payloads[n])
			}
		}()
	}
	wg.Wait()
}

func TestPooledZstdReaderClose(t *testing.T) {
	ctx := context.Background()
	store, err := NewDBinStore("memory://test?compression_config=pool=close-test")
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("close "), 100000)
	require.NoError(t, store.WriteObject(ctx, "0", bytes.NewReader(payload)))

	rc, err := store.OpenObject(ctx, "0")
	require.NoError(t, err)

	buf := make([]byte, 1024)
	_, err = rc.Read(buf)
	require.NoError(t, err)

	require.NoError(t, rc.Close())
	require.NoError(t, rc.Close(), "a second Close is a no-op")

	_, err = rc.Read(buf)
	require.ErrorIs(t, err, errReadOnClosedReader, "a closed reader never reads from a decoder that went back to the pool")

	assertReadBack(t, store, "0", payload)
}

type closeCountingReader struct {
	io.Reader
	closes int
}

func (c *closeCountingReader) Close() error { c.closes++; return nil }

func TestPooledZstdReaderClosesBody(t *testing.T) {
	conf, err := parseZstdConfig("pool=body-test")
	require.NoError(t, err)
	pool := zstdDecoderPoolFor(conf)

	body := &closeCountingReader{Reader: bytes.NewReader(encodeWith(t, nil, []byte("hello")))}
	rc, err := pool.reader(body)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))

	require.NoError(t, rc.Close())
	require.NoError(t, rc.Close())
	assert.Equal(t, 1, body.closes)
}

func boolPtr(b bool) *bool { return &b }

func TestZstdReaderClosesBody(t *testing.T) {
	for _, spec := range []string{"pool=none", "lowmem=false,pool=none", "", "pool=closes-body-test"} {
		t.Run(spec, func(t *testing.T) {
			conf, err := parseZstdConfig(spec)
			require.NoError(t, err)
			c := commonStore{compressionType: "zstd", zstdConfig: conf, zstdDecoders: zstdDecoderPoolFor(conf)}

			body := &closeCountingReader{Reader: bytes.NewReader(encodeWith(t, nil, []byte("hello")))}
			rc, err := c.uncompressedReader(context.Background(), body)
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(got))

			require.NoError(t, rc.Close())
			require.NoError(t, rc.Close())
			assert.Equal(t, 1, body.closes)
		})
	}
}

func TestZstdReaderClosesBodyReadPartially(t *testing.T) {
	payload := bytes.Repeat([]byte("partial "), 1<<20)
	conf, err := parseZstdConfig("pool=none")
	require.NoError(t, err)
	c := commonStore{compressionType: "zstd", zstdConfig: conf}

	body := &closeCountingReader{Reader: bytes.NewReader(encodeWith(t, nil, payload))}
	rc, err := c.uncompressedReader(context.Background(), body)
	require.NoError(t, err)
	_, err = io.ReadFull(rc, make([]byte, 1024))
	require.NoError(t, err)

	require.NoError(t, rc.Close())
	assert.Equal(t, 1, body.closes)
}
