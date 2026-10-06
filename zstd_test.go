package dstore

import (
	"bytes"
	"context"
	"strconv"
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
			opts, err := parseZstdConfig(tt.spec)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, encodeWith(t, tt.want, sample), encodeWith(t, opts, sample))
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
	opts, err := parseZstdConfig("best/64")
	require.NoError(t, err)

	// Larger than the 8 MiB default window, so the frame advertises the configured one.
	raw := bytes.Repeat([]byte("dstore zstd config "), (16<<20)/19)

	c := commonStore{compressionType: "zstd", zstdOptions: opts}
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
