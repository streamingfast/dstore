package dstore

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommonStoreCompressionConfig(t *testing.T) {
	level := func(n int) *int { return &n }

	tests := []struct {
		name            string
		url             string
		compressionType string
		opts            []Option
		wantZstdOptions bool
		wantLowmem      *bool
		wantPool        string
		wantGzipLevel   *int
		wantErr         string
	}{
		{name: "unset on zstd", url: "gs://b/p", compressionType: "zstd"},
		{name: "unset on none", url: "gs://b/p", compressionType: ""},
		{name: "empty on none", url: "gs://b/p?compression_config=", compressionType: ""},
		{name: "zstd level", url: "gs://b/p?compression_config=better", compressionType: "zstd", wantZstdOptions: true},
		{name: "zstd level and window", url: "gs://b/p?compression_config=best/32", compressionType: "zstd", wantZstdOptions: true},
		{name: "zstd level window and decoder settings", url: "gs://b/p?compression_config=best/32,lowmem=false,pool=blocks", compressionType: "zstd", wantZstdOptions: true, wantLowmem: boolPtr(false), wantPool: "blocks"},
		{name: "zstd decoder pool only", url: "gs://b/p?compression_config=pool=cache", compressionType: "zstd", wantPool: "cache"},
		{name: "zstd without pool", url: "gs://b/p?compression_config=better,pool=none", compressionType: "zstd", wantZstdOptions: true, wantPool: "none"},
		{name: "zstd lowmem only", url: "gs://b/p?compression_config=lowmem=false", compressionType: "zstd", wantLowmem: boolPtr(false)},
		{name: "gzip level", url: "gs://b/p?compression_config=8", compressionType: "gzip", wantGzipLevel: level(8)},
		{name: "gzip level huffman only", url: "gs://b/p?compression_config=-2", compressionType: "gzip", wantGzipLevel: level(-2)},
		{name: "option decides the compression", url: "gs://b/p?compression_config=8", compressionType: "zstd", opts: []Option{Compression("gzip")}, wantGzipLevel: level(8)},
		{name: "local path", url: "/data/blocks?compression_config=better", compressionType: "zstd", wantZstdOptions: true},
		{name: "gzip level on zstd", url: "gs://b/p?compression_config=8", compressionType: "zstd", wantErr: `invalid compression_config "8" for zstd: unknown level "8"`},
		{name: "zstd level on gzip", url: "gs://b/p?compression_config=better", compressionType: "gzip", wantErr: `invalid compression_config "better" for gzip: expected an integer level from -2 to 9`},
		{name: "zstd level on option gzip", url: "gs://b/p?compression_config=better", compressionType: "zstd", opts: []Option{Compression("gzip")}, wantErr: `invalid compression_config "better" for gzip`},
		{name: "zstd level on none", url: "gs://b/p?compression_config=better", compressionType: "", wantErr: `compression_config "better" requires a zstd or gzip store, this store has no compression`},
		{name: "gzip level on none", url: "gs://b/p?compression_config=8", compressionType: "", wantErr: "this store has no compression"},
		{name: "garbage on zstd", url: "gs://b/p?compression_config=blah", compressionType: "zstd", wantErr: `invalid compression_config "blah" for zstd`},
		{name: "garbage on gzip", url: "gs://b/p?compression_config=blah", compressionType: "gzip", wantErr: `invalid compression_config "blah" for gzip`},
		{name: "garbage on none", url: "gs://b/p?compression_config=blah", compressionType: "", wantErr: "this store has no compression"},
		{name: "gzip level too high", url: "gs://b/p?compression_config=10", compressionType: "gzip", wantErr: `invalid compression_config "10" for gzip`},
		{name: "zstd unknown decoder setting", url: "gs://b/p?compression_config=best,concurrency=2", compressionType: "zstd", wantErr: `unknown setting "concurrency", expected lowmem or pool`},
		{name: "pool on gzip", url: "gs://b/p?compression_config=8,pool=blocks", compressionType: "gzip", wantErr: "lowmem and pool apply to zstd stores only"},
		{name: "pool on none", url: "gs://b/p?compression_config=pool=blocks", compressionType: "", wantErr: "this store has no compression"},
		{name: "zstd window not a power of two", url: "gs://b/p?compression_config=best/48", compressionType: "zstd", wantErr: `invalid compression_config "best/48" for zstd: window size must be a power of 2`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			require.NoError(t, err)

			common, err := newCommonStore(u, "ext", tt.compressionType, false, tt.opts...)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "ext", common.extension)
			if common.compressionType == "zstd" {
				wantPool := tt.wantPool
				switch wantPool {
				case "":
					wantPool = "default"
				case "none":
					wantPool = ""
				}
				require.NotNil(t, common.zstdConfig)
				assert.Equal(t, tt.wantZstdOptions, common.zstdConfig.encoderOptions != nil)
				assert.Equal(t, tt.wantLowmem, common.zstdConfig.lowmem)
				assert.Equal(t, wantPool, common.zstdConfig.pool)
				assert.Equal(t, wantPool != "", common.zstdDecoders != nil)
			} else {
				assert.False(t, tt.wantZstdOptions)
				assert.Empty(t, tt.wantPool)
			}
			assert.Equal(t, tt.wantGzipLevel, common.gzipLevel)
		})
	}
}

func TestNewStoreRejectsInvalidCompressionConfig(t *testing.T) {
	_, err := NewStore("gs://mybucket/path?compression_config=blah", "dbin.zst", "zstd", false)
	require.ErrorContains(t, err, `invalid compression_config "blah" for zstd`)

	_, err = NewDBinStore("memory://test?compression_config=8")
	require.ErrorContains(t, err, `invalid compression_config "8" for zstd`)

	_, err = NewJSONLStore("memory://test?compression_config=better")
	require.ErrorContains(t, err, `invalid compression_config "better" for gzip`)

	_, err = NewSimpleStore("memory://test?compression_config=better")
	require.ErrorContains(t, err, "this store has no compression")

	_, _, err = NewStoreFromFileURL("memory://test/0000000100.dbin.zst?compression_config=better")
	require.ErrorContains(t, err, "this store has no compression")
}

func TestCompressionConfigQueryIsKeptByDerivedStores(t *testing.T) {
	ctx := context.Background()
	// Larger than the 8 MiB default window, so the frame advertises the configured one.
	payload := bytes.Repeat([]byte("dstore zstd config "), (16<<20)/19)

	dir := t.TempDir()
	store, err := NewDBinStore("file://" + dir + "?compression_config=better/32")
	require.NoError(t, err)

	clone, err := store.(Clonable).Clone(ctx)
	require.NoError(t, err)

	sub, err := store.SubStore("sub")
	require.NoError(t, err)

	fileStore, _, err := NewStoreFromFileURL("file://"+dir+"/0000000100.dbin.zst?compression_config=better/32", Compression("zstd"))
	require.NoError(t, err)

	for name, s := range map[string]Store{"store": store, "clone": clone, "sub": sub, "file url": fileStore} {
		t.Run(name, func(t *testing.T) {
			filename := "0000000100"
			path := filepath.Join(dir, "0000000100.dbin.zst")
			if name == "sub" {
				path = filepath.Join(dir, "sub", "0000000100.dbin.zst")
			}
			if name == "file url" {
				filename = "0000000100.dbin.zst"
			}

			s.SetOverwrite(true)
			require.NoError(t, s.WriteObject(ctx, filename, bytes.NewReader(payload)))

			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			var header zstd.Header
			require.NoError(t, header.Decode(raw))
			assert.Equal(t, uint64(32<<20), header.WindowSize)
		})
	}
}

func TestCompressionConfigGzipLevel(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("dstore gzip level 0123456789 "), 20000)

	sizes := map[string]int{}
	for _, level := range []string{"0", "1", "9"} {
		dir := t.TempDir()
		store, err := NewJSONLStore("file://" + dir + "?compression_config=" + level)
		require.NoError(t, err)
		require.NoError(t, store.WriteObject(ctx, "0000000100", bytes.NewReader(payload)))

		raw, err := os.ReadFile(filepath.Join(dir, "0000000100.jsonl.gz"))
		require.NoError(t, err)
		sizes[level] = len(raw)

		assertReadBack(t, store, "0000000100", payload)
	}

	assert.Greater(t, sizes["0"], len(payload), "level 0 stores without compressing")
	assert.Less(t, sizes["9"], sizes["0"])
}

func assertReadBack(t *testing.T, store Store, name string, want []byte) {
	t.Helper()
	rc, err := store.OpenObject(context.Background(), name)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestObjectURLKeepsQueryAfterPath(t *testing.T) {
	tests := []struct {
		baseURL string
		want    string
	}{
		{baseURL: "memory://bucket/path", want: "memory://bucket/path/0000000100.dbin.zst"},
		{baseURL: "memory://bucket/path?compression_config=best/32", want: "memory://bucket/path/0000000100.dbin.zst?compression_config=best/32"},
		{baseURL: "memory://bucket/path?compression_config=better&project=p", want: "memory://bucket/path/0000000100.dbin.zst?compression_config=better&project=p"},
		{baseURL: "memory://bucket?compression_config=best", want: "memory://bucket/0000000100.dbin.zst?compression_config=best"},
	}

	for _, tt := range tests {
		t.Run(tt.baseURL, func(t *testing.T) {
			store, err := NewDBinStore(tt.baseURL)
			require.NoError(t, err)
			assert.Equal(t, tt.want, store.ObjectURL("0000000100"))
		})
	}

	dir := t.TempDir()
	store, err := NewDBinStore("file://" + dir + "?compression_config=best")
	require.NoError(t, err)
	assert.Equal(t, "file://"+dir+"/0000000100.dbin.zst?compression_config=best", store.ObjectURL("0000000100"))

	sub, err := store.SubStore("sub")
	require.NoError(t, err)
	assert.Equal(t, "file://"+dir+"/sub/0000000100.dbin.zst?compression_config=best", sub.ObjectURL("0000000100"))
}
