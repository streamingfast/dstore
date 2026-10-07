package dstore

import (
	"bytes"
	"compress/gzip"
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

func TestResolveCompression(t *testing.T) {
	tests := []struct {
		name            string
		url             string
		extension       string
		compressionType string
		opts            []Option
		wantType        string
		wantExtension   string
		wantZstdOptions bool
		wantErr         string
	}{
		{name: "defaults", url: "gs://b/p", extension: "dbin.zst", compressionType: "zstd", wantType: "zstd", wantExtension: "dbin.zst"},
		{name: "query gzip keeps extension", url: "gs://b/p?compression=gzip", extension: "dbin.zst", compressionType: "zstd", wantType: "gzip", wantExtension: "dbin.zst"},
		{name: "query gzip with extension", url: "gs://b/p?compression=gzip&extension=dbin.gz", extension: "dbin.zst", compressionType: "zstd", wantType: "gzip", wantExtension: "dbin.gz"},
		{name: "query none with extension", url: "gs://b/p?compression=none&extension=dbin", extension: "dbin.zst", compressionType: "zstd", wantType: "", wantExtension: "dbin"},
		{name: "extension alone", url: "gs://b/p?extension=blocks.zst", extension: "dbin.zst", compressionType: "zstd", wantType: "zstd", wantExtension: "blocks.zst"},
		{name: "extension on store without one", url: "gs://b/p?compression=zstd&extension=jsonl.zst", extension: "", compressionType: "", wantType: "zstd", wantExtension: "jsonl.zst"},
		{name: "empty extension removes it", url: "gs://b/p?extension=", extension: "dbin.zst", compressionType: "zstd", wantType: "zstd", wantExtension: ""},
		{name: "empty compression is none", url: "gs://b/p?compression=", extension: "dbin.zst", compressionType: "zstd", wantType: "", wantExtension: "dbin.zst"},
		{name: "empty compression overrides option", url: "gs://b/p?compression=", extension: "dbin.zst", compressionType: "zstd", opts: []Option{Compression("gzip")}, wantType: "", wantExtension: "dbin.zst"},
		{name: "option overrides default", url: "gs://b/p", extension: "dbin.zst", compressionType: "zstd", opts: []Option{Compression("gzip")}, wantType: "gzip", wantExtension: "dbin.zst"},
		{name: "query overrides option", url: "gs://b/p?compression=none", extension: "dbin.zst", compressionType: "zstd", opts: []Option{Compression("gzip")}, wantType: "", wantExtension: "dbin.zst"},
		{name: "config", url: "gs://b/p?compression_config=best/32", extension: "dbin.zst", compressionType: "zstd", wantType: "zstd", wantExtension: "dbin.zst", wantZstdOptions: true},
		{name: "config with query zstd", url: "gs://b/p?compression=zstd&compression_config=better&extension=jsonl.zst", extension: "jsonl.gz", compressionType: "gzip", wantType: "zstd", wantExtension: "jsonl.zst", wantZstdOptions: true},
		{name: "local path", url: "/data/blocks?compression=gzip&extension=dbin.gz", extension: "dbin.zst", compressionType: "zstd", wantType: "gzip", wantExtension: "dbin.gz"},
		{name: "unknown compression", url: "gs://b/p?compression=lz4", extension: "dbin.zst", compressionType: "zstd", wantErr: `invalid compression "lz4"`},
		{name: "extension with leading dot", url: "gs://b/p?extension=.dbin.gz", extension: "dbin.zst", compressionType: "zstd", wantErr: `invalid extension ".dbin.gz"`},
		{name: "config on gzip", url: "gs://b/p?compression_config=best", extension: "jsonl.gz", compressionType: "gzip", wantErr: `requires zstd compression, store compression is "gzip"`},
		{name: "config on none", url: "gs://b/p?compression=none&compression_config=best", extension: "dbin.zst", compressionType: "zstd", wantErr: `requires zstd compression, store compression is "none"`},
		{name: "invalid config", url: "gs://b/p?compression_config=fast", extension: "dbin.zst", compressionType: "zstd", wantErr: `unknown level "fast"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			require.NoError(t, err)

			got, err := ResolveCompression(u, tt.extension, tt.compressionType, tt.opts...)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, got.Type)
			assert.Equal(t, tt.wantExtension, got.Extension)
			assert.Equal(t, tt.wantZstdOptions, got.ZstdOptions != nil)
		})
	}
}

func TestNewStoreRejectsInvalidCompressionQuery(t *testing.T) {
	_, err := NewDBinStore("memory://test?compression_config=fast")
	require.ErrorContains(t, err, `unknown level "fast"`)

	_, err = NewJSONLStore("memory://test?compression_config=best")
	require.ErrorContains(t, err, "requires zstd compression")

	_, err = NewDBinStore("memory://test?compression=lz4")
	require.ErrorContains(t, err, `invalid compression "lz4"`)
}

func TestNewDBinStoreFollowsCompressionAndExtensionQuery(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("dstore compression query "), 1000)

	dir := t.TempDir()
	store, err := NewDBinStore("file://" + dir + "?compression=gzip&extension=dbin.gz")
	require.NoError(t, err)
	require.NoError(t, store.WriteObject(ctx, "0000000100", bytes.NewReader(payload)))

	raw, err := os.ReadFile(filepath.Join(dir, "0000000100.dbin.gz"))
	require.NoError(t, err)
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	decoded, err := io.ReadAll(gz)
	require.NoError(t, err)
	assert.Equal(t, payload, decoded)

	assertReadBack(t, store, "0000000100", payload)

	dir = t.TempDir()
	store, err = NewDBinStore("file://" + dir + "?compression=none&extension=dbin")
	require.NoError(t, err)
	require.NoError(t, store.WriteObject(ctx, "0000000100", bytes.NewReader(payload)))

	raw, err = os.ReadFile(filepath.Join(dir, "0000000100.dbin"))
	require.NoError(t, err)
	assert.Equal(t, payload, raw)

	dir = t.TempDir()
	store, err = NewDBinStore("file://" + dir + "?compression=&extension=")
	require.NoError(t, err)
	require.NoError(t, store.WriteObject(ctx, "0000000100", bytes.NewReader(payload)))

	raw, err = os.ReadFile(filepath.Join(dir, "0000000100"))
	require.NoError(t, err)
	assert.Equal(t, payload, raw)
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

	fileStore, _, err := NewStoreFromFileURL("file://" + dir + "/0000000100.dbin.zst?compression=zstd&compression_config=better/32")
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
		{baseURL: "memory://bucket/path?compression=gzip&extension=dbin.gz&project=p", want: "memory://bucket/path/0000000100.dbin.gz?compression=gzip&extension=dbin.gz&project=p"},
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
