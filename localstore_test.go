package dstore

import (
	"context"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalStore_SanitizeLocalPath(t *testing.T) {
	store, err := NewLocalStore(&url.URL{Scheme: "", Path: "./storetests"}, "go", "", false)
	require.NoError(t, err)

	files, err := store.ListFiles(context.Background(), "", math.MaxInt64)
	require.NoError(t, err)
	assert.True(t, len(files) > 0, "Expecting more than one file to be found, got %d", len(files))

}

func TestNewLocalStore_OpenObject_notFound(t *testing.T) {
	store, err := NewLocalStore(&url.URL{Scheme: "", Path: "/tmp/storage/"}, "", "", false)
	require.NoError(t, err)

	_, err = store.OpenObject(context.Background(), "foo.txt")
	assert.Equal(t, ErrNotFound, err)
}

func TestNewLocalStore_SubStoreRelative(t *testing.T) {
	store, err := NewLocalStore(&url.URL{Scheme: "", Path: "./storage/"}, "", "", false)
	require.NoError(t, err)

	sub, err := store.SubStore("sub-folder")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(sub.BaseURL().Path, "sub-folder"))

}
func TestNewLocalStore_CurrentDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "0000000100"), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "0000000200"), []byte("b"), 0644))
	t.Chdir(dir)

	store, err := NewLocalStore(&url.URL{Scheme: "", Path: "."}, "", "", false)
	require.NoError(t, err)

	var seen []string
	require.NoError(t, store.Walk(context.Background(), "", func(filename string) error {
		seen = append(seen, filename)
		return nil
	}))
	assert.Equal(t, []string{"0000000100", "0000000200"}, seen)

	seen = nil
	require.NoError(t, store.WalkFrom(context.Background(), "", "0000000200", func(filename string) error {
		seen = append(seen, filename)
		return nil
	}))
	assert.Equal(t, []string{"0000000200"}, seen)

	reader, err := store.OpenObject(context.Background(), "0000000100")
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	reader.Close()
	assert.Equal(t, "a", string(content))
}

func TestNewLocalStore_SubStore(t *testing.T) {
	store, err := NewLocalStore(&url.URL{Scheme: "", Path: "/tmp/storage/"}, "", "", false)
	require.NoError(t, err)

	sub, err := store.SubStore("sub-folder")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(sub.BaseURL().Path, "sub-folder"))

}
