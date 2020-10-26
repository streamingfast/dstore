package dstore

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalStore_SanitizeLocalPath(t *testing.T) {
	store, err := NewLocalStore("./storetests", "go", "", false)
	require.NoError(t, err)

	files, err := store.ListFiles(context.Background(), "", "", math.MaxInt64)
	require.NoError(t, err)
	assert.True(t, len(files) > 0, "Expecting more than one file to be found, got %d", len(files))
}


func TestNewLocalStore_OpenObject_notFound(t *testing.T) {
	store, err := NewLocalStore("/tmp/storage/", "", "", false)
	require.NoError(t, err)

	_, err = store.OpenObject(context.Background(), "foo.txt")
	assert.Equal(t, ErrNotFound, err)
}
