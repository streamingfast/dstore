package storetests

import (
	"strings"
	"testing"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var openObjectTests = []StoreTestFunc{
	TestOpenObject_ReadSameFileMultipleTimes,
	TestOpenObject_ReadFileOnce,
	TestOpenObject_PartialReadThenClose,
}

func TestOpenObject_ErrNotFound(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	rd, err := store.OpenObject(ctx, "anything_that_does_not_exist")
	assert.Nil(t, rd)
	assert.Equal(t, dstore.ErrNotFound, err)
}

func TestOpenObject_ReadFileOnce(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "file", "c1")

	rd, err := store.OpenObject(ctx, "file")
	assert.NoError(t, err)
	assert.Equal(t, "c1", readObjectAndClose(t, rd))
}

func TestOpenObject_ReadSameFileMultipleTimes(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "file", "c1")

	rd, err := store.OpenObject(ctx, "file")
	assert.NoError(t, err)
	assert.Equal(t, "c1", readObjectAndClose(t, rd))

	rd, err = store.OpenObject(ctx, "file")
	assert.NoError(t, err)
	assert.Equal(t, "c1", readObjectAndClose(t, rd))
}

func TestOpenObject_PartialReadThenClose(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	content := strings.Repeat("stream-close-payload-", 2048)
	addFileToStore(t, store, "partial", content)

	rd, err := store.OpenObject(ctx, "partial")
	require.NoError(t, err)

	buf := make([]byte, 16)
	n, err := rd.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 16, n)
	require.NoError(t, rd.Close())

	rd, err = store.OpenObject(ctx, "partial")
	require.NoError(t, err)
	assert.Equal(t, content, readObjectAndClose(t, rd))
}
