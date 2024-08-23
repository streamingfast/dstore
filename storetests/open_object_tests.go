package storetests

import (
	"testing"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
)

var openObjectTests = []StoreTestFunc{
	TestOpenObject_ReadSameFileMultipleTimes,
}

func TestOpenObject_ErrNotFound(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	rd, err := store.OpenObject(ctx, "anything_that_does_not_exist")
	assert.Nil(t, rd)
	assert.Equal(t, dstore.ErrNotFound, err)
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
