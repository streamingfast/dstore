package storetests

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var openObjectTests = []StoreTestFunc{
	TestOpenObject_ReadSameFileMultipleTimes,
}

func TestOpenObject_ReadSameFileMultipleTimes(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "file", "c1")

	rd, err := store.OpenObject(ctx, "file")
	assert.NoError(t, err)
	assert.Equal(t, "c1", readObjectAndClose(t, rd))

	rd, err = store.OpenObject(ctx, "file")
	assert.NoError(t, err)
	assert.Equal(t, "c1", readObjectAndClose(t, rd))
}
