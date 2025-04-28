package storetests

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var writerTests = []StoreTestFunc{
	TestWriter_Basic,
}

func TestWriter_Basic(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	content := "hello world"
	w, err := store.Writer(ctx, "temp.txt")
	assert.NoError(t, err)

	n, err := w.Write([]byte(content))
	assert.NoError(t, err)
	assert.Equal(t, n, len(content))

	n, err = w.Write([]byte(content))
	assert.NoError(t, err)
	assert.Equal(t, n, len(content))

	assert.NoError(t, w.Close())

	rd, err := store.OpenObject(ctx, "temp.txt")
	assert.NoError(t, err)

	assert.Equal(t, content+content, readObjectAndClose(t, rd))
}
