package storetests

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var writeObjectTests = []StoreTestFunc{
	TestWriteObject_Basic,
	TestWriteObject_ConcurrentOverwrite,
	TestWriteObject_ConcurrentNoOverwrite,
}

func TestWriteObject_Basic(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	content := "hello world"
	err := store.WriteObject(ctx, "temp.txt", bytes.NewReader([]byte(content)))
	assert.NoError(t, err)

	rd, err := store.OpenObject(ctx, "temp.txt")
	assert.NoError(t, err)

	assert.Equal(t, content, readObjectAndClose(t, rd))
}

func TestWriteObject_ConcurrentOverwrite(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	if !supportsConcurrentWrites(store) {
		t.Skip("Store does not support concurrent writes, tests is not designed for this case")
		return
	}

	if !store.Overwrite() {
		t.Skip("Store is not set to overwrite files, tests is not designed for this case")
		return
	}

	e1 := make(chan error)
	e2 := make(chan error)

	// Write the same file simultaneously
	go func() {
		err := store.WriteObject(ctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e1 <- err
	}()
	go func() {
		err := store.WriteObject(ctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e2 <- err
	}()
	err1 := <-e1
	require.NoError(t, err1)
	err2 := <-e2
	require.NoError(t, err2)

	// Write the same file afterwards
	err := store.WriteObject(ctx, "samefile", strings.NewReader("pleasewriteme"))
	require.NoError(t, err)

	o, err := store.OpenObject(ctx, "samefile")
	require.NoError(t, err)

	assert.Equal(t, "pleasewriteme", readObjectAndClose(t, o), "overwrite should be true")
}

func TestWriteObject_ConcurrentNoOverwrite(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	if !supportsConcurrentWrites(store) {
		t.Skip("Store does not support concurrent writes, tests is not designed for this case")
		return
	}

	if store.Overwrite() {
		t.Skip("Store is set to overwrite files, tests is not designed for this case")
		return
	}

	e1 := make(chan error)
	e2 := make(chan error)

	// Write the same file simultaneously
	go func() {
		err := store.WriteObject(ctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e1 <- err
	}()
	go func() {
		err := store.WriteObject(ctx, "samefile", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
		e2 <- err
	}()
	err1 := <-e1
	require.NoError(t, err1)
	err2 := <-e2
	require.NoError(t, err2)

	// Write the same file afterwards
	err := store.WriteObject(ctx, "samefile", strings.NewReader("nowriteme"))
	require.NoError(t, err)

	o, err := store.OpenObject(ctx, "samefile")
	require.NoError(t, err)

	assert.Equal(t, "abcdefghijklmnopqrstuvwxyz", readObjectAndClose(t, o), "overwrite should be false")
}
