package storetests

import (
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unlimited is what the listing helpers take to mean "no limit", matching ListFiles.
const unlimited = -1

var listingTests = []StoreTestFunc{
	TestListFolders_Root,
	TestListFolders_Nested,
	TestListFolders_PrefixWithoutTrailingSlash,
	TestListFolders_IgnoresFilesAndNesting,
	TestListFolders_Max,
	TestListFolders_NotFound,
	TestWalkAttributes,
	TestWalkAttributes_MatchesWalk,
	TestWalkAttributes_StopIteration,
	TestWalkAttributes_IgnoreNotFound,
	TestListFoldersFromTo_Slices,
	TestListFoldersFromTo_Bounds,
	TestListFoldersFromTo_UnderPrefix,
	TestListFoldersFromTo_Max,
	TestListFoldersFromTo_BoundsMustMatchPrefix,
}

// The listing helpers must answer the same thing on every store, whether the store implements
// them natively or is served by the generic fallback.

func TestListFolders_Root(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/0000001", "a")
	addFileToStore(t, store, "beta/0000001", "b")
	addFileToStore(t, store, "beta/0000002", "b")

	folders, err := store.ListFolders(ctx, "", unlimited)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alpha/", "beta/"}, folders)
}

func TestListFolders_Nested(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/one/0000001", "a")
	addFileToStore(t, store, "alpha/two/0000001", "a")
	addFileToStore(t, store, "beta/three/0000001", "b")

	folders, err := store.ListFolders(ctx, "alpha/", unlimited)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alpha/one/", "alpha/two/"}, folders)
}

func TestListFolders_PrefixWithoutTrailingSlash(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/one/0000001", "a")

	withSlash, err := store.ListFolders(ctx, "alpha/", unlimited)
	require.NoError(t, err)

	withoutSlash, err := store.ListFolders(ctx, "alpha", unlimited)
	require.NoError(t, err)

	assert.Equal(t, []string{"alpha/one/"}, withSlash)
	assert.Equal(t, withSlash, withoutSlash)
}

// A folder listing stops at one level: it reports neither the objects sitting in the folder
// nor anything nested deeper.
func TestListFolders_IgnoresFilesAndNesting(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/0000001", "a")
	addFileToStore(t, store, "alpha/one/0000001", "a")
	addFileToStore(t, store, "alpha/one/deeper/0000001", "a")

	folders, err := store.ListFolders(ctx, "alpha/", unlimited)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha/one/"}, folders)
}

func TestListFolders_Max(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/0000001", "a")
	addFileToStore(t, store, "beta/0000001", "b")
	addFileToStore(t, store, "gamma/0000001", "c")

	folders, err := store.ListFolders(ctx, "", 2)
	require.NoError(t, err)
	assert.Len(t, folders, 2)

	none, err := store.ListFolders(ctx, "", 0)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestListFolders_NotFound(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	folders, err := store.ListFolders(ctx, "bubblicious/", unlimited)
	require.NoError(t, err)
	assert.Empty(t, folders)
}

func TestWalkAttributes(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	started := time.Now().Add(-time.Minute)

	addFileToStore(t, store, "0000001", "hello")
	addFileToStore(t, store, "0000002", "hello world")

	seen := map[string]dstore.ObjectEntry{}
	err := store.WalkAttributes(ctx, "0000", func(entry dstore.ObjectEntry) error {
		seen[entry.Name] = entry
		return nil
	})
	require.NoError(t, err)

	require.Len(t, seen, 2)
	for name, entry := range seen {
		assert.Greater(t, entry.Size, int64(0), "%s should report a size", name)
		assert.False(t, entry.LastModified.IsZero(), "%s should report a modification time", name)
		assert.WithinDuration(t, time.Now(), entry.LastModified, time.Since(started))
	}
}

// Whatever the store, walking with attributes must see exactly what Walk sees.
func TestWalkAttributes_MatchesWalk(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "0000001", "a")
	addFileToStore(t, store, "0000002", "b")
	addFileToStore(t, store, "nested/0000003", "c")

	var walked []string
	require.NoError(t, store.Walk(ctx, "", func(filename string) error {
		walked = append(walked, filename)
		return nil
	}))

	var withAttributes []string
	require.NoError(t, store.WalkAttributes(ctx, "", func(entry dstore.ObjectEntry) error {
		withAttributes = append(withAttributes, entry.Name)
		return nil
	}))

	assert.ElementsMatch(t, walked, withAttributes)
}

func TestWalkAttributes_StopIteration(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "0000001", "a")
	addFileToStore(t, store, "0000002", "b")
	addFileToStore(t, store, "0000003", "c")

	count := 0
	err := store.WalkAttributes(ctx, "0000", func(entry dstore.ObjectEntry) error {
		count++
		return dstore.StopIteration
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestWalkAttributes_IgnoreNotFound(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	err := store.WalkAttributes(ctx, "bubblicious/0000", func(entry dstore.ObjectEntry) error {
		return nil
	})
	require.NoError(t, err)
}

// A ranged folder listing must tile: splitting the key space and concatenating the slices has
// to give back exactly what the unrestricted listing gives, on every backend.

func TestListFoldersFromTo_Slices(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	for _, name := range []string{"a1", "b2", "c3", "d4"} {
		addFileToStore(t, store, name+"/0000001", name)
	}

	all, err := store.ListFolders(ctx, "", unlimited)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a1/", "b2/", "c3/", "d4/"}, all)

	lower, err := store.ListFoldersFromTo(ctx, "", "", "c", unlimited)
	require.NoError(t, err)
	assert.Equal(t, []string{"a1/", "b2/"}, lower)

	upper, err := store.ListFoldersFromTo(ctx, "", "c", "", unlimited)
	require.NoError(t, err)
	assert.Equal(t, []string{"c3/", "d4/"}, upper)

	assert.ElementsMatch(t, all, append(lower, upper...), "the slices must tile the whole listing")
}

func TestListFoldersFromTo_Bounds(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	for _, name := range []string{"aa", "bb", "cc"} {
		addFileToStore(t, store, name+"/0000001", name)
	}

	// inclusiveFrom includes its own folder, exclusiveTo excludes it.
	folders, err := store.ListFoldersFromTo(ctx, "", "bb", "cc", unlimited)
	require.NoError(t, err)
	assert.Equal(t, []string{"bb/"}, folders)

	empty, err := store.ListFoldersFromTo(ctx, "", "bb", "bb", unlimited)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestListFoldersFromTo_UnderPrefix(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	for _, name := range []string{"one", "two", "three"} {
		addFileToStore(t, store, "alpha/"+name+"/0000001", name)
	}

	folders, err := store.ListFoldersFromTo(ctx, "alpha/", "alpha/three", "alpha/tx", unlimited)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha/three/", "alpha/two/"}, folders)
}

func TestListFoldersFromTo_Max(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	for _, name := range []string{"a1", "b2", "c3"} {
		addFileToStore(t, store, name+"/0000001", name)
	}

	folders, err := store.ListFoldersFromTo(ctx, "", "", "", 2)
	require.NoError(t, err)
	assert.Len(t, folders, 2)
}

// The bounds must belong to the folder being listed, the same contract WalkFromTo enforces.
func TestListFoldersFromTo_BoundsMustMatchPrefix(t *testing.T, factory StoreFactory) {
	store, _, cleanup := factory()
	defer cleanup()

	addFileToStore(t, store, "alpha/one/0000001", "one")

	_, err := store.ListFoldersFromTo(ctx, "alpha/", "beta/x", "", unlimited)
	require.Error(t, err)

	_, err = store.ListFoldersFromTo(ctx, "alpha/", "", "beta/x", unlimited)
	require.Error(t, err)
}
