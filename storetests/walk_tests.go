package storetests

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var walkTests = []StoreTestFunc{
	TestListFiles,

	TestWalk_IgnoreNotFound,
	TestWalk_FilePrefix,
	TestWalk_PathPrefix,
	TestWalkFrom,
	TestWalkFrom_WithPrefix,
	TestWalkFrom_SingleLetterStartingPoint,
	TestWalkFrom_StartingPointHasWrongPrefix,
}

func TestWalk_IgnoreNotFound(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	err := store.Walk(ctx, "bubblicious/0000", func(f string) error { return nil })
	require.NoError(t, err)
}

func TestWalk_FilePrefix(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	expected := []string{"00000001", "00000002", "00000003"}
	for _, f := range expected {
		addFileToStore(t, store, f, f)
	}

	var seen []string
	err := store.Walk(ctx, "0000", func(f string) error {
		seen = append(seen, f)
		exists, err := store.FileExists(ctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, expected, seen)
}

func TestWalkFrom(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	written := []string{"00000001", "00000002", "00000003", "00000004"}
	for _, f := range written {
		addFileToStore(t, store, f, f)
	}
	expected := []string{"00000002", "00000003", "00000004"}

	var seen []string
	err := store.WalkFrom(ctx, "", "00000002", func(f string) error {
		seen = append(seen, f)
		exists, err := store.FileExists(ctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, expected, seen)
}

func TestWalkFrom_StartingPointHasWrongPrefix(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	err := store.WalkFrom(ctx, "0000", "0001/0002", func(f string) error {
		return nil
	})

	require.EqualError(t, err, `starting point "0001/0002" must start with prefix "0000"`)
}

func TestWalkFrom_WithPrefix(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	expected := []string{"0000/0001", "0000/0002", "0000/0003", "0001/0003"}
	for _, f := range expected {
		addFileToStore(t, store, f, f)
	}

	var seen []string
	err := store.WalkFrom(ctx, "0000", "0000/0002", func(f string) error {
		seen = append(seen, f)
		exists, err := store.FileExists(ctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, expected[1:3], seen)
}

func TestWalkFrom_SingleLetterStartingPoint(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	expected := []string{"a", "b", "c", "d"}
	for _, f := range expected {
		addFileToStore(t, store, f, f)
	}

	var seen []string
	err := store.WalkFrom(ctx, "", "b", func(f string) error {
		seen = append(seen, f)
		exists, err := store.FileExists(ctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, expected[1:], seen)
}

func TestWalk_PathPrefix(t *testing.T, factory StoreFactory) {
	store, cleanup := factory()
	defer cleanup()

	expected := []string{"0000/0001", "0000/0002", "0000/0003"}
	for _, f := range expected {
		addFileToStore(t, store, f, f)
	}

	var seen []string
	err := store.Walk(ctx, "0000", func(f string) error {
		seen = append(seen, f)
		exists, err := store.FileExists(ctx, f)
		assert.NoError(t, err)
		assert.True(t, exists)
		return nil
	})

	require.NoError(t, err)
	assert.EqualValues(t, expected, seen)
}

func TestListFiles(t *testing.T, factory StoreFactory) {
	testCases := []struct {
		name           string
		withQuery      listFilesQuery
		whenFiles      []testFile
		expectingNames []string
		expectedErr    error
	}{
		{
			name:           "empty",
			withQuery:      listFilesQuery{prefix: "", max: math.MaxInt64},
			whenFiles:      []testFile{},
			expectingNames: nil, expectedErr: nil,
		},
		{
			name:           "multiple",
			withQuery:      listFilesQuery{prefix: "", max: math.MaxInt64},
			whenFiles:      []testFile{{"1", "c1"}, {"2", "c2"}, {"3", "c3"}},
			expectingNames: []string{"1", "2", "3"}, expectedErr: nil,
		},

		{
			name:           "multiple with sub paths",
			withQuery:      listFilesQuery{prefix: "", max: math.MaxInt64},
			whenFiles:      []testFile{{"a/1", "c1"}, {"b/2", "c2"}, {"b/3", "c3"}},
			expectingNames: []string{"a/1", "b/2", "b/3"}, expectedErr: nil,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			store, cleanup := factory()
			defer cleanup()

			for _, file := range test.whenFiles {
				addFileToStore(t, store, file.id, file.content)
			}

			filenames, err := store.ListFiles(context.Background(), test.withQuery.prefix, test.withQuery.max)
			if test.expectedErr != nil {
				require.Equal(t, test.expectedErr, err)
			} else {
				assert.Equal(t, test.expectingNames, filenames)
			}
		})
	}
}

type listFilesQuery struct {
	prefix       string
	ignoreSuffix string
	max          int
}
