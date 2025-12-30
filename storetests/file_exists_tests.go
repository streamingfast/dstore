package storetests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fileExistsTests = []StoreTestFunc{
	TestFileExists,
}

func TestFileExists(t *testing.T, factory StoreFactory) {
	testCases := []struct {
		name          string
		searchFor     string
		shouldBeFound bool
		expectedErr   error
		whenFiles     []testFile
	}{
		{
			name:          "found",
			searchFor:     "1",
			shouldBeFound: true, expectedErr: nil,
			whenFiles: []testFile{{"1", "c1"}},
		},
		{
			name:          "found sub paths",
			searchFor:     "0/1",
			shouldBeFound: true, expectedErr: nil,
			whenFiles: []testFile{{"0/1", "c1"}},
		},
		{
			name:          "not found",
			searchFor:     "2",
			shouldBeFound: false, expectedErr: nil,
			whenFiles: []testFile{{"1", "c1"}},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			store, _, cleanup := factory()
			defer cleanup()

			for _, file := range test.whenFiles {
				addFileToStore(t, store, file.id, file.content)
			}

			exists, err := store.FileExists(context.Background(), test.searchFor)
			if test.expectedErr != nil {
				require.Equal(t, test.expectedErr, err)
			} else {
				assert.Equal(t, test.shouldBeFound, exists)
			}
		})
	}
}
