package dstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGSStore_calculateStartOffset(t *testing.T) {

	tests := []struct {
		name          string
		baseURLPath   string // always starts with '/'
		prefix        string // no / prefix
		startingPoint string // must start with prefix

		expectedOffset string
		expectedPrefix string
		expectedError  string
	}{
		{
			name:          "mostly empty",
			baseURLPath:   "/",
			prefix:        "",
			startingPoint: "",

			expectedOffset: "",
			expectedPrefix: "",
		},

		{
			name:          "starting point equals prefix",
			baseURLPath:   "/",
			prefix:        "data",
			startingPoint: "data",

			expectedOffset: "data",
			expectedPrefix: "data",
		},

		{
			name:          "starting point equals prefix with baseurlpath",
			baseURLPath:   "/base",
			prefix:        "data",
			startingPoint: "data",

			expectedOffset: "base/data",
			expectedPrefix: "base/data",
		},
		{
			name:           "starting point below prefix",
			baseURLPath:    "/",
			prefix:         "data",
			startingPoint:  "data/subdir/file.txt",
			expectedOffset: "data/subdir/file.txt",
			expectedPrefix: "data",
		},
		{
			name:           "prefix with trailing slash",
			baseURLPath:    "/",
			prefix:         "data/",
			startingPoint:  "data/file.txt",
			expectedOffset: "data/file.txt",
			expectedPrefix: "data/",
		},
		{
			name:          "starting point not matches prefix",
			baseURLPath:   "/",
			prefix:        "data/",
			startingPoint: "datax/file.txt",
			expectedError: "starting point \"datax/file.txt\" must start with prefix \"data/\"",
		},

		{
			name:           "starting point does not end with slash no baseurl",
			baseURLPath:    "/",
			prefix:         "errors.",
			startingPoint:  "errors.12345",
			expectedOffset: "errors.12345",
			expectedPrefix: "errors.",
		},
		{
			name:          "starting point does not end with slash",
			baseURLPath:   "/my/data",
			prefix:        "errors.",
			startingPoint: "errors.12345",

			expectedOffset: "my/data/errors.12345",
			expectedPrefix: "my/data/errors.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := getGSWalkQuery(tt.prefix, tt.startingPoint, "", tt.baseURLPath)

			if tt.expectedError != "" {
				if err == nil {
					t.Errorf("Expected error %q, but got nil", tt.expectedError)
					return
				}
				if err.Error() != tt.expectedError {
					t.Errorf("Expected error %q, got %q", tt.expectedError, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if query.Prefix != tt.expectedPrefix {
				t.Errorf("Expected prefix %q, got %q", tt.expectedPrefix, query.Prefix)
			}

			if query.StartOffset != tt.expectedOffset {
				t.Errorf("Expected offset %q, got %q", tt.expectedOffset, query.StartOffset)
			}
		})
	}
}

func TestGSStore_calculateEndOffset(t *testing.T) {
	tests := []struct {
		name              string
		prefix            string
		exclusiveEndPoint string
		baseURLPath       string
		expectedEndOffset string
	}{
		{
			name:              "no end point",
			prefix:            "chain/",
			baseURLPath:       "/root",
			expectedEndOffset: "",
		},
		{
			name:              "end point below the prefix",
			prefix:            "chain/",
			exclusiveEndPoint: "chain/0000000100",
			baseURLPath:       "/root",
			expectedEndOffset: "root/chain/0000000100",
		},
		{
			name:              "end point keeps its trailing slash, so the key without it stays in range",
			prefix:            "chain/",
			exclusiveEndPoint: "chain/sub/",
			baseURLPath:       "/root",
			expectedEndOffset: "root/chain/sub/",
		},
		{
			name:              "end point on a store without a path",
			prefix:            "",
			exclusiveEndPoint: "sub/",
			baseURLPath:       "/",
			expectedEndOffset: "sub/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := getGSWalkQuery(tt.prefix, "", tt.exclusiveEndPoint, tt.baseURLPath)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedEndOffset, query.EndOffset)
		})
	}
}
