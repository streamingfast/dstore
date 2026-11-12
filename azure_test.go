package dstore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_decodeAzureScheme(t *testing.T) {
	tests := []struct {
		name                string
		schema              string
		expectAccountName   string
		expectContainerName string
		expectError         bool
	}{
		{
			name:                "Sunny Path",
			schema:              "azblob://accountname.container/path",
			expectAccountName:   "accountname",
			expectContainerName: "container",
			expectError:         false,
		},
		{
			name:        "missing container",
			schema:      "azblob://accountname./path",
			expectError: true,
		},
		{
			name:        "only account name",
			schema:      "azblob://accountname/path",
			expectError: true,
		},
		{
			name:        "missing account name",
			schema:      "azblob://.container/path",
			expectError: true,
		},
		{
			name:        "only account name",
			schema:      "azblob://container/path",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := url.Parse(test.schema)
			if err != nil {
				panic("invalid test")
			}

			a, c, err := decodeAzureScheme(dsn)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectAccountName, a)
				assert.Equal(t, test.expectContainerName, c)
			}

		})
	}

}

func TestAzureStoreWriteObject(t *testing.T) {
	if os.Getenv("AZURE_STORAGE_KEY") == "" {
		t.Skip("AZURE_STORAGE_KEY not set")
	}

	base, _ := url.Parse("az://streamingfasttest01.myblobs/test")
	s, err := NewAzureStore(base, "", "", false)
	require.NoError(t, err)

	expectedFiles := []string{}
	files := map[string]string{
		"blk-0000000000.txt": "block range 0 - 499",
		"blk-0000000500.txt": "block range 500 - 999",
		"blk-0000001000.txt": "block range 1000 - 1499",
		"blk-0000001500.txt": "block range 1500 - 1999",
	}

	for fileName, content := range files {
		fmt.Printf("writting file: %s with content: %s\n", fileName, content)
		expectedFiles = append(expectedFiles, fileName)
		err = s.WriteObject(context.Background(), fileName, bytes.NewReader([]byte(content)))
		require.NoError(t, err)
	}

	readFiles, err := s.ListFiles(context.Background(), "", 10)
	require.NoError(t, err)
	assert.ElementsMatch(t, expectedFiles, readFiles)

	for _, fileName := range readFiles {
		f, err := s.OpenObject(context.Background(), fileName)
		require.NoError(t, err)
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(f)
		require.NoError(t, err)
		content := buf.String()
		fmt.Printf("reading file: %s with content: %s\n", fileName, content)
		assert.Equal(t, files[fileName], content)
	}

	for _, fileName := range readFiles {
		fmt.Printf("deleting file: %s\n", fileName)
		err = s.DeleteObject(context.Background(), fileName)
		assert.NoError(t, err)
	}

	readFiles, err = s.ListFiles(context.Background(), "", 10)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{}, readFiles)
}

func TestAzureStoreWalkFromSendsStartFrom(t *testing.T) {
	var startFroms []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startFroms = append(startFroms, r.URL.Query().Get("startFrom"))

		// "base/00000001" is below the starting point, as an endpoint ignoring startFrom
		// would return it
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="cont"><Blobs>
<Blob><Name>base/00000001.dbin</Name><Properties/></Blob>
<Blob><Name>base/00000002.dbin</Name><Properties/></Blob>
<Blob><Name>base/00000003.dbin</Name><Properties/></Blob>
</Blobs><NextMarker/></EnumerationResults>`)
	}))
	defer server.Close()

	t.Setenv("AZURE_STORAGE_ENDPOINT", server.URL)
	t.Setenv("AZURE_STORAGE_KEY", "a2V5")

	base, err := url.Parse("az://account.cont/base")
	require.NoError(t, err)
	s, err := NewAzureStore(base, "dbin", "", false)
	require.NoError(t, err)

	var seen []string
	err = s.WalkFrom(context.Background(), "", "00000002", func(filename string) error {
		seen = append(seen, filename)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"00000002", "00000003"}, seen)

	seen = nil
	err = s.WalkFromTo(context.Background(), "", "00000002", "00000003", func(filename string) error {
		seen = append(seen, filename)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"00000002"}, seen)

	require.NoError(t, s.Walk(context.Background(), "", func(string) error { return nil }))

	assert.Equal(t, []string{"base/00000002", "base/00000002", ""}, startFroms)
}
