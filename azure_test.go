package dstore

import (
	"bytes"
	"context"
	"fmt"
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

func TestAzureSToreWriteObject(t *testing.T) {
	t.Skip("needs azure access to test this")
	os.Setenv("AZURE_STORAGE_KEY", "")

	base, _ := url.Parse("az://dfusesandbox.demo/test")
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
