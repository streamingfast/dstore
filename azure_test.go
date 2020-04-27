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

//accountName := "dfuseandbox"

//container := "demo"

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
	t.Skip() // need azure access to test this, do it on your PC
	os.Setenv("AZURE_STORAGE_KEY", "")

	base, _ := url.Parse("az://dfusesandbox.demo/test")
	s, err := NewAzureStore(base, "", "", false)
	assert.NoError(t, err)

	expectedFiles := []string{}
	files := []struct {
		content string
		name    string
	}{
		{
			content: "block range 0 - 499",
			name:    "blk-0000000000.txt",
		},
		{
			content: "block range 500 - 999",
			name:    "blk-0000000500.txt",
		},
		{
			content: "block range 1000 - 1499",
			name:    "blk-0000001000.txt",
		},
		{
			content: "block range 1500 - 1999",
			name:    "blk-0000001500.txt",
		},
	}

	for _, file := range files {
		fmt.Printf("writting file: %s\n", file.name)
		expectedFiles = append(expectedFiles, file.name)
		err = s.WriteObject(context.Background(), file.name, bytes.NewReader([]byte(file.content)))
		assert.NoError(t, err)
	}

	readFiles, err := s.ListFiles(context.Background(), "", "", 10)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedFiles, readFiles)

	for _, file := range readFiles {
		err = s.DeleteObject(context.Background(), file)
		assert.NoError(t, err)

	}

	fmt.Println("read files: ", readFiles)
}
