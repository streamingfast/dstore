package dstore

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewS3Store(t *testing.T) {
	tests := []struct {
		url                  string
		expectedBucket       string
		expectedPath         string
		expectedStorageClass string
		expectedErr          error
	}{
		{url: "s3://bucket?region=test", expectedBucket: "bucket"},
		{url: "s3://bucket/path1?region=test", expectedBucket: "bucket", expectedPath: "path1"},
		{"s3://bucket/path1/path2?region=test", "bucket", "path1/path2", "", nil},

		{url: "s3://test.com/bucket?region=test", expectedBucket: "bucket"},
		{url: "s3://test.com/bucket/path1/?region=test", expectedBucket: "bucket", expectedPath: "path1"},
		{url: "s3://test.com/bucket/path1/path2?region=test", expectedBucket: "bucket", expectedPath: "path1/path2"},
		{url: "s3://test.com/bucket/path1/path2?region=test&insecure=true", expectedBucket: "bucket", expectedPath: "path1/path2"},
		{url: "s3://test.com/bucket/path1/path2?region=test&insecure=true&storageClass=cold", expectedBucket: "bucket", expectedPath: "path1/path2", expectedStorageClass: "cold"},

		{url: "s3://localhost:9000/store-tests/dstore-s3store-tests-63acbe181e32c21e?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin", expectedBucket: "store-tests", expectedPath: "dstore-s3store-tests-63acbe181e32c21e"},
		{url: "s3://localhost:9000/store-tests?region=none&insecure=true", expectedBucket: "store-tests"},

		{url: "s3://bucket-with.dot/path1?region=test&infer_aws_endpoint=true", expectedBucket: "bucket-with.dot", expectedPath: "path1"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			baseURL, err := url.Parse(test.url)
			require.NoError(t, err)

			store, err := NewS3Store(baseURL, "", "", false)
			if test.expectedErr == nil {
				require.NoError(t, err)

				assert.Equal(t, test.expectedBucket, store.bucket, "bucket not equals")
				assert.Equal(t, test.expectedPath, store.path, "path not equals")
				assert.Equal(t, test.expectedStorageClass, store.storageClass, "storage class not equals")
			} else {
				assert.Equal(t, test.expectedErr, err)
			}

			if err == nil {
				sub, err := store.SubStore("sub-folder")
				require.NoError(t, err)
				require.True(t, strings.HasSuffix(sub.BaseURL().Path, "sub-folder"))
			}
		})
	}
}

func TestParseS3URL(t *testing.T) {
	tests := []struct {
		url                  string
		expectedBucket       string
		expectedPath         string
		expectedStorageClass string
		expectedErr          bool
	}{
		{url: "s3://bucket?region=test", expectedBucket: "bucket"},
		{url: "s3://bucket/path1?region=test", expectedBucket: "bucket", expectedPath: "path1"},
		{url: "s3://bucket/path1/path2?region=test&storageClass=cold", expectedBucket: "bucket", expectedPath: "path1/path2", expectedStorageClass: "cold"},
		{url: "s3://bucket", expectedErr: true}, // no region
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			baseURL, err := url.Parse(test.url)
			require.NoError(t, err)

			configOptions, bucket, path, storageClass, err := ParseS3URL(baseURL)
			if test.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, configOptions)
				assert.Equal(t, test.expectedBucket, bucket)
				assert.Equal(t, test.expectedPath, path)
				assert.Equal(t, test.expectedStorageClass, storageClass)
			}
		})
	}
}
