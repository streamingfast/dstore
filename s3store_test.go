package dstore

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//// use with your own s3 testing endpoint
//func TestWalkFrom(t *testing.T) {
//	// requires a helloworld.html file in the bucket
//	endpoint := "s3://your_test_bucket/?region=ap-northeast-1"
//
//	baseURL, err := url.Parse(endpoint)
//	require.NoError(t, err)
//
//	store, err := NewS3Store(baseURL, "", "", false)
//	require.NoError(t, err)
//	store.WalkFrom(context.Background(), "", "helloworld.html", func(filename string) error {
//		assert.Equal(t, "helloworld.html", filename)
//		return fmt.Errorf("done")
//	})
//	assert.NoError(t, err)
//
//	store.WalkFrom(context.Background(), "", "hello", func(filename string) error {
//		assert.Equal(t, "helloworld.html", filename)
//		return fmt.Errorf("done")
//	})
//	assert.NoError(t, err)
//
//}

func TestNewS3Store(t *testing.T) {
	defaultEndpoint := "https://s3.test.amazonaws.com"

	tests := []struct {
		url              string
		expectedEndpoint string
		expectedBucket   string
		expectedPath     string
		expectedRegion   string
		expectedErr      error
	}{
		{url: "s3://bucket?region=test", expectedEndpoint: defaultEndpoint, expectedBucket: "bucket", expectedRegion: "test"},
		{url: "s3://bucket/path1?region=test", expectedEndpoint: defaultEndpoint, expectedBucket: "bucket", expectedPath: "path1", expectedRegion: "test"},
		{"s3://bucket/path1/path2?region=test", defaultEndpoint, "bucket", "path1/path2", "test", nil},

		{url: "s3://test.com/bucket?region=test", expectedEndpoint: "https://test.com", expectedBucket: "bucket", expectedRegion: "test"},
		{url: "s3://test.com/bucket/path1/?region=test", expectedEndpoint: "https://test.com", expectedBucket: "bucket", expectedPath: "path1", expectedRegion: "test"},
		{url: "s3://test.com/bucket/path1/path2?region=test", expectedEndpoint: "https://test.com", expectedBucket: "bucket", expectedPath: "path1/path2", expectedRegion: "test"},
		{url: "s3://test.com/bucket/path1/path2?region=test&insecure=true", expectedEndpoint: "http://test.com", expectedBucket: "bucket", expectedPath: "path1/path2", expectedRegion: "test"},

		{url: "s3://localhost:9000/store-tests/dstore-s3store-tests-63acbe181e32c21e?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin", expectedEndpoint: "http://localhost:9000", expectedBucket: "store-tests", expectedPath: "dstore-s3store-tests-63acbe181e32c21e", expectedRegion: "none"},
		{url: "s3://localhost:9000/store-tests?region=none&insecure=true", expectedEndpoint: "http://localhost:9000", expectedBucket: "store-tests", expectedRegion: "none"},

		{url: "s3://bucket-with.dot/path1?region=test&infer_aws_endpoint=true", expectedEndpoint: defaultEndpoint, expectedBucket: "bucket-with.dot", expectedPath: "path1", expectedRegion: "test"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			baseURL, err := url.Parse(test.url)
			require.NoError(t, err)

			store, err := NewS3Store(baseURL, "", "", false)
			if test.expectedErr == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expectedEndpoint, store.service.ClientInfo.Endpoint)
				assert.Equal(t, test.expectedRegion, store.service.ClientInfo.SigningRegion)

				assert.Equal(t, test.expectedBucket, store.bucket, "bucket not equals")
				assert.Equal(t, test.expectedPath, store.path, "path not equals")
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
			sub, err := store.SubStore("sub-folder")
			require.NoError(t, err)
			require.True(t, strings.HasSuffix(sub.BaseURL().Path, "sub-folder"))
		})
	}
}
