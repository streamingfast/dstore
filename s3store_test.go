package dstore

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		{"s3://bucket?region=test", defaultEndpoint, "bucket", "", "test", nil},
		{"s3://bucket/path1?region=test", defaultEndpoint, "bucket", "path1", "test", nil},
		{"s3://bucket/path1/path2?region=test", defaultEndpoint, "bucket", "path1/path2", "test", nil},

		{"s3://test.com/bucket?region=test", "https://test.com", "bucket", "", "test", nil},
		{"s3://test.com/bucket/path1/?region=test", "https://test.com", "bucket", "path1", "test", nil},
		{"s3://test.com/bucket/path1/path2?region=test", "https://test.com", "bucket", "path1/path2", "test", nil},
		{"s3://test.com/bucket/path1/path2?region=test&insecure=true", "http://test.com", "bucket", "path1/path2", "test", nil},

		{"s3://localhost:9000/store-tests/dstore-s3store-tests-63acbe181e32c21e?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin", "http://localhost:9000", "store-tests", "dstore-s3store-tests-63acbe181e32c21e", "none", nil},
		{"s3://localhost:9000/store-tests?region=none&insecure=true", "http://localhost:9000", "store-tests", "", "none", nil},

		{"s3://bucket-with.dot/path1?region=test&infer_aws_endpoint=true", defaultEndpoint, "bucket-with.dot", "path1", "test", nil},
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
		})
	}
}
