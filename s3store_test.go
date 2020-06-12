package dstore

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

func TestS3StoreWriteObject(t *testing.T) {
	t.Skip() // need s3 access to test this, do it on your PC
	// Requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be set

	// https://s3.console.aws.amazon.com/s3/buckets/dfuse-customer-outbox/?region=us-east-2&tab=overview
	base, _ := url.Parse("s3://dfuse-customer-outbox/testing?region=us-east-2")
	s, err := NewS3Store(base, "", "", false)
	require.NoError(t, err)

	content := "hello world"
	err = s.WriteObject(bctx, "temp.txt", bytes.NewReader([]byte(content)))
	assert.NoError(t, err)

	err = s.Walk(bctx, "eosio.token-transfers-01158", "", func(fname string) error {
		fmt.Println("Listed name", fname)
		return nil
	})
	assert.NoError(t, err)

	rd, err := s.OpenObject(bctx, "temp.txt")
	assert.NoError(t, err)
	cnt, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	rd.Close()
	assert.Equal(t, content, string(cnt))

	err = s.DeleteObject(bctx, "temp.txt")
	assert.NoError(t, err)
}
