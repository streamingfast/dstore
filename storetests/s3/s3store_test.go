package s3

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/dstore/storetests"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var ctx = context.Background()
var zlog, tracer = logging.PackageLogger("dstore", "github.com/streamingfast/dstore/storetests/s3")

// For dfusers, one can use:
//
//	STORETESTS_S3_STORE_URL="s3://dfuse-customer-outbox/store-tests?region=us-east-2"
//
// @see https://s3.console.aws.amazon.com/s3/buckets/dfuse-customer-outbox/?region=us-east-2&tab=overview
var s3StoreBaseURL = os.Getenv("STORETESTS_S3_STORE_URL")

// You can start `minio` on your computer with:
//
// ```
// mkdir -p /tmp/minio-tests/store-tests
// cd /tmp/minio-tests
// minio server .
// ```
//
// And then use:
//
//	STORETESTS_S3_MINIO_STORE_URL="s3://localhost:9000/store-tests?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin"
var s3MinioStoreBaseURL = os.Getenv("STORETESTS_S3_MINIO_STORE_URL")

func TestS3Store(t *testing.T) {
	if s3StoreBaseURL == "" {
		t.Skip("You must provide a valid S3 URL via STORETESTS_S3_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3StoreBaseURL, "", false, false))
}

func TestS3Store_Overwrite(t *testing.T) {
	if s3StoreBaseURL == "" {
		t.Skip("You must provide a valid S3 URL via STORETESTS_S3_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3StoreBaseURL, "", true, false))
}

func TestS3Store_Minio(t *testing.T) {
	if s3MinioStoreBaseURL == "" {
		t.Skip("You must provide a valid Minio S3 URL via STORETESTS_S3_MINIO_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3MinioStoreBaseURL, "", false, false))
}

func TestS3Store_Minio_EmptyBucket_FilePrefix(t *testing.T) {
	if s3MinioStoreBaseURL == "" {
		t.Skip("You must provide a valid Minio S3 URL via STORETESTS_S3_MINIO_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestWalk_FilePrefix(t, createS3StoreFactory(t, s3MinioStoreBaseURL, "", false, true))
}

func createS3StoreFactory(t *testing.T, baseURL string, compression string, overwrite bool, emptyBucket bool) storetests.StoreFactory {
	random := rand.NewSource(time.Now().UnixNano())

	return func() (dstore.Store, storetests.StoreCleanup) {
		storeURL, err := url.Parse(baseURL)
		require.NoError(t, err)

		if !emptyBucket {
			testPath := fmt.Sprintf("dstore-s3store-tests-%08x", random.Int63())
			fullPath := storeURL.Path
			if !strings.HasSuffix(fullPath, "/") {
				fullPath += "/"
			}

			storeURL.Path = fullPath + testPath
		}

		awsConfig, bucket, path, err := dstore.ParseS3URL(storeURL)
		require.NoError(t, err)

		zlog.Debug("creating a new s3store for test",
			zap.Stringer("url", storeURL),
			zap.String("bucket", bucket),
			zap.String("path", path),
		)

		store, err := dstore.NewS3Store(storeURL, "", compression, overwrite)
		require.NoError(t, err)

		sess, err := session.NewSession(awsConfig)
		require.NoError(t, err)

		client := s3.New(sess)

		if emptyBucket {
			prefix := strings.TrimLeft(path, "/") + "/"
			query := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: &prefix}
			seenFile := ""
			err := client.ListObjectsV2PagesWithContext(ctx, query, func(page *s3.ListObjectsV2Output, _ bool) bool {
				for _, el := range page.Contents {
					seenFile = *el.Key
				}
				return false
			})
			if err != nil {
				t.Fatalf("error returned: %s", err)
			}
			if seenFile != "" {
				t.Fatalf("requested empty bucket, but given s3 store URL bucket (%s) is not empty", baseURL)
			}
		}

		return store, func() {
			if storetests.NoCleanup {
				return
			}

			prefix := strings.TrimLeft(path, "/") + "/"
			query := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: &prefix}

			if tracer.Enabled() {
				zlog.Debug("cleaning out bucket", zap.String("bucket", bucket), zap.String("prefix", prefix))
			}

			var innerErr error
			err := client.ListObjectsV2PagesWithContext(ctx, query, func(page *s3.ListObjectsV2Output, _ bool) bool {
				for _, el := range page.Contents {
					_, err := client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(bucket),
						Key:    el.Key,
					})
					if err != nil {
						innerErr = err
						return false
					}
				}
				return true
			})

			require.NoError(t, err)
			require.NoError(t, innerErr)
		}
	}
}
