package s3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/dstore/storetests"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/assert"
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

// You can start a local Ceph RGW cluster via docker compose (see docker/ceph-local):
//
//	docker compose up -d ceph
//
// And then use:
//
//	STORETESTS_S3_CEPH_STORE_URL="s3://localhost:8080/store-tests?region=none&insecure=true&access_key_id=cephaccesskey&secret_access_key=cephsecretkey"
var s3CephStoreBaseURL = os.Getenv("STORETESTS_S3_CEPH_STORE_URL")

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

func TestS3Store_Minio_CompressionAndMetering(t *testing.T) {
	compressedReadByteCount := 0
	compressedWriteByteCount := 0
	uncompressedReadByteCount := 0
	uncompressedWriteByteCount := 0

	var uncompressedRead string
	var compressedRead string
	var compressedWrite string
	var uncompressedWrite string

	opts := []dstore.Option{
		dstore.WithCompressedReadCallback(func(ctx context.Context, i int) {
			compressedReadByteCount += i
			compressedRead = "compressedRead"
		}),
		dstore.WithUncompressedReadCallback(func(ctx context.Context, i int) {
			uncompressedReadByteCount += i
			uncompressedRead = "uncompressedRead"
		}),
		dstore.WithCompressedWriteCallback(func(ctx context.Context, i int) {
			compressedWriteByteCount += i
			compressedWrite = "compressedWrite"
		}),
		dstore.WithUncompressedWriteCallback(func(ctx context.Context, i int) {
			uncompressedWriteByteCount += i
			uncompressedWrite = "uncompressedWrite"
		}),
	}

	if s3MinioStoreBaseURL == "" {
		t.Skip("You must provide a valid Google Storage Bucket via STORETESTS_GS_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3MinioStoreBaseURL, "zstd", false, false, opts...))

	require.Equal(t, "compressedRead", compressedRead)
	require.Equal(t, "uncompressedRead", uncompressedRead)
	require.Equal(t, "compressedWrite", compressedWrite)
	require.Equal(t, "uncompressedWrite", uncompressedWrite)

	require.True(t, compressedReadByteCount > 0, "compressed read byte count should be greater than 0")
	require.True(t, compressedWriteByteCount > 0, "compressed write byte count should be greater than 0")
	require.True(t, uncompressedReadByteCount > 0, "uncompressed read byte count should be greater than 0")
	require.True(t, uncompressedWriteByteCount > 0, "uncompressed write byte count should be greater than 0")
}

func TestS3Store_Ceph(t *testing.T) {
	if s3CephStoreBaseURL == "" {
		t.Skip("You must provide a valid Ceph RGW S3 URL via STORETESTS_S3_CEPH_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3CephStoreBaseURL, "", false, false))
}

func TestS3Store_Ceph_EmptyBucket_FilePrefix(t *testing.T) {
	if s3CephStoreBaseURL == "" {
		t.Skip("You must provide a valid Ceph RGW S3 URL via STORETESTS_S3_CEPH_STORE_URL environment variable to execute those tests")
		return
	}

	storetests.TestWalk_FilePrefix(t, createS3StoreFactory(t, s3CephStoreBaseURL, "", false, true))
}

func TestS3Store_Ceph_CompressionAndMetering(t *testing.T) {
	if s3CephStoreBaseURL == "" {
		t.Skip("You must provide a valid Ceph RGW S3 URL via STORETESTS_S3_CEPH_STORE_URL environment variable to execute those tests")
		return
	}

	compressedReadByteCount := 0
	compressedWriteByteCount := 0
	uncompressedReadByteCount := 0
	uncompressedWriteByteCount := 0

	var uncompressedRead string
	var compressedRead string
	var compressedWrite string
	var uncompressedWrite string

	opts := []dstore.Option{
		dstore.WithCompressedReadCallback(func(ctx context.Context, i int) {
			compressedReadByteCount += i
			compressedRead = "compressedRead"
		}),
		dstore.WithUncompressedReadCallback(func(ctx context.Context, i int) {
			uncompressedReadByteCount += i
			uncompressedRead = "uncompressedRead"
		}),
		dstore.WithCompressedWriteCallback(func(ctx context.Context, i int) {
			compressedWriteByteCount += i
			compressedWrite = "compressedWrite"
		}),
		dstore.WithUncompressedWriteCallback(func(ctx context.Context, i int) {
			uncompressedWriteByteCount += i
			uncompressedWrite = "uncompressedWrite"
		}),
	}

	storetests.TestAll(t, createS3StoreFactory(t, s3CephStoreBaseURL, "zstd", false, false, opts...))

	require.Equal(t, "compressedRead", compressedRead)
	require.Equal(t, "uncompressedRead", uncompressedRead)
	require.Equal(t, "compressedWrite", compressedWrite)
	require.Equal(t, "uncompressedWrite", uncompressedWrite)

	require.True(t, compressedReadByteCount > 0, "compressed read byte count should be greater than 0")
	require.True(t, compressedWriteByteCount > 0, "compressed write byte count should be greater than 0")
	require.True(t, uncompressedReadByteCount > 0, "uncompressed read byte count should be greater than 0")
	require.True(t, uncompressedWriteByteCount > 0, "uncompressed write byte count should be greater than 0")
}

// TestS3Store_Minio_NoConnectionLeak verifies that partially-read objects don't
// exhaust the HTTP connection pool. Each iteration opens an object, reads only
// the first byte, then closes the reader. With a correct implementation the
// underlying HTTP body is drained on Close() so the connection is returned to
// the pool and reused for the next request. Without the fix every open would
// create a new TCP connection, and after MaxIdleConnsPerHost requests the pool
// would overflow — visible as new connections instead of reused ones.
//
// Both the uncompressed and zstd-compressed paths are exercised because the
// Close() chain differs between them (compression adds an extra reader layer
// that can inadvertently close the HTTP body before the drain runs).
func TestS3Store_Minio_NoConnectionLeak(t *testing.T) {
	if s3MinioStoreBaseURL == "" {
		t.Skip("You must provide a valid Minio S3 URL via STORETESTS_S3_MINIO_STORE_URL environment variable to execute those tests")
		return
	}

	for _, compression := range []string{"", "zstd"} {
		name := "uncompressed"
		if compression != "" {
			name = compression
		}
		t.Run(name, func(t *testing.T) {
			assertNoConnectionLeak(t, createS3StoreFactory(t, s3MinioStoreBaseURL, compression, false, false))
		})
	}
}

func assertNoConnectionLeak(t *testing.T, factory storetests.StoreFactory) {
	t.Helper()

	store, _, cleanup := factory()
	defer cleanup()

	// Write a small file once — this establishes the first TCP connection
	// outside of the traced region so the loop starts from an idle connection.
	err := store.WriteObject(t.Context(), "connleak-probe", strings.NewReader("hello connection pool"))
	require.NoError(t, err)

	const iterations = 500
	var newConns, reusedConns atomic.Int64

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reusedConns.Add(1)
			} else {
				newConns.Add(1)
			}
		},
	}
	tracedCtx := httptrace.WithClientTrace(t.Context(), trace)

	// Open the object many times, reading only the first byte each time.
	// This is the scenario that triggered the original leak: the caller
	// abandons the stream after a partial read and relies on Close() to
	// clean up, which must drain the HTTP body so the connection is reusable.
	buf := make([]byte, 1)
	for range iterations {
		rc, err := store.OpenObject(tracedCtx, "connleak-probe")
		require.NoError(t, err)
		_, err = rc.Read(buf)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}

	t.Logf("connections: new=%d reused=%d (out of %d iterations)", newConns.Load(), reusedConns.Load(), iterations)

	// After the WriteObject above drains the pool, every traced OpenObject
	// should reuse the idle connection. We allow a small slack (≤5 new) in
	// case the server closes a keep-alive connection between iterations.
	assert.LessOrEqual(t, newConns.Load(), int64(5), "too many new connections — possible connection pool leak")
	assert.GreaterOrEqual(t, reusedConns.Load(), int64(iterations-5), "too few reused connections — possible connection pool leak")
}

func createS3StoreFactory(t *testing.T, baseURL string, compression string, overwrite bool, emptyBucket bool, opts ...dstore.Option) storetests.StoreFactory {
	random := rand.NewSource(time.Now().UnixNano())

	return func() (dstore.Store, storetests.StoreDescriptor, storetests.StoreCleanup) {
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

		configOptions, bucket, path, _, err := dstore.ParseS3URL(storeURL)
		require.NoError(t, err)

		zlog.Debug("creating a new s3store for test",
			zap.Stringer("url", storeURL),
			zap.String("bucket", bucket),
			zap.String("path", path),
		)

		store, err := dstore.NewS3Store(storeURL, "", compression, overwrite, opts...)
		require.NoError(t, err)

		cfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)
		require.NoError(t, err)

		client := s3.NewFromConfig(cfg)

		if emptyBucket {
			prefix := strings.TrimLeft(path, "/") + "/"
			input := &s3.ListObjectsV2Input{
				Bucket: aws.String(bucket),
				Prefix: aws.String(prefix),
			}
			seenFile := ""
			paginator := s3.NewListObjectsV2Paginator(client, input)

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					t.Fatalf("error returned: %s", err)
				}
				for _, obj := range page.Contents {
					seenFile = *obj.Key
					break
				}
				if seenFile != "" {
					break
				}
			}

			if seenFile != "" {
				t.Fatalf("requested empty bucket, but given s3 store URL bucket (%s) is not empty", baseURL)
			}
		}

		return store, storetests.StoreDescriptor{
				Compression: compression,
			}, func() {
				if storetests.NoCleanup {
					return
				}

				prefix := strings.TrimLeft(path, "/") + "/"
				input := &s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
					Prefix: aws.String(prefix),
				}

				if tracer.Enabled() {
					zlog.Debug("cleaning out bucket", zap.String("bucket", bucket), zap.String("prefix", prefix))
				}

				paginator := s3.NewListObjectsV2Paginator(client, input)

				for paginator.HasMorePages() {
					page, err := paginator.NextPage(ctx)
					require.NoError(t, err)

					for _, obj := range page.Contents {
						_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(bucket),
							Key:    obj.Key,
						})
						require.NoError(t, err)
					}
				}
			}
	}
}
