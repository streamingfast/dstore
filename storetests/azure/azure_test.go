package azure

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/dstore/storetests"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var zlog, tracer = logging.PackageLogger("dstore", "github.com/streamingfast/dstore/storetests/azure")

// For dfusers, one can use: `export STORETESTS_AZ_STORE_URL=az://streamingfasttest01.myblobs`
var azStoreBaseURL = os.Getenv("STORETESTS_AZ_STORE_URL")

func TestAZStore(t *testing.T) {
	if azStoreBaseURL == "" {
		t.Skip("You must provide a valid Azure Bucket via STORETESTS_AZ_STORE_URL environment variable to execute those tests, ex: az://streamingfasttest01.myblobs")
		return
	}

	storetests.TestAll(t, createAZStoreFactory(t, azStoreBaseURL, "", false))
}

func TestAZStore_Overwrite(t *testing.T) {
	if azStoreBaseURL == "" {
		t.Skip("You must provide a valid Azure Bucket via STORETESTS_AZ_STORE_URL environment variable to execute those tests, ex: az://streamingfasttest01.myblobs")
		return
	}

	storetests.TestAll(t, createAZStoreFactory(t, azStoreBaseURL, "", true))
}

func TestAZStore_CompressionAndMetering(t *testing.T) {
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

	if azStoreBaseURL == "" {
		t.Skip("You must provide a valid Azure Bucket via STORETESTS_AZ_STORE_URL environment variable to execute those tests, ex: az://streamingfasttest01.myblobs")
		return
	}

	storetests.TestAll(t, createAZStoreFactory(t, azStoreBaseURL, "zstd", false, opts...))

	require.Equal(t, "compressedRead", compressedRead)
	require.Equal(t, "uncompressedRead", uncompressedRead)
	require.Equal(t, "compressedWrite", compressedWrite)
	require.Equal(t, "uncompressedWrite", uncompressedWrite)

	require.True(t, compressedReadByteCount > 0, "compressed read byte count should be greater than 0")
	require.True(t, compressedWriteByteCount > 0, "compressed write byte count should be greater than 0")
	require.True(t, uncompressedReadByteCount > 0, "uncompressed read byte count should be greater than 0")
	require.True(t, uncompressedWriteByteCount > 0, "uncompressed write byte count should be greater than 0")
}

func createAZStoreFactory(t *testing.T, directory string, compression string, overwrite bool, opts ...dstore.Option) storetests.StoreFactory {
	random := rand.NewSource(time.Now().UnixNano())

	return func() (dstore.Store, storetests.StoreDescriptor, storetests.StoreCleanup) {
		testPath := fmt.Sprintf("dstore-azstore-tests-%08x", random.Int63())
		fullPath := directory
		if !strings.HasSuffix(fullPath, "/") {
			fullPath += "/"
		}

		storeURL, err := url.Parse(fullPath + testPath)
		require.NoError(t, err)

		zlog.Debug("creating a new azstore for test", zap.Stringer("url", storeURL), zap.String("host", storeURL.Host), zap.String("path", storeURL.Path))
		store, err := dstore.NewAzureStore(storeURL, "", compression, overwrite, opts...)
		require.NoError(t, err)

		client, err := storage.NewClient(context.Background())
		require.NoError(t, err)

		return store, storetests.StoreDescriptor{
				Compression: compression,
			}, func() {
				if storetests.NoCleanup {
					client.Close()
					return
				}
				store.Walk(context.Background(), "", func(filename string) error {
					return store.DeleteObject(context.Background(), filename)
				})
			}
	}
}
