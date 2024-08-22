package local

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/dstore/storetests"
	"github.com/stretchr/testify/require"
)

var localStoreBasePath = os.Getenv("STORETESTS_LOCAL_STORE_PATH")

func TestLocalStore(t *testing.T) {
	storetests.TestAll(t, createlocalStoreFactory(t, ""))
}

func TestLocalStoreCompressedZst(t *testing.T) {
	storetests.TestAll(t, createlocalStoreFactory(t, "zstd"))
}

func TestLocalStore_CompressionAndMetering(t *testing.T) {
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

	storetests.TestAll(t, createlocalStoreFactory(t, "zstd", opts...))

	require.Equal(t, "compressedRead", compressedRead)
	require.Equal(t, "uncompressedRead", uncompressedRead)
	require.Equal(t, "compressedWrite", compressedWrite)
	require.Equal(t, "uncompressedWrite", uncompressedWrite)

	require.True(t, compressedReadByteCount > 0, "compressed read byte count should be greater than 0")
	require.True(t, compressedWriteByteCount > 0, "compressed write byte count should be greater than 0")
	require.True(t, uncompressedReadByteCount > 0, "uncompressed read byte count should be greater than 0")
	require.True(t, uncompressedWriteByteCount > 0, "uncompressed write byte count should be greater than 0")
}

func createlocalStoreFactory(t *testing.T, compression string, opts ...dstore.Option) storetests.StoreFactory {
	random := rand.NewSource(time.Now().UnixNano())

	return func() (dstore.Store, storetests.StoreDescriptor, storetests.StoreCleanup) {
		dir := localStoreBasePath
		removeOnExit := false
		suffix := "compression-none"
		if compression != "" {
			suffix = "compression-" + compression
		}

		if dir == "" {
			var err error
			dir, err = ioutil.TempDir("", fmt.Sprintf("dstore-localstore-tests-%08x", random.Int63()))
			require.NoError(t, err)
			removeOnExit = true
		} else {
			dir = path.Join(dir, suffix)
			os.RemoveAll(dir)
		}

		store, err := dstore.NewLocalStore(&url.URL{Scheme: "file", Path: dir}, "", compression, false, opts...)
		require.NoError(t, err)

		return store, storetests.StoreDescriptor{
				Compression: compression,
			}, func() {
				if storetests.NoCleanup {
					return
				}

				if removeOnExit {
					os.RemoveAll(dir)
				}
			}
	}
}
