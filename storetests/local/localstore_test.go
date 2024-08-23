package local

import (
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

func createlocalStoreFactory(t *testing.T, compression string) storetests.StoreFactory {
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

		store, err := dstore.NewLocalStore(&url.URL{Scheme: "file", Path: dir}, "", compression, false)
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
