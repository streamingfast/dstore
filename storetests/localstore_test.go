package storetests

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dfuse-io/dstore"
	"github.com/stretchr/testify/require"
)

func TestLocalStore(t *testing.T) {
	TestAll(t, createlocalStoreFactory(t, ""))
}

func TestLocalStoreCompressedZst(t *testing.T) {
	TestAll(t, createlocalStoreFactory(t, "zstd"))
}

func createlocalStoreFactory(t *testing.T, compression string) StoreFactory {
	return func() (dstore.Store, StoreCleanup) {
		name, err := ioutil.TempDir("", "dstore-local-store")
		require.NoError(t, err)

		store, err := dstore.NewLocalStore(name, "", compression, false)
		require.NoError(t, err)

		return store, func() {
			os.RemoveAll(name)
		}
	}
}
