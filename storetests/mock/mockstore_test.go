package mock

import (
	"testing"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/dstore/storetests"
)

func TestMockStore(t *testing.T) {
	storetests.TestAll(t, createMockStoreFactory(t, ""))
}

func createMockStoreFactory(t *testing.T, compression string) storetests.StoreFactory {
	return func() (dstore.Store, storetests.StoreCleanup) {
		return dstore.NewMockStore(nil), func() {
		}
	}
}
