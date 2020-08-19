package storetests

import (
	"testing"

	"github.com/dfuse-io/dstore"
)

func TestMockStore(t *testing.T) {
	TestAll(t, createMockStoreFactory(t, ""))
}

func createMockStoreFactory(t *testing.T, compression string) StoreFactory {
	return func() (dstore.Store, StoreCleanup) {
		return dstore.NewMockStore(nil), func() {
		}
	}
}
