package storetests

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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

// For dfusers, one can use:
//	STORETESTS_GS_STORE_URL="gs://dfuse-developement-random/store-tests"
var gsstoreBaseURL = os.Getenv("STORETESTS_GS_STORE_URL")

func TestGSStore(t *testing.T) {
	if gsstoreBaseURL == "" {
		t.Skip("You must provide a valid Google Storage Bucket via STORETESTS_GS_STORE_URL environment variable to execute those tests")
		return
	}

	TestAll(t, createGSStoreFactory(t, gsstoreBaseURL, "", false))
}

func TestGSStore_Overwrite(t *testing.T) {
	if gsstoreBaseURL == "" {
		t.Skip("You must provide a valid Google Storage Bucket via STORETESTS_GS_STORE_URL environment variable to execute those tests")
		return
	}

	TestAll(t, createGSStoreFactory(t, gsstoreBaseURL, "", true))
}

func createGSStoreFactory(t *testing.T, directory string, compression string, overwrite bool) StoreFactory {
	random := rand.NewSource(time.Now().UnixNano())

	return func() (dstore.Store, StoreCleanup) {
		testPath := fmt.Sprintf("dstore-gsstore-tests-%08x", random.Int63())
		fullPath := gsstoreBaseURL
		if !strings.HasSuffix(fullPath, "/") {
			fullPath += "/"
		}

		storeURL, err := url.Parse(fullPath + testPath)
		require.NoError(t, err)

		zlog.Debug("creating a new gsstore for test", zap.Stringer("url", storeURL), zap.String("host", storeURL.Host), zap.String("path", storeURL.Path))
		store, err := dstore.NewGSStore(storeURL, "", compression, overwrite)
		require.NoError(t, err)

		client, err := storage.NewClient(context.Background())
		require.NoError(t, err)

		return store, func() {
			if noCleanup {
				client.Close()
				return
			}

			bucket := client.Bucket(storeURL.Host)
			itr := client.Bucket(storeURL.Host).Objects(context.Background(), &storage.Query{
				Prefix: strings.TrimLeft(storeURL.Path, "/") + "/",
			})

			if traceEnabled {
				zlog.Debug("cleaning out bucket", zap.String("bucket", storeURL.Host), zap.String("prefix", storeURL.Path))
			}

			for {
				value, err := itr.Next()
				if err == iterator.Done {
					break
				}

				require.NoError(t, err)
				object := bucket.Object(value.Name)

				if traceEnabled {
					zlog.Debug("about to delete bucket file", zap.String("name", value.Name))
				}
				err = object.Delete(context.Background())
				require.NoError(t, err)
			}

			client.Close()
		}
	}
}
