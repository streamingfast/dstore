package storetests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/dfuse-io/dstore"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()
var noCleanup = os.Getenv("STORETESTS_NO_CLEANUP") == "true"

func TestAll(t *testing.T, factory StoreFactory) {
	all := [][]StoreTestFunc{
		fileExistsTests,
		walkTests,
		writeObjectTests,
	}

	for _, testFuncs := range all {
		for _, testFunc := range testFuncs {
			t.Run(getFunctionName(testFunc), func(t *testing.T) {
				testFunc(t, factory)
			})
		}
	}
}

type StoreCleanup func()
type StoreFactory func() (dstore.Store, StoreCleanup)
type StoreTestFunc func(t *testing.T, factory StoreFactory)

// getFunctionName reads the program counter adddress and return the function
// name representing this address.
//
// The `FuncForPC` format is in the form of `github.com/.../.../package.func`.
// As such, we use `filepath.Base` to obtain the `package.func` part and then
// split it at the `.` to extract the function name.
func getFunctionName(i interface{}) string {
	pcIdentifier := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	baseName := filepath.Base(pcIdentifier)
	parts := strings.SplitN(baseName, ".", 2)
	if len(parts) <= 1 {
		return parts[0]
	}

	return parts[1]
}

func addFileToStore(t *testing.T, store dstore.Store, id string, data string) {
	buf := bytes.NewBuffer([]byte(data))
	require.NoError(t, store.WriteObject(ctx, id, buf))
}

type testFile struct {
	id      string
	content string
}

func readObjectAndClose(t *testing.T, o io.ReadCloser) string {
	defer o.Close()
	data, err := ioutil.ReadAll(o)
	require.NoError(t, err)

	return string(data)
}

func supportsConcurrentWrites(store dstore.Store) bool {
	switch store.(type) {
	case *dstore.GSStore, *dstore.S3Store, *dstore.AzureStore:
		return true
	case *dstore.LocalStore, *dstore.MockStore:
		return false
	}

	panic(fmt.Errorf("unknown store type %T regarding support for concurrent writes", store))
}
