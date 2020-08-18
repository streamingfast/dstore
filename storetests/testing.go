package storetests

import (
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/dfuse-io/dstore"
)

func TestAll(t *testing.T, factory StoreFactory) {
	all := map[string][]StoreTestFunc{
		"file_exists": fileExistsTest,
	}

	for category, testFuncs := range all {
		for _, testFunc := range testFuncs {
			t.Run(category+"/"+getFunctionName(testFunc), func(t *testing.T) {
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
