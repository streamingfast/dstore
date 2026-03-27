package gs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
)

// benchSizesMiB are the payload sizes (MiB) exercised by each benchmark.
// They represent typical block-file sizes once compressed.
var benchSizesMiB = []int{7, 15, 30, 60, 120}

// benchCompressions covers both the uncompressed (raw upload) and zstd paths.
var benchCompressions = []struct {
	label string
	typ   string
}{
	{"none", ""},
	{"zstd", "zstd"},
}

var directConnectivityHeader sync.Once

// ── Write ────────────────────────────────────────────────────────────────────
//
// Run with:
//
//	./gs_bench.test -test.bench=BenchmarkGSStore_Write -test.run='^$' -test.benchtime=5x -test.v
//
// Compare HTTP vs gRPC:
//
//	benchstat -col /client output.txt

func BenchmarkGSStore_Write(b *testing.B) {
	if gsStoreBaseURL == "" {
		b.Skip("set STORETESTS_GS_STORE_URL to run")
	}

	setDirectConnectivityHeader(b, gsStoreBaseURL)

	b.Run("client=http", func(b *testing.B) { runGSWriteBenchmarks(b, gsStoreBaseURL, "") })
	b.Run("client=grpc", func(b *testing.B) { runGSWriteBenchmarks(b, gsStoreBaseURL, "grpc") })
}

func BenchmarkGSStore_Emulator_Write(b *testing.B) {
	if gsEmulatorStoreBaseURL == "" {
		b.Skip("set STORETESTS_GS_EMULATOR_STORE_URL to run")
	}

	setDirectConnectivityHeader(b, gsStoreBaseURL)

	b.Setenv("STORAGE_EMULATOR_HOST", "localhost:4443")
	runGSWriteBenchmarks(b, gsEmulatorStoreBaseURL, "")
}

// ── Read ─────────────────────────────────────────────────────────────────────

func BenchmarkGSStore_Read(b *testing.B) {
	if gsStoreBaseURL == "" {
		b.Skip("set STORETESTS_GS_STORE_URL to run")
	}

	setDirectConnectivityHeader(b, gsStoreBaseURL)

	b.Run("client=http", func(b *testing.B) { runGSReadBenchmarks(b, gsStoreBaseURL, "") })
	b.Run("client=grpc", func(b *testing.B) { runGSReadBenchmarks(b, gsStoreBaseURL, "grpc") })
}

func BenchmarkGSStore_Emulator_Read(b *testing.B) {
	if gsEmulatorStoreBaseURL == "" {
		b.Skip("set STORETESTS_GS_EMULATOR_STORE_URL to run")
	}

	setDirectConnectivityHeader(b, gsStoreBaseURL)

	b.Setenv("STORAGE_EMULATOR_HOST", "localhost:4443")
	runGSReadBenchmarks(b, gsEmulatorStoreBaseURL, "")
}

// ── Shared runners ───────────────────────────────────────────────────────────

func runGSWriteBenchmarks(b *testing.B, baseURL, clientProtocol string) {
	b.Helper()
	for _, c := range benchCompressions {
		b.Run(fmt.Sprintf("compression=%s", c.label), func(b *testing.B) {
			for _, sizeMiB := range benchSizesMiB {
				sizeBytes := sizeMiB * 1024 * 1024
				b.Run(fmt.Sprintf("%dMiB", sizeMiB), func(b *testing.B) {
					b.SetBytes(int64(sizeBytes))

					// Allocate inside the sub-benchmark so it can be GC'd once
					// this size finishes, rather than holding all sizes at once.
					data := newBenchData(sizeBytes)
					store := newBenchGSStore(b, baseURL, c.typ, clientProtocol)
					// A random prefix per run ensures -test.count repetitions never
					// reuse the same object name (delete+recreate within 1s triggers
					// ResourceExhausted). Cleanup walks only this run's prefix.
					runPrefix := fmt.Sprintf("bench-%x-", rand.Int64())
					b.Cleanup(func() {
						_ = store.Walk(context.Background(), runPrefix, func(name string) error {
							_ = store.DeleteObject(context.Background(), name)
							return nil
						})
					})

					b.ResetTimer()
					var n int
					for b.Loop() {
						err := store.WriteObject(context.Background(), fmt.Sprintf("%s%06d", runPrefix, n), bytes.NewReader(data))
						require.NoError(b, err)
						n++
					}
				})
			}
		})
	}
}

func runGSReadBenchmarks(b *testing.B, baseURL, clientProtocol string) {
	b.Helper()
	for _, c := range benchCompressions {
		b.Run(fmt.Sprintf("compression=%s", c.label), func(b *testing.B) {
			for _, sizeMiB := range benchSizesMiB {
				sizeBytes := sizeMiB * 1024 * 1024
				b.Run(fmt.Sprintf("%dMiB", sizeMiB), func(b *testing.B) {
					b.SetBytes(int64(sizeBytes))

					data := newBenchData(sizeBytes)
					store := newBenchGSStore(b, baseURL, c.typ, clientProtocol)
					// Unique name per run: GCS enforces ~1 mutation/s per object, so
					// reusing the same name across -test.count repetitions (delete then
					// write in quick succession) triggers ResourceExhausted. A random
					// suffix ensures each invocation targets a fresh object.
					objName := fmt.Sprintf("bench-read-%s-%dMiB-%x", c.label, sizeMiB, rand.Int64())

					// upload once; every iteration reads the same object
					err := store.WriteObject(context.Background(), objName, bytes.NewReader(data))
					require.NoError(b, err)
					b.Cleanup(func() { _ = store.DeleteObject(context.Background(), objName) })

					for b.Loop() {
						rc, err := store.OpenObject(context.Background(), objName)
						require.NoError(b, err)
						_, err = io.Copy(io.Discard, rc)
						require.NoError(b, err)
						require.NoError(b, rc.Close())
					}
				})
			}
		})
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// newBenchGSStore creates a store at <baseURL>/bench with overwrite enabled.
// clientProtocol, when non-empty, is set as the client_protocol query param.
func newBenchGSStore(b *testing.B, baseURL, compression, clientProtocol string) dstore.Store {
	b.Helper()
	storeURL, err := url.Parse(baseURL)
	require.NoError(b, err)
	storeURL.Path = strings.TrimRight(storeURL.Path, "/") + "/bench"

	if clientProtocol != "" {
		q := storeURL.Query()
		q.Set("client_protocol", clientProtocol)
		storeURL.RawQuery = q.Encode()
	}

	store, err := dstore.NewGSStore(storeURL, "", compression, true /* overwrite */)
	require.NoError(b, err)
	return store
}

// newBenchData produces size bytes of pseudo-random data from a fixed seed.
// Random data is effectively incompressible, representing pre-compressed
// blockchain payloads; the zstd benchmark variants therefore measure
// compression overhead rather than compression savings.
func newBenchData(size int) []byte {
	data := make([]byte, size)
	rng := rand.New(rand.NewPCG(42, 0))
	for i := range len(data) {
		data[i] = byte(rng.Uint32())
	}
	return data
}

func setDirectConnectivityHeader(b *testing.B, baseURL string) {
	directConnectivityHeader.Do(func() {
		supported, err := isDirectConnectivitySupported(b, baseURL)
		if err != nil {
			fmt.Printf("# Direct connectivity not supported due to: %q\n", err)
		}

		fmt.Printf("direct_connectivity: %t\n", supported)
	})
}

func isDirectConnectivitySupported(b *testing.B, baseURL string) (bool, error) {
	storeURL, err := url.Parse(baseURL)
	require.NoError(b, err)

	err = storage.CheckDirectConnectivitySupported(context.Background(), storeURL.Host)

	return err == nil, err
}
