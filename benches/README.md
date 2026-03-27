# GCS Store Benchmarks

Benchmarks for the GCS HTTP and gRPC clients across a range of file sizes and compression modes.

## Prerequisites

| What | How |
|------|-----|
| GCS bucket | Set `STORETESTS_GS_STORE_URL=gs://<bucket>/<prefix>` |
| Credentials | ADC (`gcloud auth application-default login`) or `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` or workload identity on GKE |
| benchstat | `go install golang.org/x/perf/cmd/benchstat@latest` |

## Compile

```bash
# Local architecture
go test -c ./storetests/gs/ -o gs_bench.test

# Cross-compile for a Linux amd64 cluster node
GOOS=linux GOARCH=amd64 go test -c ./storetests/gs/ -o gs_bench.test
```

## Run

`-test.count=6` is the minimum for benchstat's 95 % confidence interval.
`-test.benchtime=3x` runs each operation 3 times per sample to stabilise timing.

```bash
STORETESTS_GS_STORE_URL=gs://<bucket>/<prefix> \
  ./gs_bench.test \
    -test.bench=BenchmarkGSStore_Read \
    -test.run='^$' \
    -test.benchtime=3x \
    -test.count=6 \
    -test.v \
  | tee read.txt
```

Run write benchmarks the same way:

```bash
STORETESTS_GS_STORE_URL=gs://<bucket>/<prefix> \
  ./gs_bench.test \
    -test.bench=BenchmarkGSStore_Write \
    -test.run='^$' \
    -test.benchtime=3x \
    -test.count=6 \
    -test.v \
  | tee write.txt
```

## Compare HTTP vs gRPC

```bash
benchstat -col /client read.txt
benchstat -col /client write.txt
```

Example output:

```
goos: linux
goarch: amd64

                                          │    http     │              grpc               │
                                          │    MB/s     │    MB/s      vs base            │
BenchmarkGSStore_Read/compression=none/7MiB    98.40        136.03   +38.24% (p=0.002 n=6)
BenchmarkGSStore_Read/compression=none/15MiB   99.35        147.21   +48.16% (p=0.002 n=6)
BenchmarkGSStore_Read/compression=none/30MiB  128.15        260.15  +103.00% (p=0.002 n=6)
```

## Tips

- Run on a GKE node in the **same region** as the bucket for representative latency.
- Use `-test.count=10` for tighter confidence intervals on noisy networks.
- Filter to a single benchmark with `-test.bench=BenchmarkGSStore_Read/client=grpc/compression=none`.
- The emulator benchmark (`BenchmarkGSStore_Emulator_*`) requires `docker compose up -d fake-gcs` and `STORETESTS_GS_EMULATOR_STORE_URL=gs://store-tests`.
