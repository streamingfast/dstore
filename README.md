# StreamingFast Storage Abstraction
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/streamingfast/dstore)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

`dstore` is a simple abstraction on top of Local storage and Cloud
storage. It handles commonly used functions to store things (locally,
or on cloud storage providers), list files, delete, etc..

It is used by **[StreamingFast](https://github.com/streamingfast/streamingfast)**.

## Features

It currently supports:
* AWS S3 (`s3://[bucket]/path?region=us-east-1`, with [AWS-specific env vars](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html))
    * Minio (through the S3 interface)
* Google Storage (`gs://[bucket]/path`, with `GOOGLE_APPLICATION_CREDENTIALS` env var set)
* Azure Blob Storage (`az://[account].[container]/path`, with `AZURE_STORAGE_KEY` env var set)
* Local file systems (including virtual of fused-based) (`file:///` prefix)

### Compression

The `compression_config` query parameter of a store URL, whatever the scheme, tunes
how that store compresses the files it writes:

* zstd stores: `<level>` or `<level>/<window MiB>`, for example `best`, `better/32`
  or `best/64`. Levels are `fastest`, `default`, `better` and `best`; the window
  must be a power of two.
* gzip stores: an integer level from `-2` to `9`, as defined by `compress/gzip`:
  `1` is the fastest, `9` the smallest.

For example, `NewDBinStore("gs://bucket/merged-blocks?compression_config=best/32")`.

The store constructor fails when the value is not valid for the store's
compression (`8` on a zstd store, `better` on a gzip one), or when the store has
no compression. Sub-stores and clones keep the setting, and `NewStore` logs it.

Nothing changes on the read side: decoders take the window from the frame header.
Every reader needs memory for that window, though, and the `zstd` command line tool
refuses windows above 128 MiB unless run with `--long=31` or `--memory`.

Measured on BNB Chain merged blocks (16-core arm64), relative to the defaults:

| config | compressed size | encode | decode |
|---|---|---|---|
| (unset) | 100% | 879 MB/s | 2362 MB/s |
| `better` | 89.5% | 475 MB/s | 2466 MB/s |
| `better/32` | 86.1% | 490 MB/s | 1347 MB/s |
| `best` | 86.8% | 73 MB/s | 2512 MB/s |
| `best/32` | 77.6% | 66 MB/s | 1365 MB/s |
| `best/64` | 73.6% | 64 MB/s | 892 MB/s |
| `best/128` | 71.4% | 64 MB/s | 623 MB/s |

### Testing

The `storetests` package contains all our integration tests we perform on our store implementation.
Some of the store implementations can be tested directly while few others, from Cloud Providers
essentially, requires some extra environment variables to run. They are skipped if the correct
environment variables for the provider are not set.

#### Local backends (MinIO + Ceph RGW + fake-gcs-server)

A `docker-compose.yml` is provided at the root of the repository. It starts:
- **MinIO** on port `9000` — S3-compatible object storage
- **Ceph RGW** on port `8080` — built from `docker/ceph-local` using `quay.io/ceph/ceph:v19` (native arm64 + amd64); bootstraps a single-node cluster on first start (~30–60 s)
- **fake-gcs-server** on port `4443` — GCS-compatible object storage (native arm64 + amd64); data is in-memory (lost on restart)

```bash
docker compose up -d
```

Once the containers are healthy, run the local tests:

```bash
STORETESTS_S3_MINIO_STORE_URL="s3://localhost:9000/store-tests?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin" \
STORETESTS_S3_CEPH_STORE_URL="s3://localhost:8080/store-tests?region=none&insecure=true&access_key_id=cephaccesskey&secret_access_key=cephsecretkey" \
STORETESTS_GS_EMULATOR_STORE_URL="gs://store-tests" \
STORAGE_EMULATOR_HOST="localhost:4443" \
go test ./...
```

#### Cloud backends

To also run against real cloud providers, supply the relevant environment variables:

```bash
STORETESTS_GS_STORE_URL="gs://streamingfast-developement-random/store-tests" \
STORETESTS_S3_STORE_URL="s3://streamingfast-customer-outbox/store-tests?region=us-east-2" \
go test ./...
```

> [!NOTE]
> The bucket names above are placeholders — replace them with real buckets you have access to.

Any variable that is not set will cause the corresponding tests to be skipped automatically.

## Contributing

**Issues and PR in this repo related strictly to the dstore library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/streamingfast/streamingfast#protocols)

**Please first refer to the general
[StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

## License

[Apache 2.0](LICENSE)

