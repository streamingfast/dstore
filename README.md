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
go test ./storetests/...
```

#### Cloud backends

To also run against real cloud providers, supply the relevant environment variables:

```bash
STORETESTS_GS_STORE_URL="gs://streamingfast-developement-random/store-tests" \
STORETESTS_S3_STORE_URL="s3://streamingfast-customer-outbox/store-tests?region=us-east-2" \
go test ./storetests/...
```

> [!NOTE]
> The bucket names above are placeholders — replace them with real buckets you have access to.

Any variable that is not set will cause the corresponding tests to be skipped automatically.

## Configuration

### S3

| Environment Variable | Default | Description |
|---|---|---|
| `DSTORE_S3_READ_ATTEMPTS` | `1` | Number of attempts for object read operations before returning an error. |
| `DSTORE_S3_BUFFERED_READ` | `false` | Set to `true` to buffer the full object into memory before returning it to the caller. Useful when the underlying stream is unreliable. |
| `DSTORE_S3_RETRY_PUSH_DELAY` | `0` (disabled) | Duration to wait between retries when pushing a local file (e.g. `500ms`, `2s`). Zero means no retry. |
| `DSTORE_S3_MAX_IDLE_CONNS` | `500` | Maximum number of idle (keep-alive) HTTP connections across all S3 hosts. |
| `DSTORE_S3_MAX_IDLE_CONNS_PER_HOST` | `100` | Maximum number of idle (keep-alive) HTTP connections per S3 host. Raise this for single-host setups (MinIO, Ceph) under heavy concurrency to avoid connection pool exhaustion. |
| `DSTORE_S3_IDLE_CONN_TIMEOUT` | `90s` | How long an idle HTTP connection is kept alive before being closed (e.g. `30s`, `2m`). |

> [!NOTE]
> Standard AWS SDK environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, etc.) are also honoured. See the [AWS SDK for Go documentation](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) for the full list.

### Google Cloud Storage

| Environment Variable | Default | Description |
|---|---|---|
| `DSTORE_WARN_SILENCED` | `false` | Set to `true` to log a warning whenever a GCS precondition-failed error is silenced (i.e. when writing without overwrite and the object already exists). |
| `STORAGE_EMULATOR_HOST` | — | Points the GCS client at a local emulator such as [fake-gcs-server](https://github.com/fsouza/fake-gcs-server). Format: `host:port` (e.g. `localhost:4443`). When set, authentication is disabled and the JSON API is used for reads. |

> [!NOTE]
> `GOOGLE_APPLICATION_CREDENTIALS` (path to a service-account JSON key) is the standard way to authenticate the GCS client. See the [Google Cloud authentication documentation](https://cloud.google.com/docs/authentication/application-default-credentials) for other options.

### Azure Blob Storage

| Environment Variable | Default | Description |
|---|---|---|
| `AZURE_STORAGE_KEY` | — | Shared-key credential for the Azure storage account. When set this takes precedence over all other credential sources. When unset, `DefaultAzureCredential` is used (supports Managed Identity, Service Principal via `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID`, Azure CLI, and VS Code credentials). |

## Contributing

**Issues and PR in this repo related strictly to the dstore library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/streamingfast/streamingfast#protocols)

**Please first refer to the general
[StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

## License

[Apache 2.0](LICENSE)

