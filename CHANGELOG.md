# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See [MAINTAINERS.md](./MAINTAINERS.md) for instructions to keep up to date.

## Unreleased

### Fixed

* S3 store: `WriteObject` now drains the input reader when skipping a write because the destination already exists and `overwrite` is disabled. Previously the reader was left untouched, which would deadlock pipe-based producers (e.g. a goroutine writing to an `io.Pipe`) and leak both the goroutine and any memory it had captured.

## v0.2.3

### Fixed

* GCS store: disable gRPC DirectPath when both a project is set and `client_protocol=grpc` is used, preventing connection issues in that configuration.
* S3 store: share a single HTTP transport across all store clones for proper HTTP/2 multiplexing, replacing the previous per-clone transport that broke connection sharing.

## v0.2.2

### Added

* GCS store: opt-in gRPC transport via `client_protocol=grpc` query parameter (e.g. `gs://bucket/path?client_protocol=grpc`). Defaults to the existing HTTP client; the gRPC client is selected only when this parameter is explicitly set.
* S3 store: `storage_class` query parameter as the canonical snake_case name for `storageClass`.

### Changed

* S3 store: each store clone now gets its own transport for failure isolation; adds `ResponseHeaderTimeout` to prevent hung requests and configures HTTP/2 health checks via `x/net/http2`; default connection pool sizes are reduced.

### Deprecated

* S3 store: `storageClass` query parameter is deprecated in favour of `storage_class`; a warning is logged when the old form is used.

## v0.2.1

### Added

* S3 store: fixed connection pool leak when closing an object after a partial read; the raw HTTP body must be drained before closing the outer reader chain — closing outer first (when there is no compression layer) already closes the body, making the subsequent drain a no-op and preventing connection reuse

## v0.2.0

### Added

* Added `docker-compose.yml` with MinIO, Ceph RGW, and fake-gcs-server for local integration testing
* Added S3 integration tests for Ceph RGW (`TestS3Store_Ceph`, `TestS3Store_Ceph_EmptyBucket_FilePrefix`, `TestS3Store_Ceph_CompressionAndMetering`)
* Added GCS integration tests for fake-gcs-server emulator (`TestGSStore_Emulator`, `TestGSStore_Emulator_Overwrite`, `TestGSStore_Emulator_CompressionAndMetering`)
* S3 store: configurable HTTP connection pool via `DSTORE_S3_MAX_IDLE_CONNS`, `DSTORE_S3_MAX_IDLE_CONNS_PER_HOST`, `DSTORE_S3_IDLE_CONN_TIMEOUT` env vars

### Fixed

* S3 store: fixed goroutine leak caused by connection pool exhaustion on single-host S3 stores (e.g. MinIO); HTTP body is now explicitly drained and closed, and the transport is configured with `MaxIdleConnsPerHost=100` by default
* GCS store now uses the JSON API for object reads when `STORAGE_EMULATOR_HOST` is set, fixing compatibility with fake-gcs-server (which does not handle the XML API with percent-encoded path slashes)

# v0.1.2

## Changed

* Migrated from AWS SDK for Go v1 to v2 (`github.com/aws/aws-sdk-go` → `github.com/aws/aws-sdk-go-v2`)
  - Public API remains backward compatible for dstore users

# v0.1.1

## Added

* Added support for "workload identity credentials" in Azure. Order of preference is:
  - If `AZURE_STORAGE_KEY` is set, use shared key credential (previous behavior)
	- Otherwise, use DefaultAzureCredential which supports:
	  - Managed Identity (for Azure resources)
	  - Service Principal (via AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
	  - Azure CLI credentials
	  - Visual Studio Code credentials

## Changed

* Legacy azure library changed from `github.com/Azure/azure-storage-blob-go` to `github.com/Azure/azure-sdk-for-go/sdk`

# 2025-11-05

## Fixed

* Fixed `WalkFrom` on `S3` and `GCS` when both `Prefix` and `StartingPoint` was provided.

## Added

* Added `SetMetadata` to interface, which you can now get back from ObjectAttributes.

* Added `storageClass` query parameter for the s3 store to define a storage class on upload.

* Added Clonable interface so you can call `dstore.Clone(ctx)` on a remote store, instantiate a new network client and context.

* Added `dstore.ReadObject` to easily read a single file from a `fileURL`.

* Added `dstore.NewStoreFromFileURL` to replace `dstore.NewStoreFromURL` which was not clear that it's usage is meant to create a store from a file directly.

* Added `dstore.OpenObject` that is able to open a single store element without having to create a separate store, this is a shortcut for splitting the path & filename, creating a new store from the path and then calling `store.OpenObject`.

* Added `Store::BaseURL()` to retrieve the underlying URL of the store.

## Changed

* Improved 'Walk' speed on gstore by 25% by only fetching 'Name'

* Store `dstore.MockStore` now opened up public access to `Files` to easily get all written content.

* BREAKING: `MockStore`'s `SubStore` method has changed behavior. It now removes the prefix from files already present, to conform to the behavior of other stores. This might affect your use of `MockStore::SetFile()` in tests.

* The `Walk()` and `ListFiles()` methods does not have an `ignoreSuffix` parameter anymore. This is managed internally by the LocalStore which was the only one that needed it, when writing temporary files (and renaming afterwards). Simplifies it for everyone else.

* The `dstore.NewLocalStore` (local store implementation) sanitize the input if it does not start with `file://`.

* BREAKING: The `NewLocalStore` now takes a `*url.URL` object instead of a `string`. Just pass a `&url.URL{Scheme: "file", Path: originalString}` to fix your code, if you're using `NewLocalStore` directly and not the recommended `NewStore`.

### Deprecation

* **Deprecated** The method `dstore.NewStoreFromURL` is deprecated, use `dstore.NewStoreFromFileURL` which is clearer in semantics.
