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
essentially, requires some extra environment variables to run. They are skip if the correct
environment variables for the provider is not set.

To run the full test suite, you will need to peform the following steps.

First, you will need to have locally a few dependencies:
- [minio](https://github.com/minio/minio)

Then, start `minio` server:

```bash
mkdir -p /tmp/minio-tests/store-tests
cd /tmp/minio-tests
minio server .
```

Ensure you have access to GCP Storage Bucket, S3 Bucket, then run the full test suite:

```bash
STORETESTS_GS_STORE_URL="gs://streamingfast-developement-random/store-tests"\
STORETESTS_S3_STORE_URL="s3://streamingfast-customer-outbox/store-tests?region=us-east-2"\
STORETESTS_S3_MINIO_STORE_URL="s3://localhost:9000/store-tests?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin"
STORETESTS_S3_MINIO_STORE_EMPTY_BUCKET_URL="s3://localhost:9000/store-tests?region=none&insecure=true&access_key_id=minioadmin&secret_access_key=minioadmin" # this bucket MUST be empty for the test to run
go test ./...
```
## Contributing

**Issues and PR in this repo related strictly to the dstore library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

## License

[Apache 2.0](LICENSE)

