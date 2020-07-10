# dfuse Storage Abstraction
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/dstore)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

`dstore` is a simple abstraction on top of Local storage and Cloud
storage. It handles commonly used functions to store things (locally,
or on cloud storage providers), list files, delete, etc..

It is used by **[dfuse](https://github.com/dfuse-io/dfuse)**.

## Features

It currently supports:
* AWS S3 (`s3://[bucket]/path?region=us-east-1`, with [AWS-specific env vars](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html))
    * Minio (through the S3 interface)
* Google Storage (`gs://[bucket]/path`, with `GOOGLE_APPLICATION_CREDENTIALS` env var set)
* Azure Blob Storage (`az://[account].[container]/path`, with `AZURE_STORAGE_KEY` env var set)
* Local file systems (including virtual of fused-based) (`file:///` prefix)


## Contributing

**Issues and PR in this repo related strictly to the dstore library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.


## License

[Apache 2.0](LICENSE)
