# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See [MAINTAINERS.md](./MAINTAINERS.md) for instructions to keep up to date.

# Unreleased

## Fixed

* Fixed `WalkFrom` on `S3` and `GCS` when both `Prefix` and `StartingPoint` was provided.

## Added

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
