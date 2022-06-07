# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See [MAINTAINERS.md](./MAINTAINERS.md) for instructions to keep up to date.

# Unreleased

## Added

* Added `dstore.OpenObject` that is able to open a single store element without having to create a separate store, this is a shortcut for splitting the path & filename, creating a new store from the path and then calling `store.OpenObject`.
* Added `Store::BaseURL()` to retrieve the underlying URL of the store.

## Changed

* The `Walk()` and `ListFiles()` methods does not have an `ignoreSuffix` parameter anymore. This is managed internally by the LocalStore which was the only one that needed it, when writing temporary files (and renaming afterwards). Simplifies it for everyone else.
* The `dstore.NewLocalStore` (local store implementation) sanitize the input if it does not start with `file://`.
* BREAKING: The `NewLocalStore` now takes a `*url.URL` object instead of a `string`. Just pass a `&url.URL{Scheme: "file", Path: originalString}` to fix your code, if you're using `NewLocalStore` directly and not the recommended `NewStore`.
