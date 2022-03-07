package dstore

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

//
// Common Archive Store
//

type commonStore struct {
	extension       string
	compressionType string
	overwrite       bool
}

func (c *commonStore) Overwrite() bool      { return c.overwrite }
func (c *commonStore) SetOverwrite(in bool) { c.overwrite = in }

func (c *commonStore) pathWithExt(base string) string {
	if c.extension != "" {
		return base + "." + c.extension
	}
	return base
}

func commonWalkFrom(store Store, ctx context.Context, prefix, startingPoint string, f func(filename string) (err error)) error {
	var gatePassed bool
	return store.Walk(ctx, prefix, "", func(filename string) error {
		if gatePassed {
			return f(filename)
		}
		if filename >= startingPoint {
			gatePassed = true
			return f(filename)
		}
		return nil
	})
}

func pushLocalFile(ctx context.Context, store Store, localFile, toBaseName string) (removeFunc func() error, err error) {
	f, err := os.Open(localFile)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	objPath := store.ObjectPath(toBaseName)

	err = store.WriteObject(ctx, toBaseName, f)
	if err != nil {
		return nil, fmt.Errorf("writing %q to storage %q: %w", localFile, objPath, err)
	}

	return func() error {
		return os.Remove(localFile)
	}, nil
}

func listFiles(ctx context.Context, store Store, prefix, ignoreSuffix string, max int) (out []string, err error) {
	var count int
	err = store.Walk(ctx, prefix, ignoreSuffix, func(filename string) error {
		count++
		if count > max {
			return StopIteration
		}

		out = append(out, filename)

		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}

func (c *commonStore) compressedCopy(f io.Reader, w io.Writer) error {
	switch c.compressionType {
	case "gzip":
		gw := gzip.NewWriter(w)
		if _, err := io.Copy(gw, f); err != nil {
			return err
		}
		if err := gw.Close(); err != nil {
			return err
		}
	case "zstd":
		zstdEncoder, err := zstd.NewWriter(w)
		if err != nil {
			return err
		}
		if _, err := io.Copy(zstdEncoder, f); err != nil {
			return err
		}
		if err := zstdEncoder.Close(); err != nil {
			return err
		}
	default:
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
	}
	return nil
}

func (c *commonStore) uncompressedReader(reader io.ReadCloser) (out io.ReadCloser, err error) {
	switch c.compressionType {
	case "gzip":
		gzipReader, err := NewGZipReadCloser(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %w", err)
		}

		return gzipReader, nil
	case "zstd":
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create zstd reader: %w", err)
		}

		return zstdReader.IOReadCloser(), nil
	default:
		return reader, nil
	}
}
