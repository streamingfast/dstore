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

func pushLocalFile(ctx context.Context, store Store, localFile, toBaseName string) (err error) {
	f, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("opening local file %q: %s", localFile, err)
	}
	defer f.Close()

	objPath := store.ObjectPath(toBaseName)

	// The file doesn't exist, let's continue.
	err = store.WriteObject(ctx, toBaseName, f)
	if err != nil {
		return fmt.Errorf("writing %q to storage %q: %s", localFile, objPath, err)
	}

	if err = os.Remove(localFile); err != nil {
		return fmt.Errorf("error removing local file %q: %s", localFile, err)
	}

	return nil
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
			return nil, fmt.Errorf("unable to create gzip reader: %s", err)
		}

		return gzipReader, nil
	case "zstd":
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create zstd reader: %s", err)
		}

		return zstdReader.IOReadCloser(), nil
	default:
		return reader, nil
	}
}
