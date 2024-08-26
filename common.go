package dstore

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
)

//
// Common Archive Store
//

type commonStore struct {
	extension       string
	compressionType string
	overwrite       bool

	compressedWriteCallback   func(ctx context.Context, size int)
	uncompressedWriteCallback func(ctx context.Context, size int)
	compressedReadCallback    func(ctx context.Context, size int)
	uncompressedReadCallback  func(ctx context.Context, size int)
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
	if startingPoint != "" && !strings.HasPrefix(startingPoint, prefix) {
		return fmt.Errorf("starting point %q must start with prefix %q", startingPoint, prefix)
	}

	var gatePassed bool
	return store.Walk(ctx, prefix, func(filename string) error {
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

func listFiles(ctx context.Context, store Store, prefix string, max int) (out []string, err error) {
	var count int
	err = store.Walk(ctx, prefix, func(filename string) error {
		count++
		if max >= 0 && count > max {
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

func (c *commonStore) compressedCopy(ctx context.Context, destination io.Writer, source io.Reader) error {
	// Wrap the writer with the uncompressed write callback if it exists
	if c.compressedWriteCallback != nil {
		destination = &callbackWriter{w: destination, callback: c.compressedWriteCallback, ctx: ctx}
	}

	var dest io.Writer
	switch c.compressionType {
	case "gzip":
		gw := gzip.NewWriter(destination)
		if c.uncompressedWriteCallback != nil {
			dest = &callbackWriter{w: gw, callback: c.uncompressedWriteCallback, ctx: ctx}
		} else {
			dest = gw
		}
		if _, err := io.Copy(dest, source); err != nil {
			return err
		}
		if err := gw.Close(); err != nil {
			return err
		}
	case "zstd":
		zstdEncoder, err := zstd.NewWriter(destination)
		if err != nil {
			return err
		}
		if c.uncompressedWriteCallback != nil {
			dest = &callbackWriter{w: zstdEncoder, callback: c.uncompressedWriteCallback, ctx: ctx}
		} else {
			dest = zstdEncoder
		}
		if _, err := io.Copy(dest, source); err != nil {
			return err
		}
		if err := zstdEncoder.Close(); err != nil {
			return err
		}
	default:
		if c.uncompressedWriteCallback != nil {
			dest = &callbackWriter{w: destination, callback: c.uncompressedWriteCallback, ctx: ctx}
		} else {
			dest = destination
		}
		if _, err := io.Copy(dest, source); err != nil {
			return err
		}
	}
	return nil
}

func (c *commonStore) uncompressedReader(ctx context.Context, reader io.ReadCloser) (out io.ReadCloser, err error) {
	if c.compressedReadCallback != nil {
		reader = &callbackReadCloser{rc: reader, callback: c.compressedReadCallback, ctx: ctx}
	}

	switch c.compressionType {
	case "gzip":
		gzipReader, err := NewGZipReadCloser(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %w", err)
		}

		if c.uncompressedReadCallback != nil {
			out = &callbackReadCloser{rc: gzipReader, callback: c.uncompressedReadCallback, ctx: ctx}
		} else {
			out = gzipReader
		}

	case "zstd":
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("unable to create zstd reader: %w", err)
		}

		if c.uncompressedReadCallback != nil {
			out = &callbackReadCloser{rc: zstdReader.IOReadCloser(), callback: c.uncompressedReadCallback, ctx: ctx}
		} else {
			out = zstdReader.IOReadCloser()
		}
	default:
		if c.uncompressedReadCallback != nil {
			out = &callbackReadCloser{rc: reader, callback: c.uncompressedReadCallback, ctx: ctx}
		} else {
			out = reader
		}
	}

	return out, nil
}

func wrapReadCloser(orig io.ReadCloser, f func()) io.ReadCloser {
	return &wrappedReadCloser{
		orig:      orig,
		closeHook: f,
	}
}

type wrappedReadCloser struct {
	orig      io.ReadCloser
	closeHook func()
}

func (wrc *wrappedReadCloser) Close() error {
	wrc.closeHook()
	return wrc.orig.Close()
}

func (wrc *wrappedReadCloser) Read(p []byte) (n int, err error) {
	return wrc.orig.Read(p)
}
