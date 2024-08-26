package dstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleCompressedWriteCallback(t *testing.T) {
	compressedWriteBytes := 0

	c := commonStore{
		compressionType: "gzip",
		compressedWriteCallback: func(ctx context.Context, n int) {
			compressedWriteBytes += n
		},
	}

	fullsize := 1024

	f := io.Reader(bytes.NewBuffer(bytes.Repeat([]byte("1"), fullsize)))
	w := bytes.NewBuffer(nil)

	err := c.compressedCopy(context.Background(), w, f)
	require.NoError(t, err)

	assert.Greater(t, compressedWriteBytes, 0)
	assert.Less(t, compressedWriteBytes, fullsize)
}

func TestSimpleUncompressedWriteCallback(t *testing.T) {
	uncompressedWriteBytes := 0

	c := commonStore{
		compressionType: "gzip",
		uncompressedWriteCallback: func(ctx context.Context, n int) {
			uncompressedWriteBytes += n
		},
	}

	fullsize := 1024

	f := io.Reader(bytes.NewBuffer(bytes.Repeat([]byte("1"), fullsize)))
	w := bytes.NewBuffer(nil)

	err := c.compressedCopy(context.Background(), w, f)
	require.NoError(t, err)

	assert.Greater(t, uncompressedWriteBytes, 0)
	assert.Equal(t, uncompressedWriteBytes, fullsize)
}

func TestSimpleCompressedReadCallback(t *testing.T) {
	compressedReadBytes := 0

	c := commonStore{
		compressionType: "gzip",
		compressedReadCallback: func(ctx context.Context, n int) {
			compressedReadBytes += n
		},
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(bytes.Repeat([]byte("1"), 1024))
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)

	f := io.NopCloser(&buf)

	r, err := c.uncompressedReader(context.Background(), f)
	require.NoError(t, err)

	_, err = io.ReadAll(r)
	require.NoError(t, err)

	assert.Greater(t, compressedReadBytes, 0)
	assert.Less(t, compressedReadBytes, 1024)
}

func TestSimpleUncompressedReadCallback(t *testing.T) {
	uncompressedReadBytes := 0

	c := commonStore{
		compressionType: "gzip",
		uncompressedReadCallback: func(ctx context.Context, n int) {
			uncompressedReadBytes += n
		},
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(bytes.Repeat([]byte("1"), 1024))
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)

	f := io.NopCloser(&buf)

	r, err := c.uncompressedReader(context.Background(), f)
	require.NoError(t, err)

	_, err = io.ReadAll(r)
	require.NoError(t, err)

	assert.Greater(t, uncompressedReadBytes, 0)
	assert.Equal(t, uncompressedReadBytes, 1024)
}

func TestCompressedCopyGzip(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	readBytes := 0

	c := commonStore{
		compressionType: "gzip",
		uncompressedWriteCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedWriteCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	f := io.Reader(bytes.NewBuffer(bytes.Repeat([]byte("1"), 1024)))
	f = &callbackReader{
		r:   f,
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			readBytes += n
		},
	}
	w := bytes.NewBuffer(nil)

	err := c.compressedCopy(context.Background(), w, f)
	require.NoError(t, err)

	assert.Greater(t, readBytes, 0)
	assert.Equal(t, readBytes, uncompressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Greater(t, uncompressedN, compressedN)
}

func TestCompressedCopyZstd(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	readBytes := 0

	c := commonStore{
		compressionType: "zstd",
		uncompressedWriteCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedWriteCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	f := io.Reader(bytes.NewBuffer(bytes.Repeat([]byte("1"), 1024)))
	f = &callbackReader{
		r:   f,
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			readBytes += n
		},
	}
	w := bytes.NewBuffer(nil)

	err := c.compressedCopy(context.Background(), w, f)
	require.NoError(t, err)

	assert.Greater(t, readBytes, 0)
	assert.Equal(t, readBytes, uncompressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Greater(t, uncompressedN, compressedN)
}

func TestCompressedCopyPlain(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	readBytes := 0

	c := commonStore{
		uncompressedWriteCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedWriteCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	f := io.Reader(bytes.NewBuffer(bytes.Repeat([]byte("1"), 1024)))
	f = &callbackReader{
		r:   f,
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			readBytes += n
		},
	}
	w := bytes.NewBuffer(nil)

	err := c.compressedCopy(context.Background(), w, f)
	require.NoError(t, err)

	assert.Greater(t, readBytes, 0)
	assert.Equal(t, readBytes, uncompressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Equal(t, uncompressedN, compressedN)
}

func TestUncompressedReaderGzip(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	readBytes := 0

	c := commonStore{
		compressionType: "gzip",
		uncompressedReadCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedReadCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(bytes.Repeat([]byte("1"), 1024))
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)

	f := &callbackReadCloser{
		rc:  io.NopCloser(&buf),
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			readBytes += n
		},
	}

	ur, err := c.uncompressedReader(context.Background(), f)
	require.NoError(t, err)

	_, err = io.ReadAll(ur)
	require.NoError(t, err)

	require.Greater(t, readBytes, 0)
	assert.Equal(t, readBytes, compressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Greater(t, uncompressedN, compressedN)
}

func TestUncompressedReaderZstd(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	readBytes := 0

	c := commonStore{
		compressionType: "zstd",
		uncompressedReadCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedReadCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	var buf bytes.Buffer
	zw, _ := zstd.NewWriter(&buf)
	_, err := zw.Write(bytes.Repeat([]byte("1"), 1024))
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)

	f := &callbackReadCloser{
		rc:  io.NopCloser(&buf),
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			readBytes += n
		},
	}

	ur, err := c.uncompressedReader(context.Background(), f)
	require.NoError(t, err)

	_, err = io.ReadAll(ur)
	require.NoError(t, err)

	require.Greater(t, readBytes, 0)
	assert.Equal(t, readBytes, compressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Greater(t, uncompressedN, compressedN)
}

func TestUncompressedReaderPlain(t *testing.T) {
	uncompressedN := 0
	compressedN := 0

	writtenBytes := 0

	c := commonStore{
		uncompressedReadCallback: func(ctx context.Context, n int) {
			uncompressedN += n
		},
		compressedReadCallback: func(ctx context.Context, n int) {
			compressedN += n
		},
	}

	f := &callbackReadCloser{
		rc:  io.NopCloser(bytes.NewBuffer(bytes.Repeat([]byte("1"), 1024))),
		ctx: context.Background(),
		callback: func(ctx context.Context, n int) {
			writtenBytes += n
		},
	}

	ur, err := c.uncompressedReader(context.Background(), f)
	require.NoError(t, err)

	_, err = io.ReadAll(ur)
	require.NoError(t, err)

	assert.Greater(t, writtenBytes, 0)
	assert.Equal(t, writtenBytes, uncompressedN)

	assert.Greater(t, uncompressedN, 0)
	assert.Greater(t, compressedN, 0)
	assert.Equal(t, uncompressedN, compressedN)
}
