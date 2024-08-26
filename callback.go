package dstore

import (
	"context"
	"io"
)

type callbackWriter struct {
	w   io.Writer
	ctx context.Context

	callback func(ctx context.Context, n int)
}

func (cbw *callbackWriter) Write(p []byte) (n int, err error) {
	n, err = cbw.w.Write(p)

	if cbw.callback != nil {
		cbw.callback(cbw.ctx, n)
	}

	return
}

type callbackReadCloser struct {
	rc  io.ReadCloser
	ctx context.Context

	callback func(ctx context.Context, n int)
}

func (cbr *callbackReadCloser) Read(p []byte) (n int, err error) {
	n, err = cbr.rc.Read(p)

	if cbr.callback != nil {
		cbr.callback(cbr.ctx, n)
	}
	return
}

func (cbr *callbackReadCloser) Close() error {
	return cbr.rc.Close()
}

type callbackReader struct {
	r   io.Reader
	ctx context.Context

	callback func(ctx context.Context, n int)
}

func (cbr *callbackReader) Read(p []byte) (n int, err error) {
	n, err = cbr.r.Read(p)

	if cbr.callback != nil {
		cbr.callback(cbr.ctx, n)
	}
	return
}
