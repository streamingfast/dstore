package dstore

import (
	"context"
	"io"
)

type Meter interface {
	AddBytesRead(int)
	AddBytesWritten(int)

	AddBytesWrittenCtx(context.Context, int)
	AddBytesReadCtx(context.Context, int)
}

type meteredWriter struct {
	w   io.Writer
	m   Meter
	ctx context.Context
}

func (mw *meteredWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if mw.m == nil {
		return
	}

	mw.m.AddBytesWrittenCtx(mw.ctx, n)
	return
}

type meteredReadCloser struct {
	rc  io.ReadCloser
	m   Meter
	ctx context.Context
}

func (mr *meteredReadCloser) Read(p []byte) (n int, err error) {
	n, err = mr.rc.Read(p)
	if mr.m == nil {
		return
	}

	mr.m.AddBytesReadCtx(mr.ctx, n)
	return
}

func (mr *meteredReadCloser) Close() error {
	return mr.rc.Close()
}
