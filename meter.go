package dstore

import "io"

type Meter interface {
	AddBytesRead(int)
	AddBytesWritten(int)
}

type meteredWriter struct {
	w io.Writer
	m Meter
}

func (mw *meteredWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if mw.m == nil {
		return
	}

	mw.m.AddBytesWritten(n)
	return
}

type meteredReadCloser struct {
	rc io.ReadCloser
	m  Meter
}

func (mr *meteredReadCloser) Read(p []byte) (n int, err error) {
	n, err = mr.rc.Read(p)
	if mr.m == nil {
		return
	}

	mr.m.AddBytesRead(n)
	return
}

func (mr *meteredReadCloser) Close() error {
	return mr.rc.Close()
}
