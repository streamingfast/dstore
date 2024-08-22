package dstore

import (
	"context"
)

// Deprecated: This interface will no longer be used, use the Options to inject all metering logic from the upstream code instead
type Meter interface {
	AddBytesRead(int)
	AddBytesWritten(int)

	AddBytesWrittenCtx(context.Context, int)
	AddBytesReadCtx(context.Context, int)
}

// Deprecated: Use the Options to add callbacks to inject metering from the upstream code instead
func (c *commonStore) SetMeter(meter Meter) {
	// if any of the callbacks are defined, ignore this
	if c.compressedReadCallback != nil || c.uncompressedWriteCallback != nil || c.compressedWriteCallback != nil || c.uncompressedReadCallback != nil {
		zlog.Warn("Callbacks have already been defined, SetMeter will not override them")
		return
	}

	zlog.Warn("SetMeter is deprecated, use the dstore Options to add callbacks to inject metering from the upstream code instead")

	//imitate the old behavior:

	c.compressedReadCallback = func(ctx context.Context, n int) {
		meter.AddBytesRead(n)
	}

	c.uncompressedWriteCallback = func(ctx context.Context, n int) {
		meter.AddBytesWritten(n)
	}
}

// Deprecated: SetMeter on MockStore no longer does anything
func (_ *MockStore) SetMeter(_ Meter) {}
