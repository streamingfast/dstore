package dstore

import (
	"context"
)

// Deprecated: This interface will no longer be used, use the Options to inject all metering logic from the upstream code instead
type Meter interface {
	AddBytesRead(int)
	AddBytesWritten(int)
}

// Deprecated: Use the Options to add callbacks to inject metering from the upstream code instead
func (c *commonStore) SetMeter(meter Meter) {
	//imitate the old behavior:

	rcb := func(ctx context.Context, n int) {}
	if c.compressedReadCallback != nil {
		rcb = c.compressedReadCallback
	}
	c.compressedReadCallback = func(ctx context.Context, n int) {
		meter.AddBytesRead(n)
		rcb(ctx, n)
	}

	wcb := func(ctx context.Context, n int) {}
	if c.uncompressedWriteCallback != nil {
		wcb = c.uncompressedWriteCallback
	}
	c.uncompressedWriteCallback = func(ctx context.Context, n int) {
		meter.AddBytesWritten(n)
		wcb(ctx, n)
	}
}

// Deprecated: SetMeter on MockStore no longer does anything
func (_ *MockStore) SetMeter(_ Meter) {}
