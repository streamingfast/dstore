package dstore

import (
	"context"

	"github.com/streamingfast/logging"

	"go.uber.org/zap"
)

func withLogger(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) context.Context {
	ctx = context.WithValue(ctx, "logger", logger)
	ctx = context.WithValue(ctx, "tracer", tracer)
	return ctx
}

func withStore(ctx context.Context, storeType string) context.Context {
	return context.WithValue(ctx, "store", storeType)
}

func withFile(ctx context.Context, filename string) context.Context {
	return context.WithValue(ctx, "file", filename)
}
