package dstore

import (
	"context"

	"github.com/streamingfast/logging"

	"go.uber.org/zap"
)

type fileKey string
type storeKey string

func withLogger(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) context.Context {
	ctx = context.WithValue(ctx, "logger", logger)
	ctx = context.WithValue(ctx, "tracer", tracer)
	return ctx
}

func withStoreType(ctx context.Context, storeType string) context.Context {
	return context.WithValue(ctx, storeKey("store"), storeType)
}

func StoreTypeFromContext(ctx context.Context) string {
	if v := ctx.Value(storeKey("store")); v != nil {
		return v.(string)
	}
	return ""
}

func withFileName(ctx context.Context, filename string) context.Context {
	return context.WithValue(ctx, fileKey("file"), filename)
}

func FileNameFromContext(ctx context.Context) string {
	if v := ctx.Value(fileKey("file")); v != nil {
		return v.(string)
	}
	return ""
}
