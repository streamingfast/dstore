package dstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithUncompressedReadCallback(t *testing.T) {
	conf := &config{}

	opt := WithUncompressedReadCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	assert.NotNil(t, conf.uncompressedReadCallback)
	assert.Nil(t, conf.compressedReadCallback)
	assert.Nil(t, conf.compressedWriteCallback)
	assert.Nil(t, conf.uncompressedWriteCallback)
}

func TestWithCompressedReadCallback(t *testing.T) {
	conf := &config{}

	opt := WithCompressedReadCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	assert.Nil(t, conf.uncompressedReadCallback)
	assert.NotNil(t, conf.compressedReadCallback)
	assert.Nil(t, conf.compressedWriteCallback)
	assert.Nil(t, conf.uncompressedWriteCallback)
}

func TestWithCompressedWriteCallback(t *testing.T) {
	conf := &config{}

	opt := WithCompressedWriteCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	assert.Nil(t, conf.uncompressedReadCallback)
	assert.Nil(t, conf.compressedReadCallback)
	assert.NotNil(t, conf.compressedWriteCallback)
	assert.Nil(t, conf.uncompressedWriteCallback)
}

func TestWithUncompressedWriteCallback(t *testing.T) {
	conf := &config{}

	opt := WithUncompressedWriteCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	assert.Nil(t, conf.uncompressedReadCallback)
	assert.Nil(t, conf.compressedReadCallback)
	assert.Nil(t, conf.compressedWriteCallback)
	assert.NotNil(t, conf.uncompressedWriteCallback)
}

func TestWithAllCallbacks(t *testing.T) {
	conf := &config{}

	opt := WithCompressedWriteCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	opt = WithCompressedReadCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	opt = WithUncompressedWriteCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	opt = WithUncompressedReadCallback(func(ctx context.Context, n int) {})
	opt.apply(conf)

	assert.NotNil(t, conf.uncompressedReadCallback)
	assert.NotNil(t, conf.compressedReadCallback)
	assert.NotNil(t, conf.compressedWriteCallback)
	assert.NotNil(t, conf.uncompressedWriteCallback)
}
