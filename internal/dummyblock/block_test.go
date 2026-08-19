package dummyblock

import (
	"testing"

	pbacme "github.com/streamingfast/dummy-blockchain/pb/sf/acme/type/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProtoLooksLikeABlock(t *testing.T) {
	raw := Proto(64 << 10)
	require.Greater(t, len(raw), 1024)

	var blk pbacme.Block
	require.NoError(t, proto.Unmarshal(raw, &blk))
	require.NotNil(t, blk.Header)
	require.Equal(t, uint64(1), blk.Header.Height)
	require.NotEmpty(t, blk.Transactions)
	require.NotEmpty(t, blk.Transactions[0].Hash)
	require.NotEmpty(t, blk.Transactions[0].Data)
}

func TestProtoCached(t *testing.T) {
	a := Proto(8 << 10)
	b := Proto(8 << 10)
	require.Equal(t, &a[0], &b[0])
}
