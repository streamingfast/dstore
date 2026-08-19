// Package dummyblock builds a marshaled dummy-blockchain protobuf block of
// approximately the requested size, using the same transaction-fill algorithm
// as github.com/streamingfast/dummy-blockchain/core.Engine.addTransactions.
package dummyblock

import (
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	pbacme "github.com/streamingfast/dummy-blockchain/pb/sf/acme/type/v1"
	"github.com/streamingfast/dummy-blockchain/types"
	"google.golang.org/protobuf/proto"
)

var cache sync.Map // sizeBytes -> []byte

// Proto returns a protobuf-encoded sf.acme.type.v1.Block of about sizeBytes.
// The result is cached per size so benches can reuse it.
func Proto(sizeBytes int) []byte {
	if v, ok := cache.Load(sizeBytes); ok {
		return v.([]byte)
	}
	payload := mustMarshal(toProto(newSizedBlock(sizeBytes)))
	actual, _ := cache.LoadOrStore(sizeBytes, payload)
	return actual.([]byte)
}

func newSizedBlock(sizeBytes int) *types.Block {
	const height = uint64(1)
	prevNum := uint64(0)
	prevHash := types.MakeHash(prevNum)
	blk := &types.Block{
		Header: &types.BlockHeader{
			Height:    height,
			Hash:      types.MakeHash(height),
			PrevNum:   &prevNum,
			PrevHash:  &prevHash,
			FinalNum:  prevNum,
			FinalHash: prevHash,
			Timestamp: time.Unix(1_700_000_000, 0).UTC(),
		},
	}
	addTransactions(blk, sizeBytes)
	return blk
}

var simulateTypes = []string{"transfer", "delegate", "undelegate", "reward", "slash"}
var bigZero = big.NewInt(0)

const kiB = 1024

func addTransactions(block *types.Block, sizeInBytes int) {
	addTx := func(data []byte) {
		i := len(block.Transactions)
		txHash := types.MakeFakeHash(block.Header.Height, i)
		sender := "0x" + txHash[:40]
		receiver := "0x" + txHash[24:64]
		amount := new(big.Int).SetUint64((block.Header.Height << 32) | uint64(i))
		success := true
		if i%7 == 0 {
			sender = "0xDEADBEEF"
		}
		if i%11 == 0 {
			receiver = "0xBAAAAAAD"
		}
		if i%3 == 0 {
			amount = bigZero
		}
		if i%13 == 0 {
			success = false
		}
		block.Transactions = append(block.Transactions, types.Transaction{
			Type:     simulateTypes[i%len(simulateTypes)],
			Hash:     txHash,
			Sender:   sender,
			Receiver: receiver,
			Data:     fillData(data, block.Header.Height, i),
			Amount:   amount,
			Fee:      new(big.Int).SetUint64(block.Header.Height + uint64(i)),
			Success:  success,
			Events:   generateEvents(block.Header.Height),
		})
	}

	if sizeInBytes < 10*kiB {
		for size := block.ApproximatedSize(); size <= sizeInBytes; size = block.ApproximatedSize() {
			addTx(make([]byte, 32))
		}
		return
	}

	txCount := targetTxCount(sizeInBytes)
	dataSizePerTx := (sizeInBytes / txCount) - 250
	if dataSizePerTx < 0 {
		dataSizePerTx = 0
	}
	for range txCount {
		addTx(make([]byte, dataSizePerTx))
	}
}

func targetTxCount(blockSizeInBytes int) int {
	scale := math.Log10(float64(blockSizeInBytes))
	exponent := scale - 3
	if exponent < 1 {
		exponent = 1
	}
	return int(math.Pow(10, exponent))
}

func fillData(buf []byte, blockHeight uint64, txIndex int) []byte {
	for i := range buf {
		buf[i] = byte((blockHeight + uint64(txIndex) + uint64(i)) % 256)
	}
	return buf
}

func generateEvents(height uint64) []types.Event {
	events := []types.Event{{
		Type: "token_transfer",
		Attributes: []types.Attribute{
			{Key: "foo", Value: "bar"},
		},
	}}
	switch {
	case height%2 == 0:
		events = append(events, types.Event{
			Type: "coin_spent",
			Attributes: []types.Attribute{
				{Key: "spender", Value: "fizz"},
				{Key: "amount", Value: "buzz"},
			},
		})
	case height%3 == 0:
		events = append(events, types.Event{
			Type: "delegate",
			Attributes: []types.Attribute{
				{Key: "delegator", Value: "addr1"},
				{Key: "validator", Value: "addr2"},
				{Key: "amount", Value: "123456789"},
			},
		})
	case height%5 == 0:
		events = append(events, types.Event{
			Type: "undelegate",
			Attributes: []types.Attribute{
				{Key: "delegator", Value: "addr1"},
				{Key: "amount", Value: "123456789"},
			},
		})
	}
	return events
}

func toProto(blk *types.Block) *pbacme.Block {
	out := &pbacme.Block{
		Header: &pbacme.BlockHeader{
			Height:    blk.Header.Height,
			Hash:      blk.Header.Hash,
			FinalNum:  blk.Header.FinalNum,
			FinalHash: blk.Header.FinalHash,
			Timestamp: blk.Header.Timestamp.UnixNano(),
		},
	}
	if blk.Header.PrevNum != nil && blk.Header.PrevHash != nil {
		out.Header.PreviousNum = blk.Header.PrevNum
		out.Header.PreviousHash = blk.Header.PrevHash
	}
	for i := range blk.Transactions {
		out.Transactions = append(out.Transactions, txToProto(&blk.Transactions[i]))
	}
	return out
}

func txToProto(tx *types.Transaction) *pbacme.Transaction {
	pb := &pbacme.Transaction{
		Type:     tx.Type,
		Hash:     tx.Hash,
		Sender:   tx.Sender,
		Receiver: tx.Receiver,
		Data:     tx.Data,
		Success:  tx.Success,
	}
	if tx.Amount != nil {
		pb.Amount = &pbacme.BigInt{Bytes: tx.Amount.Bytes()}
	}
	if tx.Fee != nil {
		pb.Fee = &pbacme.BigInt{Bytes: tx.Fee.Bytes()}
	}
	for _, ev := range tx.Events {
		pbev := &pbacme.Event{Type: ev.Type}
		for _, attr := range ev.Attributes {
			pbev.Attributes = append(pbev.Attributes, &pbacme.Attribute{Key: attr.Key, Value: attr.Value})
		}
		pb.Events = append(pb.Events, pbev)
	}
	return pb
}

func mustMarshal(blk *pbacme.Block) []byte {
	payload, err := proto.Marshal(blk)
	if err != nil {
		panic(fmt.Sprintf("dummyblock: marshal: %v", err))
	}
	return payload
}
