// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.

package builder

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	gokzg4844 "github.com/crate-crypto/go-eth-kzg"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
)

func TestBidBlockArgsToDecodedBidBlockNormalizesNilSidecars(t *testing.T) {
	args := &BidBlockArgs{
		BidBlock: &BidBlock{
			Header: &types.Header{
				Difficulty: big.NewInt(1),
				Number:     big.NewInt(1),
				Extra:      make([]byte, 32),
			},
		},
	}

	decoded, err := args.ToDecodedBidBlock(common.Address{0x1})
	if err != nil {
		t.Fatalf("ToDecodedBidBlock failed: %v", err)
	}
	if decoded.Sidecars == nil {
		t.Fatal("nil sidecars should be normalized to an empty slice")
	}
	if len(decoded.Sidecars) != 0 {
		t.Fatalf("sidecars length mismatch: got %d, want 0", len(decoded.Sidecars))
	}
}

func TestBidBlockArgsToDecodedBidBlockCopiesHeader(t *testing.T) {
	args := &BidBlockArgs{
		BidBlock: &BidBlock{
			Header: &types.Header{
				Difficulty: big.NewInt(1),
				Number:     big.NewInt(1),
				Extra:      []byte{1, 2, 3},
			},
		},
	}

	decoded, err := args.ToDecodedBidBlock(common.Address{0x1})
	if err != nil {
		t.Fatalf("ToDecodedBidBlock failed: %v", err)
	}
	if decoded.Header == args.BidBlock.Header {
		t.Fatal("decoded BidBlock header must not share the original header pointer")
	}

	decoded.Header.Number.SetUint64(2)
	decoded.Header.Extra[0] = 9

	if args.BidBlock.Header.Number.Uint64() != 1 {
		t.Fatalf("original header number mutated: got %d, want 1", args.BidBlock.Header.Number.Uint64())
	}
	if args.BidBlock.Header.Extra[0] != 1 {
		t.Fatalf("original header extra mutated: got %d, want 1", args.BidBlock.Header.Extra[0])
	}
}

var benchmarkBidBlockHash common.Hash

func BenchmarkBidBlockSigningHash(b *testing.B) {
	for _, tc := range []struct {
		name     string
		txs      int
		txSize   int
		sidecars int
	}{
		{name: "100tx_no_blob", txs: 100, txSize: 250},
		{name: "500tx_3blob", txs: 500, txSize: 250, sidecars: 3},
	} {
		bidBlock := makeBenchmarkBidBlock(tc.txs, tc.txSize, tc.sidecars)
		b.Run(tc.name+"/old_full_bidblock", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkBidBlockHash = rlpHash(bidBlock)
			}
		})
		b.Run(tc.name+"/new_header_only", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkBidBlockHash = rlpHash(bidBlock.Header)
			}
		})
	}
}

func makeBenchmarkBidBlock(txs, txSize, sidecars int) *BidBlock {
	transactions := make([]hexutil.Bytes, txs)
	for i := range transactions {
		tx := make([]byte, txSize)
		for j := range tx {
			tx[j] = byte(i + j)
		}
		transactions[i] = tx
	}

	return &BidBlock{
		Header: &types.Header{
			ParentHash:  common.HexToHash("0x01"),
			UncleHash:   types.EmptyUncleHash,
			Coinbase:    common.HexToAddress("0x02"),
			Root:        common.HexToHash("0x03"),
			TxHash:      common.HexToHash("0x04"),
			ReceiptHash: common.HexToHash("0x05"),
			Bloom:       types.Bloom{},
			Difficulty:  big.NewInt(1),
			Number:      big.NewInt(1),
			GasLimit:    30_000_000,
			GasUsed:     21_000,
			Time:        1,
			Extra:       make([]byte, 32),
			MixDigest:   common.HexToHash("0x06"),
		},
		Transactions: transactions,
		Sidecars:     makeBenchmarkSidecars(sidecars),
	}
}

func makeBenchmarkSidecars(count int) types.BlobSidecars {
	sidecars := make(types.BlobSidecars, count)
	for i := range sidecars {
		blob := randBenchmarkBlob()
		sidecars[i] = &types.BlobSidecar{
			BlobTxSidecar: types.BlobTxSidecar{
				Blobs:       []kzg4844.Blob{blob},
				Commitments: []kzg4844.Commitment{{byte(i)}},
				Proofs:      []kzg4844.Proof{{byte(i + 1)}},
			},
			BlockNumber: big.NewInt(1),
			BlockHash:   common.BytesToHash([]byte{byte(i + 1)}),
			TxIndex:     uint64(i),
			TxHash:      common.BytesToHash([]byte{byte(i + 100)}),
		}
	}
	return sidecars
}

func randBenchmarkFieldElement() [32]byte {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		panic("failed to get random field element")
	}
	var r fr.Element
	r.SetBytes(bytes)
	return gokzg4844.SerializeScalar(r)
}

func randBenchmarkBlob() kzg4844.Blob {
	var blob kzg4844.Blob
	for i := 0; i < len(blob); i += gokzg4844.SerializedScalarSize {
		fieldElementBytes := randBenchmarkFieldElement()
		copy(blob[i:i+gokzg4844.SerializedScalarSize], fieldElementBytes[:])
	}
	return blob
}
