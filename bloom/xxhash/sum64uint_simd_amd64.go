//go:build goexperiment.simd

package xxhash

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// This file provides implementations of the MultiSum64 functions based on
// the simd/archsimd package, replacing the hand-written assembly of
// sum64uint_amd64.s when GOEXPERIMENT=simd is set. Eight hashes are computed
// per vector (Uint64x8); the multiply-heavy dependency chains are hidden by
// processing four independent vector streams per loop iteration, and the
// prime and shift constants are individual local variables so they stay
// register resident (a pointer-accessed struct would reload them from the
// stack on every use).
//
// The kernels require the AVX-512 feature set: the 64 bits multiplies are
// VPMULLQ. Note that the assembly gate tested AVX512CD, which it never used;
// this gate is the accurate one.
//
// Shifts use per-lane variable forms with constant vectors rather than
// ShiftAllRight (whose scalar count materializes through a legacy MOVQ,
// golang/go#80835); rotates use the immediate form, which encodes the count
// in the instruction.

func MultiSum64Uint8(h []uint64, v []uint8) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 32 {
		p1 := archsimd.BroadcastUint64x8(prime1)
		p2 := archsimd.BroadcastUint64x8(prime2)
		p3 := archsimd.BroadcastUint64x8(prime3)
		s33 := archsimd.BroadcastUint64x8(33)
		s29 := archsimd.BroadcastUint64x8(29)
		s32 := archsimd.BroadcastUint64x8(32)
		p5 := archsimd.BroadcastUint64x8(prime5)
		seed := archsimd.BroadcastUint64x8(prime5 + 1)
		m := n / 32
		cv := unsafecast.Slice[[32]uint8](v)[:m]
		ch := unsafecast.Slice[[32]uint64](h)[:m]
		for j := range cv {
			x0 := archsimd.LoadUint8x16Slice(cv[j][0:16])
			x1 := archsimd.LoadUint8x16Slice(cv[j][16:32])
			v0 := x0.ExtendLo8ToUint64()
			v1 := x0.ConcatShiftBytesRight(8, x0).ExtendLo8ToUint64()
			v2 := x1.ExtendLo8ToUint64()
			v3 := x1.ConcatShiftBytesRight(8, x1).ExtendLo8ToUint64()
			h0 := seed.Xor(v0.Mul(p5)).RotateAllLeft(11).Mul(p1)
			h1 := seed.Xor(v1.Mul(p5)).RotateAllLeft(11).Mul(p1)
			h2 := seed.Xor(v2.Mul(p5)).RotateAllLeft(11).Mul(p1)
			h3 := seed.Xor(v3.Mul(p5)).RotateAllLeft(11).Mul(p1)
			h0 = h0.Xor(h0.ShiftRight(s33)).Mul(p2)
			h0 = h0.Xor(h0.ShiftRight(s29)).Mul(p3)
			h0 = h0.Xor(h0.ShiftRight(s32))
			h1 = h1.Xor(h1.ShiftRight(s33)).Mul(p2)
			h1 = h1.Xor(h1.ShiftRight(s29)).Mul(p3)
			h1 = h1.Xor(h1.ShiftRight(s32))
			h2 = h2.Xor(h2.ShiftRight(s33)).Mul(p2)
			h2 = h2.Xor(h2.ShiftRight(s29)).Mul(p3)
			h2 = h2.Xor(h2.ShiftRight(s32))
			h3 = h3.Xor(h3.ShiftRight(s33)).Mul(p2)
			h3 = h3.Xor(h3.ShiftRight(s29)).Mul(p3)
			h3 = h3.Xor(h3.ShiftRight(s32))
			h0.StoreSlice(ch[j][0:8])
			h1.StoreSlice(ch[j][8:16])
			h2.StoreSlice(ch[j][16:24])
			h3.StoreSlice(ch[j][24:32])
		}
		i = m * 32
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint8(v[i])
	}
	return n
}

func MultiSum64Uint16(h []uint64, v []uint16) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 32 {
		p1 := archsimd.BroadcastUint64x8(prime1)
		p2 := archsimd.BroadcastUint64x8(prime2)
		p3 := archsimd.BroadcastUint64x8(prime3)
		s33 := archsimd.BroadcastUint64x8(33)
		s29 := archsimd.BroadcastUint64x8(29)
		s32 := archsimd.BroadcastUint64x8(32)
		p5 := archsimd.BroadcastUint64x8(prime5)
		seed := archsimd.BroadcastUint64x8(prime5 + 2)
		lowByte := archsimd.BroadcastUint64x8(0xFF)
		s8 := archsimd.BroadcastUint64x8(8)
		m := n / 32
		cv := unsafecast.Slice[[32]uint16](v)[:m]
		ch := unsafecast.Slice[[32]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint16x8Slice(cv[j][0:8]).ExtendToUint64()
			v1 := archsimd.LoadUint16x8Slice(cv[j][8:16]).ExtendToUint64()
			v2 := archsimd.LoadUint16x8Slice(cv[j][16:24]).ExtendToUint64()
			v3 := archsimd.LoadUint16x8Slice(cv[j][24:32]).ExtendToUint64()
			h0 := seed.Xor(v0.And(lowByte).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h1 := seed.Xor(v1.And(lowByte).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h2 := seed.Xor(v2.And(lowByte).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h3 := seed.Xor(v3.And(lowByte).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h0 = h0.Xor(v0.ShiftRight(s8).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h1 = h1.Xor(v1.ShiftRight(s8).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h2 = h2.Xor(v2.ShiftRight(s8).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h3 = h3.Xor(v3.ShiftRight(s8).Mul(p5)).RotateAllLeft(11).Mul(p1)
			h0 = h0.Xor(h0.ShiftRight(s33)).Mul(p2)
			h0 = h0.Xor(h0.ShiftRight(s29)).Mul(p3)
			h0 = h0.Xor(h0.ShiftRight(s32))
			h1 = h1.Xor(h1.ShiftRight(s33)).Mul(p2)
			h1 = h1.Xor(h1.ShiftRight(s29)).Mul(p3)
			h1 = h1.Xor(h1.ShiftRight(s32))
			h2 = h2.Xor(h2.ShiftRight(s33)).Mul(p2)
			h2 = h2.Xor(h2.ShiftRight(s29)).Mul(p3)
			h2 = h2.Xor(h2.ShiftRight(s32))
			h3 = h3.Xor(h3.ShiftRight(s33)).Mul(p2)
			h3 = h3.Xor(h3.ShiftRight(s29)).Mul(p3)
			h3 = h3.Xor(h3.ShiftRight(s32))
			h0.StoreSlice(ch[j][0:8])
			h1.StoreSlice(ch[j][8:16])
			h2.StoreSlice(ch[j][16:24])
			h3.StoreSlice(ch[j][24:32])
		}
		i = m * 32
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint16(v[i])
	}
	return n
}

func MultiSum64Uint32(h []uint64, v []uint32) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 32 {
		p1 := archsimd.BroadcastUint64x8(prime1)
		p2 := archsimd.BroadcastUint64x8(prime2)
		p3 := archsimd.BroadcastUint64x8(prime3)
		s33 := archsimd.BroadcastUint64x8(33)
		s29 := archsimd.BroadcastUint64x8(29)
		s32 := archsimd.BroadcastUint64x8(32)
		seed := archsimd.BroadcastUint64x8(prime5 + 4)
		m := n / 32
		cv := unsafecast.Slice[[32]uint32](v)[:m]
		ch := unsafecast.Slice[[32]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint32x8Slice(cv[j][0:8]).ExtendToUint64()
			v1 := archsimd.LoadUint32x8Slice(cv[j][8:16]).ExtendToUint64()
			v2 := archsimd.LoadUint32x8Slice(cv[j][16:24]).ExtendToUint64()
			v3 := archsimd.LoadUint32x8Slice(cv[j][24:32]).ExtendToUint64()
			h0 := seed.Xor(v0.Mul(p1)).RotateAllLeft(23).Mul(p2).Add(p3)
			h1 := seed.Xor(v1.Mul(p1)).RotateAllLeft(23).Mul(p2).Add(p3)
			h2 := seed.Xor(v2.Mul(p1)).RotateAllLeft(23).Mul(p2).Add(p3)
			h3 := seed.Xor(v3.Mul(p1)).RotateAllLeft(23).Mul(p2).Add(p3)
			h0 = h0.Xor(h0.ShiftRight(s33)).Mul(p2)
			h0 = h0.Xor(h0.ShiftRight(s29)).Mul(p3)
			h0 = h0.Xor(h0.ShiftRight(s32))
			h1 = h1.Xor(h1.ShiftRight(s33)).Mul(p2)
			h1 = h1.Xor(h1.ShiftRight(s29)).Mul(p3)
			h1 = h1.Xor(h1.ShiftRight(s32))
			h2 = h2.Xor(h2.ShiftRight(s33)).Mul(p2)
			h2 = h2.Xor(h2.ShiftRight(s29)).Mul(p3)
			h2 = h2.Xor(h2.ShiftRight(s32))
			h3 = h3.Xor(h3.ShiftRight(s33)).Mul(p2)
			h3 = h3.Xor(h3.ShiftRight(s29)).Mul(p3)
			h3 = h3.Xor(h3.ShiftRight(s32))
			h0.StoreSlice(ch[j][0:8])
			h1.StoreSlice(ch[j][8:16])
			h2.StoreSlice(ch[j][16:24])
			h3.StoreSlice(ch[j][24:32])
		}
		i = m * 32
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint32(v[i])
	}
	return n
}

func MultiSum64Uint64(h []uint64, v []uint64) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 32 {
		p1 := archsimd.BroadcastUint64x8(prime1)
		p2 := archsimd.BroadcastUint64x8(prime2)
		p3 := archsimd.BroadcastUint64x8(prime3)
		s33 := archsimd.BroadcastUint64x8(33)
		s29 := archsimd.BroadcastUint64x8(29)
		s32 := archsimd.BroadcastUint64x8(32)
		p4 := archsimd.BroadcastUint64x8(prime4)
		seed := archsimd.BroadcastUint64x8(prime5 + 8)
		m := n / 32
		cv := unsafecast.Slice[[32]uint64](v)[:m]
		ch := unsafecast.Slice[[32]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint64x8Slice(cv[j][0:8])
			v1 := archsimd.LoadUint64x8Slice(cv[j][8:16])
			v2 := archsimd.LoadUint64x8Slice(cv[j][16:24])
			v3 := archsimd.LoadUint64x8Slice(cv[j][24:32])
			h0 := seed.Xor(v0.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h1 := seed.Xor(v1.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h2 := seed.Xor(v2.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h3 := seed.Xor(v3.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h0 = h0.Xor(h0.ShiftRight(s33)).Mul(p2)
			h0 = h0.Xor(h0.ShiftRight(s29)).Mul(p3)
			h0 = h0.Xor(h0.ShiftRight(s32))
			h1 = h1.Xor(h1.ShiftRight(s33)).Mul(p2)
			h1 = h1.Xor(h1.ShiftRight(s29)).Mul(p3)
			h1 = h1.Xor(h1.ShiftRight(s32))
			h2 = h2.Xor(h2.ShiftRight(s33)).Mul(p2)
			h2 = h2.Xor(h2.ShiftRight(s29)).Mul(p3)
			h2 = h2.Xor(h2.ShiftRight(s32))
			h3 = h3.Xor(h3.ShiftRight(s33)).Mul(p2)
			h3 = h3.Xor(h3.ShiftRight(s29)).Mul(p3)
			h3 = h3.Xor(h3.ShiftRight(s32))
			h0.StoreSlice(ch[j][0:8])
			h1.StoreSlice(ch[j][8:16])
			h2.StoreSlice(ch[j][16:24])
			h3.StoreSlice(ch[j][24:32])
		}
		i = m * 32
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint64(v[i])
	}
	return n
}

// Lane indexes deinterleaving the low and high halves of 8 consecutive
// 128 bits values loaded as two vectors of 8 uint64.
var (
	evenLanes = [8]uint64{0, 2, 4, 6, 8, 10, 12, 14}
	oddLanes  = [8]uint64{1, 3, 5, 7, 9, 11, 13, 15}
)

func MultiSum64Uint128(h []uint64, v [][16]byte) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 16 {
		p1 := archsimd.BroadcastUint64x8(prime1)
		p2 := archsimd.BroadcastUint64x8(prime2)
		p3 := archsimd.BroadcastUint64x8(prime3)
		s33 := archsimd.BroadcastUint64x8(33)
		s29 := archsimd.BroadcastUint64x8(29)
		s32 := archsimd.BroadcastUint64x8(32)
		p4 := archsimd.BroadcastUint64x8(prime4)
		seed := archsimd.BroadcastUint64x8(prime5 + 16)
		even := archsimd.LoadUint64x8Slice(evenLanes[:])
		odd := archsimd.LoadUint64x8Slice(oddLanes[:])
		m := n / 16
		cv := unsafecast.Slice[[32]uint64](v)[:m]
		ch := unsafecast.Slice[[16]uint64](h)[:m]
		for j := range cv {
			z0 := archsimd.LoadUint64x8Slice(cv[j][0:8])
			z1 := archsimd.LoadUint64x8Slice(cv[j][8:16])
			z2 := archsimd.LoadUint64x8Slice(cv[j][16:24])
			z3 := archsimd.LoadUint64x8Slice(cv[j][24:32])
			lo0 := z0.ConcatPermute(z1, even)
			hi0 := z0.ConcatPermute(z1, odd)
			lo1 := z2.ConcatPermute(z3, even)
			hi1 := z2.ConcatPermute(z3, odd)
			h0 := seed.Xor(lo0.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h1 := seed.Xor(lo1.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h0 = h0.Xor(hi0.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h1 = h1.Xor(hi1.Mul(p2).RotateAllLeft(31).Mul(p1)).RotateAllLeft(27).Mul(p1).Add(p4)
			h0 = h0.Xor(h0.ShiftRight(s33)).Mul(p2)
			h0 = h0.Xor(h0.ShiftRight(s29)).Mul(p3)
			h0 = h0.Xor(h0.ShiftRight(s32))
			h1 = h1.Xor(h1.ShiftRight(s33)).Mul(p2)
			h1 = h1.Xor(h1.ShiftRight(s29)).Mul(p3)
			h1 = h1.Xor(h1.ShiftRight(s32))
			h0.StoreSlice(ch[j][0:8])
			h1.StoreSlice(ch[j][8:16])
		}
		i = m * 16
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint128(v[i])
	}
	return n
}
