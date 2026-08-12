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
// processing two independent vector streams per loop iteration.
//
// The kernels require the AVX-512 feature set: the 64 bits multiplies are
// VPMULLQ. Note that the assembly gate tested AVX512CD, which it never used;
// this gate is the accurate one.
//
// Shifts use per-lane variable forms with constant vectors rather than
// ShiftAllRight (whose scalar count materializes through a legacy MOVQ,
// golang/go#80835); rotates use the immediate form, which encodes the count
// in the instruction.

type sum64Consts struct {
	p1, p2, p3, p5 archsimd.Uint64x8
	s33, s29, s32  archsimd.Uint64x8
}

func sum64ConstsOf() (c sum64Consts) {
	c.p1 = archsimd.BroadcastUint64x8(prime1)
	c.p2 = archsimd.BroadcastUint64x8(prime2)
	c.p3 = archsimd.BroadcastUint64x8(prime3)
	c.p5 = archsimd.BroadcastUint64x8(prime5)
	c.s33 = archsimd.BroadcastUint64x8(33)
	c.s29 = archsimd.BroadcastUint64x8(29)
	c.s32 = archsimd.BroadcastUint64x8(32)
	return c
}

func avalanche8x64(h archsimd.Uint64x8, c *sum64Consts) archsimd.Uint64x8 {
	h = h.Xor(h.ShiftRight(c.s33))
	h = h.Mul(c.p2)
	h = h.Xor(h.ShiftRight(c.s29))
	h = h.Mul(c.p3)
	h = h.Xor(h.ShiftRight(c.s32))
	return h
}

// round8x64 computes round(0, input): rol31(input*prime2) * prime1.
func round8x64(input archsimd.Uint64x8, c *sum64Consts) archsimd.Uint64x8 {
	return input.Mul(c.p2).RotateAllLeft(31).Mul(c.p1)
}

func MultiSum64Uint8(h []uint64, v []uint8) int {
	n := min(len(h), len(v))
	i := 0
	if archsimd.X86.AVX512() && n >= 16 {
		c := sum64ConstsOf()
		seed := archsimd.BroadcastUint64x8(prime5 + 1)
		m := n / 16
		cv := unsafecast.Slice[[16]uint8](v)[:m]
		ch := unsafecast.Slice[[16]uint64](h)[:m]
		for j := range cv {
			x := archsimd.LoadUint8x16Slice(cv[j][:])
			v0 := x.ExtendLo8ToUint64()
			v1 := x.ConcatShiftBytesRight(8, x).ExtendLo8ToUint64()
			h0 := seed.Xor(v0.Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			h1 := seed.Xor(v1.Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			avalanche8x64(h0, &c).StoreSlice(ch[j][0:8])
			avalanche8x64(h1, &c).StoreSlice(ch[j][8:16])
		}
		i = m * 16
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
	if archsimd.X86.AVX512() && n >= 16 {
		c := sum64ConstsOf()
		seed := archsimd.BroadcastUint64x8(prime5 + 2)
		lowByte := archsimd.BroadcastUint64x8(0xFF)
		s8 := archsimd.BroadcastUint64x8(8)
		m := n / 16
		cv := unsafecast.Slice[[16]uint16](v)[:m]
		ch := unsafecast.Slice[[16]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint16x8Slice(cv[j][0:8]).ExtendToUint64()
			v1 := archsimd.LoadUint16x8Slice(cv[j][8:16]).ExtendToUint64()
			h0 := seed.Xor(v0.And(lowByte).Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			h1 := seed.Xor(v1.And(lowByte).Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			h0 = h0.Xor(v0.ShiftRight(s8).Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			h1 = h1.Xor(v1.ShiftRight(s8).Mul(c.p5)).RotateAllLeft(11).Mul(c.p1)
			avalanche8x64(h0, &c).StoreSlice(ch[j][0:8])
			avalanche8x64(h1, &c).StoreSlice(ch[j][8:16])
		}
		i = m * 16
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
	if archsimd.X86.AVX512() && n >= 16 {
		c := sum64ConstsOf()
		seed := archsimd.BroadcastUint64x8(prime5 + 4)
		m := n / 16
		cv := unsafecast.Slice[[16]uint32](v)[:m]
		ch := unsafecast.Slice[[16]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint32x8Slice(cv[j][0:8]).ExtendToUint64()
			v1 := archsimd.LoadUint32x8Slice(cv[j][8:16]).ExtendToUint64()
			h0 := seed.Xor(v0.Mul(c.p1)).RotateAllLeft(23).Mul(c.p2).Add(c.p3)
			h1 := seed.Xor(v1.Mul(c.p1)).RotateAllLeft(23).Mul(c.p2).Add(c.p3)
			avalanche8x64(h0, &c).StoreSlice(ch[j][0:8])
			avalanche8x64(h1, &c).StoreSlice(ch[j][8:16])
		}
		i = m * 16
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
	if archsimd.X86.AVX512() && n >= 16 {
		c := sum64ConstsOf()
		seed := archsimd.BroadcastUint64x8(prime5 + 8)
		p4 := archsimd.BroadcastUint64x8(prime4)
		m := n / 16
		cv := unsafecast.Slice[[16]uint64](v)[:m]
		ch := unsafecast.Slice[[16]uint64](h)[:m]
		for j := range cv {
			v0 := archsimd.LoadUint64x8Slice(cv[j][0:8])
			v1 := archsimd.LoadUint64x8Slice(cv[j][8:16])
			h0 := seed.Xor(round8x64(v0, &c)).RotateAllLeft(27).Mul(c.p1).Add(p4)
			h1 := seed.Xor(round8x64(v1, &c)).RotateAllLeft(27).Mul(c.p1).Add(p4)
			avalanche8x64(h0, &c).StoreSlice(ch[j][0:8])
			avalanche8x64(h1, &c).StoreSlice(ch[j][8:16])
		}
		i = m * 16
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
	if archsimd.X86.AVX512() && n >= 8 {
		c := sum64ConstsOf()
		seed := archsimd.BroadcastUint64x8(prime5 + 16)
		p4 := archsimd.BroadcastUint64x8(prime4)
		even := archsimd.LoadUint64x8Slice(evenLanes[:])
		odd := archsimd.LoadUint64x8Slice(oddLanes[:])
		m := n / 8
		cv := unsafecast.Slice[[16]uint64](v)[:m]
		ch := unsafecast.Slice[[8]uint64](h)[:m]
		for j := range cv {
			z0 := archsimd.LoadUint64x8Slice(cv[j][0:8])
			z1 := archsimd.LoadUint64x8Slice(cv[j][8:16])
			lo := z0.ConcatPermute(z1, even)
			hi := z0.ConcatPermute(z1, odd)
			hh := seed.Xor(round8x64(lo, &c)).RotateAllLeft(27).Mul(c.p1).Add(p4)
			hh = hh.Xor(round8x64(hi, &c)).RotateAllLeft(27).Mul(c.p1).Add(p4)
			avalanche8x64(hh, &c).StoreSlice(ch[j][:])
		}
		i = m * 8
		archsimd.ClearAVXUpperBits()
	}
	for ; i < n; i++ {
		h[i] = Sum64Uint128(v[i])
	}
	return n
}
