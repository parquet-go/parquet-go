//go:build goexperiment.simd

package bytestreamsplit

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// This file provides implementations of the BYTE_STREAM_SPLIT codecs based
// on the simd/archsimd package, replacing the hand-written assembly of
// bytestreamsplit_amd64.s when GOEXPERIMENT=simd is set.
//
// The assembly used gather and scatter instructions, which archsimd does
// not expose; these implementations redesign the algorithm as a byte
// transpose in registers built entirely from two-source permutes
// (VPERMI2B/VPERMI2D/VPERMI2Q), with no gather or scatter at all. The byte
// grouping step is fused into the adjacent permute round via composed index
// tables computed by simulation (the byte level VPERMI2B round performs
// both at once), which matters because every 512 bit shuffle competes for a
// single execution port. The vector paths require AVX512VBMI for the byte
// level permutes.

var (
	bssEncFloatLo  = [64]uint8{0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 81, 85, 89, 93, 97, 101, 105, 109, 113, 117, 121, 125}
	bssEncFloatHi  = [64]uint8{2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94, 98, 102, 106, 110, 114, 118, 122, 126, 3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47, 51, 55, 59, 63, 67, 71, 75, 79, 83, 87, 91, 95, 99, 103, 107, 111, 115, 119, 123, 127}
	bssDecFloatLo  = [64]uint8{0, 16, 64, 80, 1, 17, 65, 81, 2, 18, 66, 82, 3, 19, 67, 83, 4, 20, 68, 84, 5, 21, 69, 85, 6, 22, 70, 86, 7, 23, 71, 87, 8, 24, 72, 88, 9, 25, 73, 89, 10, 26, 74, 90, 11, 27, 75, 91, 12, 28, 76, 92, 13, 29, 77, 93, 14, 30, 78, 94, 15, 31, 79, 95}
	bssDecFloatHi  = [64]uint8{32, 48, 96, 112, 33, 49, 97, 113, 34, 50, 98, 114, 35, 51, 99, 115, 36, 52, 100, 116, 37, 53, 101, 117, 38, 54, 102, 118, 39, 55, 103, 119, 40, 56, 104, 120, 41, 57, 105, 121, 42, 58, 106, 122, 43, 59, 107, 123, 44, 60, 108, 124, 45, 61, 109, 125, 46, 62, 110, 126, 47, 63, 111, 127}
	bssEncDoubleLo = [64]uint8{0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 1, 9, 17, 25, 33, 41, 49, 57, 65, 73, 81, 89, 97, 105, 113, 121, 2, 10, 18, 26, 34, 42, 50, 58, 66, 74, 82, 90, 98, 106, 114, 122, 3, 11, 19, 27, 35, 43, 51, 59, 67, 75, 83, 91, 99, 107, 115, 123}
	bssEncDoubleHi = [64]uint8{4, 12, 20, 28, 36, 44, 52, 60, 68, 76, 84, 92, 100, 108, 116, 124, 5, 13, 21, 29, 37, 45, 53, 61, 69, 77, 85, 93, 101, 109, 117, 125, 6, 14, 22, 30, 38, 46, 54, 62, 70, 78, 86, 94, 102, 110, 118, 126, 7, 15, 23, 31, 39, 47, 55, 63, 71, 79, 87, 95, 103, 111, 119, 127}
	bssDecDoubleLo = [64]uint8{0, 8, 16, 24, 64, 72, 80, 88, 1, 9, 17, 25, 65, 73, 81, 89, 2, 10, 18, 26, 66, 74, 82, 90, 3, 11, 19, 27, 67, 75, 83, 91, 4, 12, 20, 28, 68, 76, 84, 92, 5, 13, 21, 29, 69, 77, 85, 93, 6, 14, 22, 30, 70, 78, 86, 94, 7, 15, 23, 31, 71, 79, 87, 95}
	bssDecDoubleHi = [64]uint8{32, 40, 48, 56, 96, 104, 112, 120, 33, 41, 49, 57, 97, 105, 113, 121, 34, 42, 50, 58, 98, 106, 114, 122, 35, 43, 51, 59, 99, 107, 115, 123, 36, 44, 52, 60, 100, 108, 116, 124, 37, 45, 53, 61, 101, 109, 117, 125, 38, 46, 54, 62, 102, 110, 118, 126, 39, 47, 55, 63, 103, 111, 119, 127}
	bssBlkLo4      = [16]uint32{0, 1, 2, 3, 16, 17, 18, 19, 4, 5, 6, 7, 20, 21, 22, 23}
	bssBlkHi4      = [16]uint32{8, 9, 10, 11, 24, 25, 26, 27, 12, 13, 14, 15, 28, 29, 30, 31}
	bssHalfLo4     = [16]uint32{0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23}
	bssHalfHi4     = [16]uint32{8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31}
	bssQLo1        = [8]uint64{0, 8, 1, 9, 2, 10, 3, 11}
	bssQHi1        = [8]uint64{4, 12, 5, 13, 6, 14, 7, 15}
	bssQLo2        = [8]uint64{0, 1, 8, 9, 2, 3, 10, 11}
	bssQHi2        = [8]uint64{4, 5, 12, 13, 6, 7, 14, 15}
	bssQLo3        = [8]uint64{0, 1, 2, 3, 8, 9, 10, 11}
	bssQHi3        = [8]uint64{4, 5, 6, 7, 12, 13, 14, 15}
)

func encodeFloat(dst, src []byte) {
	n := len(src) / 4
	i := 0
	if archsimd.X86.AVX512() && archsimd.X86.AVX512VBMI() && n >= 64 {
		glo := archsimd.LoadUint8x64Slice(bssEncFloatLo[:])
		ghi := archsimd.LoadUint8x64Slice(bssEncFloatHi[:])
		hlo := archsimd.LoadUint32x16Slice(bssHalfLo4[:])
		hhi := archsimd.LoadUint32x16Slice(bssHalfHi4[:])
		for ; i+64 <= n; i += 64 {
			z0 := archsimd.LoadUint8x64Slice(src[4*i+0 : 4*i+64])
			z1 := archsimd.LoadUint8x64Slice(src[4*i+64 : 4*i+128])
			z2 := archsimd.LoadUint8x64Slice(src[4*i+128 : 4*i+192])
			z3 := archsimd.LoadUint8x64Slice(src[4*i+192 : 4*i+256])
			t0 := z0.ConcatPermute(z1, glo).AsUint32x16()
			t1 := z0.ConcatPermute(z1, ghi).AsUint32x16()
			t2 := z2.ConcatPermute(z3, glo).AsUint32x16()
			t3 := z2.ConcatPermute(z3, ghi).AsUint32x16()
			t0.ConcatPermute(t2, hlo).AsUint8x64().StoreSlice(dst[0*n+i : 0*n+i+64])
			t0.ConcatPermute(t2, hhi).AsUint8x64().StoreSlice(dst[1*n+i : 1*n+i+64])
			t1.ConcatPermute(t3, hlo).AsUint8x64().StoreSlice(dst[2*n+i : 2*n+i+64])
			t1.ConcatPermute(t3, hhi).AsUint8x64().StoreSlice(dst[3*n+i : 3*n+i+64])
		}
		archsimd.ClearAVXUpperBits()
	}
	if i < n {
		b0 := dst[0*n : 1*n]
		b1 := dst[1*n : 2*n]
		b2 := dst[2*n : 3*n]
		b3 := dst[3*n : 4*n]
		for j, v := range unsafecast.Slice[uint32](src)[i:] {
			b0[i+j] = byte(v >> 0)
			b1[i+j] = byte(v >> 8)
			b2[i+j] = byte(v >> 16)
			b3[i+j] = byte(v >> 24)
		}
	}
}

func decodeFloat(dst, src []byte) {
	n := len(src) / 4
	i := 0
	if archsimd.X86.AVX512() && archsimd.X86.AVX512VBMI() && n >= 64 {
		blo := archsimd.LoadUint32x16Slice(bssBlkLo4[:])
		bhi := archsimd.LoadUint32x16Slice(bssBlkHi4[:])
		glo := archsimd.LoadUint8x64Slice(bssDecFloatLo[:])
		ghi := archsimd.LoadUint8x64Slice(bssDecFloatHi[:])
		for ; i+64 <= n; i += 64 {
			p0 := archsimd.LoadUint8x64Slice(src[0*n+i : 0*n+i+64]).AsUint32x16()
			p1 := archsimd.LoadUint8x64Slice(src[1*n+i : 1*n+i+64]).AsUint32x16()
			p2 := archsimd.LoadUint8x64Slice(src[2*n+i : 2*n+i+64]).AsUint32x16()
			p3 := archsimd.LoadUint8x64Slice(src[3*n+i : 3*n+i+64]).AsUint32x16()
			t0 := p0.ConcatPermute(p1, blo).AsUint8x64()
			t1 := p0.ConcatPermute(p1, bhi).AsUint8x64()
			t2 := p2.ConcatPermute(p3, blo).AsUint8x64()
			t3 := p2.ConcatPermute(p3, bhi).AsUint8x64()
			t0.ConcatPermute(t2, glo).StoreSlice(dst[4*i+0 : 4*i+64])
			t0.ConcatPermute(t2, ghi).StoreSlice(dst[4*i+64 : 4*i+128])
			t1.ConcatPermute(t3, glo).StoreSlice(dst[4*i+128 : 4*i+192])
			t1.ConcatPermute(t3, ghi).StoreSlice(dst[4*i+192 : 4*i+256])
		}
		archsimd.ClearAVXUpperBits()
	}
	if i < n {
		b0 := src[0*n : 1*n]
		b1 := src[1*n : 2*n]
		b2 := src[2*n : 3*n]
		b3 := src[3*n : 4*n]
		dst32 := unsafecast.Slice[uint32](dst)
		for j := i; j < n; j++ {
			dst32[j] = uint32(b0[j]) |
				uint32(b1[j])<<8 |
				uint32(b2[j])<<16 |
				uint32(b3[j])<<24
		}
	}
}

func encodeDouble(dst, src []byte) {
	n := len(src) / 8
	i := 0
	if archsimd.X86.AVX512() && archsimd.X86.AVX512VBMI() && n >= 64 {
		glo := archsimd.LoadUint8x64Slice(bssEncDoubleLo[:])
		ghi := archsimd.LoadUint8x64Slice(bssEncDoubleHi[:])
		lo2 := archsimd.LoadUint64x8Slice(bssQLo2[:])
		hi2 := archsimd.LoadUint64x8Slice(bssQHi2[:])
		lo3 := archsimd.LoadUint64x8Slice(bssQLo3[:])
		hi3 := archsimd.LoadUint64x8Slice(bssQHi3[:])
		for ; i+64 <= n; i += 64 {
			z0 := archsimd.LoadUint8x64Slice(src[8*i+0 : 8*i+64])
			z1 := archsimd.LoadUint8x64Slice(src[8*i+64 : 8*i+128])
			z2 := archsimd.LoadUint8x64Slice(src[8*i+128 : 8*i+192])
			z3 := archsimd.LoadUint8x64Slice(src[8*i+192 : 8*i+256])
			z4 := archsimd.LoadUint8x64Slice(src[8*i+256 : 8*i+320])
			z5 := archsimd.LoadUint8x64Slice(src[8*i+320 : 8*i+384])
			z6 := archsimd.LoadUint8x64Slice(src[8*i+384 : 8*i+448])
			z7 := archsimd.LoadUint8x64Slice(src[8*i+448 : 8*i+512])
			a0 := z0.ConcatPermute(z1, glo).AsUint64x8()
			a1 := z0.ConcatPermute(z1, ghi).AsUint64x8()
			a2 := z2.ConcatPermute(z3, glo).AsUint64x8()
			a3 := z2.ConcatPermute(z3, ghi).AsUint64x8()
			a4 := z4.ConcatPermute(z5, glo).AsUint64x8()
			a5 := z4.ConcatPermute(z5, ghi).AsUint64x8()
			a6 := z6.ConcatPermute(z7, glo).AsUint64x8()
			a7 := z6.ConcatPermute(z7, ghi).AsUint64x8()
			b0 := a0.ConcatPermute(a2, lo2)
			b1 := a0.ConcatPermute(a2, hi2)
			b2 := a1.ConcatPermute(a3, lo2)
			b3 := a1.ConcatPermute(a3, hi2)
			b4 := a4.ConcatPermute(a6, lo2)
			b5 := a4.ConcatPermute(a6, hi2)
			b6 := a5.ConcatPermute(a7, lo2)
			b7 := a5.ConcatPermute(a7, hi2)
			b0.ConcatPermute(b4, lo3).AsUint8x64().StoreSlice(dst[0*n+i : 0*n+i+64])
			b0.ConcatPermute(b4, hi3).AsUint8x64().StoreSlice(dst[1*n+i : 1*n+i+64])
			b1.ConcatPermute(b5, lo3).AsUint8x64().StoreSlice(dst[2*n+i : 2*n+i+64])
			b1.ConcatPermute(b5, hi3).AsUint8x64().StoreSlice(dst[3*n+i : 3*n+i+64])
			b2.ConcatPermute(b6, lo3).AsUint8x64().StoreSlice(dst[4*n+i : 4*n+i+64])
			b2.ConcatPermute(b6, hi3).AsUint8x64().StoreSlice(dst[5*n+i : 5*n+i+64])
			b3.ConcatPermute(b7, lo3).AsUint8x64().StoreSlice(dst[6*n+i : 6*n+i+64])
			b3.ConcatPermute(b7, hi3).AsUint8x64().StoreSlice(dst[7*n+i : 7*n+i+64])
		}
		archsimd.ClearAVXUpperBits()
	}
	if i < n {
		b0 := dst[0*n : 1*n]
		b1 := dst[1*n : 2*n]
		b2 := dst[2*n : 3*n]
		b3 := dst[3*n : 4*n]
		b4 := dst[4*n : 5*n]
		b5 := dst[5*n : 6*n]
		b6 := dst[6*n : 7*n]
		b7 := dst[7*n : 8*n]
		for j, v := range unsafecast.Slice[uint64](src)[i:] {
			b0[i+j] = byte(v >> 0)
			b1[i+j] = byte(v >> 8)
			b2[i+j] = byte(v >> 16)
			b3[i+j] = byte(v >> 24)
			b4[i+j] = byte(v >> 32)
			b5[i+j] = byte(v >> 40)
			b6[i+j] = byte(v >> 48)
			b7[i+j] = byte(v >> 56)
		}
	}
}

func decodeDouble(dst, src []byte) {
	n := len(src) / 8
	i := 0
	if archsimd.X86.AVX512() && archsimd.X86.AVX512VBMI() && n >= 64 {
		lo1 := archsimd.LoadUint64x8Slice(bssQLo1[:])
		hi1 := archsimd.LoadUint64x8Slice(bssQHi1[:])
		lo2 := archsimd.LoadUint64x8Slice(bssQLo2[:])
		hi2 := archsimd.LoadUint64x8Slice(bssQHi2[:])
		glo := archsimd.LoadUint8x64Slice(bssDecDoubleLo[:])
		ghi := archsimd.LoadUint8x64Slice(bssDecDoubleHi[:])
		for ; i+64 <= n; i += 64 {
			p0 := archsimd.LoadUint8x64Slice(src[0*n+i : 0*n+i+64]).AsUint64x8()
			p1 := archsimd.LoadUint8x64Slice(src[1*n+i : 1*n+i+64]).AsUint64x8()
			p2 := archsimd.LoadUint8x64Slice(src[2*n+i : 2*n+i+64]).AsUint64x8()
			p3 := archsimd.LoadUint8x64Slice(src[3*n+i : 3*n+i+64]).AsUint64x8()
			p4 := archsimd.LoadUint8x64Slice(src[4*n+i : 4*n+i+64]).AsUint64x8()
			p5 := archsimd.LoadUint8x64Slice(src[5*n+i : 5*n+i+64]).AsUint64x8()
			p6 := archsimd.LoadUint8x64Slice(src[6*n+i : 6*n+i+64]).AsUint64x8()
			p7 := archsimd.LoadUint8x64Slice(src[7*n+i : 7*n+i+64]).AsUint64x8()
			a0 := p0.ConcatPermute(p1, lo1)
			a1 := p0.ConcatPermute(p1, hi1)
			a2 := p2.ConcatPermute(p3, lo1)
			a3 := p2.ConcatPermute(p3, hi1)
			a4 := p4.ConcatPermute(p5, lo1)
			a5 := p4.ConcatPermute(p5, hi1)
			a6 := p6.ConcatPermute(p7, lo1)
			a7 := p6.ConcatPermute(p7, hi1)
			b0 := a0.ConcatPermute(a2, lo2).AsUint8x64()
			b1 := a0.ConcatPermute(a2, hi2).AsUint8x64()
			b2 := a1.ConcatPermute(a3, lo2).AsUint8x64()
			b3 := a1.ConcatPermute(a3, hi2).AsUint8x64()
			b4 := a4.ConcatPermute(a6, lo2).AsUint8x64()
			b5 := a4.ConcatPermute(a6, hi2).AsUint8x64()
			b6 := a5.ConcatPermute(a7, lo2).AsUint8x64()
			b7 := a5.ConcatPermute(a7, hi2).AsUint8x64()
			b0.ConcatPermute(b4, glo).StoreSlice(dst[8*i+0 : 8*i+64])
			b0.ConcatPermute(b4, ghi).StoreSlice(dst[8*i+64 : 8*i+128])
			b1.ConcatPermute(b5, glo).StoreSlice(dst[8*i+128 : 8*i+192])
			b1.ConcatPermute(b5, ghi).StoreSlice(dst[8*i+192 : 8*i+256])
			b2.ConcatPermute(b6, glo).StoreSlice(dst[8*i+256 : 8*i+320])
			b2.ConcatPermute(b6, ghi).StoreSlice(dst[8*i+320 : 8*i+384])
			b3.ConcatPermute(b7, glo).StoreSlice(dst[8*i+384 : 8*i+448])
			b3.ConcatPermute(b7, ghi).StoreSlice(dst[8*i+448 : 8*i+512])
		}
		archsimd.ClearAVXUpperBits()
	}
	if i < n {
		b0 := src[0*n : 1*n]
		b1 := src[1*n : 2*n]
		b2 := src[2*n : 3*n]
		b3 := src[3*n : 4*n]
		b4 := src[4*n : 5*n]
		b5 := src[5*n : 6*n]
		b6 := src[6*n : 7*n]
		b7 := src[7*n : 8*n]
		dst64 := unsafecast.Slice[uint64](dst)
		for j := i; j < n; j++ {
			dst64[j] = uint64(b0[j]) |
				uint64(b1[j])<<8 |
				uint64(b2[j])<<16 |
				uint64(b3[j])<<24 |
				uint64(b4[j])<<32 |
				uint64(b5[j])<<40 |
				uint64(b6[j])<<48 |
				uint64(b7[j])<<56
		}
	}
}

func encodeInt32(dst, src []byte) {
	encodeFloat(dst, src)
}

func decodeInt32(dst, src []byte) {
	decodeFloat(dst, src)
}

func encodeInt64(dst, src []byte) {
	encodeDouble(dst, src)
}

func decodeInt64(dst, src []byte) {
	decodeDouble(dst, src)
}
