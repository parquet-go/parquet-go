//go:build goexperiment.simd && go1.27

// Go 1.27 simd/archsimd API. The Go 1.26 kernels stay in the sibling file gated !go1.27.
package rle

import (
	"encoding/binary"

	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// The functions in this file are simd/archsimd replacements for some of the
// kernels dispatched through the function variables of rle_amd64.go (assembly
// builds) or rle_purego.go (purego builds). The BMI2 bit-packing kernels have
// no archsimd equivalent (PDEP/PEXT are not exposed) and keep their default
// dispatch.
//
// The override is safe in both builds: package variable initializers run
// before all init functions, and Go compiles the files of a package in file
// name order, so this init runs after the one in rle_amd64.go.
func init() {
	if archsimd.X86.AVX512() && archsimd.X86.AVX512VBMI() {
		encodeBytesBitpack = encodeBytesBitpackSIMD
		decodeBytesBitpack = decodeBytesBitpackSIMD
	}
}

// encodeBytesBitpackSIMD packs 8 byte values per input word into bitWidth
// bits each, replacing the PDEP kernel of the assembly. Each 64 byte load
// covers 8 words; three fold levels narrow the byte lanes in place
// (a|b<<8 -> a|b<<w at 16, 32 and 64 bit granularity, all shifts with
// broadcast counts to avoid the legacy MOVQ of the scalar shift forms), and
// one VPERMB compacts the 8 groups of bitWidth bytes for a single store.
// The full 64 byte stores require room past the packed output, so the last
// words fall back to the scalar loop, which only overruns by the 8 bytes
// the callers guarantee (see encodeBytesBitpackDefault).
func encodeBytesBitpackSIMD(dst []byte, src []uint64, bitWidth uint) int {
	if bitWidth == 8 {
		return copy(dst, unsafecast.Slice[byte](src))
	}
	w := bitWidth
	bitMask := uint64(1<<w) - 1
	cp := archsimd.LoadUint8x64(bitpackCompact[w][:])
	vmask := archsimd.BroadcastUint8x64(uint8(bitMask))
	m16 := archsimd.BroadcastUint16x32(0x00FF)
	s16r := archsimd.BroadcastUint16x32(8)
	s16l := archsimd.BroadcastUint16x32(uint16(w))
	m32 := archsimd.BroadcastUint32x16(0x0000FFFF)
	s32r := archsimd.BroadcastUint32x16(16)
	s32l := archsimd.BroadcastUint32x16(uint32(2 * w))
	m64 := archsimd.BroadcastUint64x8(0x00000000FFFFFFFF)
	s64r := archsimd.BroadcastUint64x8(32)
	s64l := archsimd.BroadcastUint64x8(uint64(4 * w))
	b := unsafecast.Slice[byte](src)
	n := 0
	j := 0
	for ; j+64 <= len(b) && n+64 <= len(dst); j += 64 {
		c := archsimd.LoadUint8x64(b[j : j+64]).And(vmask)
		t16 := c.AsUint16x32()
		t16 = t16.And(m16).Or(t16.ShiftRight(s16r).ShiftLeft(s16l))
		t32 := t16.AsUint32x16()
		t32 = t32.And(m32).Or(t32.ShiftRight(s32r).ShiftLeft(s32l))
		t64 := t32.AsUint64x8()
		t64 = t64.And(m64).Or(t64.ShiftRight(s64r).ShiftLeft(s64l))
		t64.AsUint8x64().Permute(cp).Store(dst[n : n+64])
		n += int(8 * w)
	}
	archsimd.ClearAVXUpperBits()
	for _, word := range src[j/8:] {
		word = (word & bitMask) |
			(((word >> 8) & bitMask) << (1 * w)) |
			(((word >> 16) & bitMask) << (2 * w)) |
			(((word >> 24) & bitMask) << (3 * w)) |
			(((word >> 32) & bitMask) << (4 * w)) |
			(((word >> 40) & bitMask) << (5 * w)) |
			(((word >> 48) & bitMask) << (6 * w)) |
			(((word >> 56) & bitMask) << (7 * w))
		binary.LittleEndian.PutUint64(dst[n:], word)
		n += int(w)
	}
	return int(uint(len(src)) * w)
}

// decodeBytesBitpackSIMD expands bitWidth bit fields into bytes, replacing
// the PEXT kernel of the assembly. Two VPERMB gathers place the byte pair
// containing each field into a 16 bit lane, per lane variable shifts align
// the field, a mask isolates it, and a two source byte permute compacts the
// low bytes of the 64 lanes into the output vector. The index and shift
// tables depend only on bitWidth and are built once per call. Full 64 byte
// loads read past the consumed input, so the loop requires 64 readable
// bytes and leaves the tail to the scalar path.
func decodeBytesBitpackSIMD(dst, src []byte, count, bitWidth uint) {
	w := bitWidth
	if w == 8 {
		copy(dst, src[:count])
		return
	}
	bitMask := uint64(1<<w) - 1
	t := &bitpackUnpack[w]
	ia := archsimd.LoadUint8x64(t.idxA[:])
	ib := archsimd.LoadUint8x64(t.idxB[:])
	cm := archsimd.LoadUint8x64(bitpackInterleave[:])
	sa := archsimd.LoadUint16x32(t.shA[:])
	sb := archsimd.LoadUint16x32(t.shB[:])
	vmask := archsimd.BroadcastUint16x32(uint16(bitMask))
	i := 0
	o := 0
	for ; count >= 64 && i+64 <= len(src); count -= 64 {
		c := archsimd.LoadUint8x64(src[i : i+64])
		va := c.Permute(ia).AsUint16x32().ShiftRight(sa).And(vmask)
		vb := c.Permute(ib).AsUint16x32().ShiftRight(sb).And(vmask)
		va.AsUint8x64().ConcatPermute(vb.AsUint8x64(), cm).Store(dst[o : o+64])
		i += int(8 * w)
		o += 64
	}
	archsimd.ClearAVXUpperBits()
	if count >= 64 {
		// The loop consumes 8*bitWidth bytes per iteration but loads 64, so
		// the last iterations would read past the input. Copy the (at most
		// 64) remaining bytes into a zero padded buffer and recurse once:
		// the padded length keeps the recursion entirely in the vector
		// loop above. A second inline loop would share the table vectors
		// with the first and make the register allocator spill them on
		// every iteration of both (measured 3.5x on the whole kernel).
		var buf [128]byte
		copy(buf[:], src[i:])
		decodeBytesBitpackSIMD(dst[o:], buf[:], count, w)
		return
	}
	for ; count > 0; count -= 8 {
		var bits [8]byte
		copy(bits[:], src[i:min(i+int(w), len(src))])
		word := binary.LittleEndian.Uint64(bits[:])
		for e := range 8 {
			dst[o] = byte((word >> (uint(e) * w)) & bitMask)
			o++
		}
		i += int(w)
	}
}

// Permute and shift tables for the byte bit-packers, precomputed per bit
// width: building them per call dominated the runtime for small inputs
// (the first benchmark round measured up to +1550% on decode from the per
// call setup alone).
var (
	bitpackCompact    [8][64]uint8
	bitpackUnpack     [8]bitpackUnpackTables
	bitpackInterleave [64]uint8
)

type bitpackUnpackTables struct {
	idxA, idxB [64]uint8
	shA, shB   [32]uint16
}

func init() {
	for w := uint(1); w < 8; w++ {
		for m := range 8 * w {
			bitpackCompact[w][m] = uint8((m/w)*8 + m%w)
		}
		t := &bitpackUnpack[w]
		for m := range 64 {
			q, e := m/8, m%8
			bit := uint(e) * w
			b := uint(q)*w + bit/8
			if m < 32 {
				t.idxA[2*m] = uint8(b)
				t.idxA[2*m+1] = uint8(b + 1)
				t.shA[m] = uint16(bit % 8)
			} else {
				t.idxB[2*(m-32)] = uint8(b)
				t.idxB[2*(m-32)+1] = uint8(b + 1)
				t.shB[m-32] = uint16(bit % 8)
			}
		}
	}
	for m := range 64 {
		if m < 32 {
			bitpackInterleave[m] = uint8(2 * m)
		} else {
			bitpackInterleave[m] = uint8(64 + 2*(m-32))
		}
	}
}
