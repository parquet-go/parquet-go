//go:build goexperiment.simd

package hashprobe

import (
	"math/bits"
	"unsafe"

	"simd/archsimd"

	"github.com/parquet-go/parquet-go/sparse"
)

// This file provides implementations of the multiProbe kernels based on the
// simd/archsimd package, replacing the hand-written assembly of
// hashprobe_amd64.s when GOEXPERIMENT=simd is set.
//
// The tables use open addressing with groups of keys sized so that a whole
// group can be compared against the probe key with a single vector compare;
// the compare mask is filtered by the group's occupancy bits (which also
// discards the false match that the bits field itself could produce in the
// eighth lane of the 32-bit groups).

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int {
	if archsimd.X86.AVX2() {
		return multiProbe32SIMD(table, numKeys, hashes, keys, values)
	}
	return multiProbe32Default(table, numKeys, hashes, keys, values)
}

func multiProbe32SIMD(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int {
	modulo := uintptr(len(table)) - 1

	for i, hash := range hashes {
		key := keys.Index(i)
		kv := archsimd.BroadcastUint32x8(key)
		for {
			group := &table[hash&modulo]
			g := (*[16]uint32)(unsafe.Pointer(group))
			m := uint32(kv.Equal(archsimd.LoadUint32x8Slice(g[0:8])).ToBits()) & group.bits

			if m != 0 {
				values[i] = int32(group.values[bits.TrailingZeros32(m)])
				break
			}
			n := bits.OnesCount32(group.bits)
			if n == table32GroupSize {
				hash++
				continue
			}
			group.bits = (group.bits << 1) | 1
			group.keys[n] = key
			group.values[n] = uint32(numKeys)
			values[i] = int32(numKeys)
			numKeys++
			break
		}
	}

	return numKeys
}

func multiProbe64(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int {
	if archsimd.X86.AVX2() {
		return multiProbe64SIMD(table, numKeys, hashes, keys, values)
	}
	return multiProbe64Default(table, numKeys, hashes, keys, values)
}

func multiProbe64SIMD(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int {
	modulo := uintptr(len(table)) - 1

	for i, hash := range hashes {
		key := keys.Index(i)
		kv := archsimd.BroadcastUint64x4(key)
		for {
			group := &table[hash&modulo]
			g := (*[8]uint64)(unsafe.Pointer(group))
			m := uint32(kv.Equal(archsimd.LoadUint64x4Slice(g[0:4])).ToBits()) & group.bits

			if m != 0 {
				values[i] = int32(group.values[bits.TrailingZeros32(m)])
				break
			}
			n := bits.OnesCount32(group.bits)
			if n == table64GroupSize {
				hash++
				continue
			}
			group.bits = (group.bits << 1) | 1
			group.keys[n] = key
			group.values[n] = uint32(numKeys)
			values[i] = int32(numKeys)
			numKeys++
			break
		}
	}

	return numKeys
}

// multiProbe128 delegates to the scalar implementation: a 16 bytes key
// compare is two 8 bytes compares in general purpose registers, and holding
// the key in a vector register across the probe loop makes the compiler
// spill it with a legacy (non-VEX) MOVUPS on every iteration, which costs
// more than the compare itself.
func multiProbe128(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int {
	return multiProbe128Default(table, tableCap, tableLen, hashes, keys, values)
}
