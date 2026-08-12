//go:build goexperiment.simd

package aeshash

import (
	"math/rand"
	"unsafe"

	"simd/archsimd"

	"github.com/parquet-go/parquet-go/sparse"
)

// This file provides implementations of the AES hash functions based on the
// simd/archsimd package, replacing the hand-written assembly of
// aeshash_amd64.s when GOEXPERIMENT=simd is set. The algorithms mirror the
// assembly (which itself mirrors the Go runtime's aeshash family): the only
// behavioral difference is the feature gate, which requires AVX in addition
// to AES-NI because archsimd's AESEncryptOneRound uses the VEX encoding.

// hashRandomBytes is 48 since this is what the hash algorithms depend on.
const hashRandomBytes = 48

var aeskeysched [hashRandomBytes]byte

func init() {
	for _, v := range aeskeysched {
		if v != 0 {
			// aeskeysched was initialized somewhere else (e.g. tests), so we
			// can skip initialization. No synchronization is needed since init
			// functions are called sequentially in a single goroutine (see
			// https://go.dev/ref/spec#Package_initialization).
			return
		}
	}

	key := (*[hashRandomBytes / 8]uint64)(unsafe.Pointer(&aeskeysched))
	for i := range key {
		key[i] = rand.Uint64()
	}
}

// Enabled returns true if AES hash is available on the system.
func Enabled() bool { return archsimd.X86.AVXAES() }

func roundKeys() (k0, k1, k2 archsimd.Uint32x4) {
	k := (*[12]uint32)(unsafe.Pointer(&aeskeysched))
	k0 = archsimd.LoadUint32x4Slice(k[0:4])
	k1 = archsimd.LoadUint32x4Slice(k[4:8])
	k2 = archsimd.LoadUint32x4Slice(k[8:12])
	return k0, k1, k2
}

func hash32(value uint32, seed uintptr, k0, k1, k2 archsimd.Uint32x4) uintptr {
	s := [4]uint32{uint32(seed), uint32(uint64(seed) >> 32), value, 0}
	x := archsimd.LoadUint32x4Slice(s[:]).AsUint8x16()
	x = x.AESEncryptOneRound(k0)
	x = x.AESEncryptOneRound(k1)
	x = x.AESEncryptOneRound(k2)
	return uintptr(x.AsUint64x2().GetElem(0))
}

func hash64(value uint64, seed uintptr, k0, k1, k2 archsimd.Uint32x4) uintptr {
	s := [2]uint64{uint64(seed), value}
	x := archsimd.LoadUint64x2Slice(s[:]).AsUint8x16()
	x = x.AESEncryptOneRound(k0)
	x = x.AESEncryptOneRound(k1)
	x = x.AESEncryptOneRound(k2)
	return uintptr(x.AsUint64x2().GetElem(0))
}

// scrambledSeed computes the seed vector shared by the 128 bits hashes: the
// per-call seed with the value length (16) repeated in the high words, mixed
// with the per-process key and scrambled with one AES round against itself.
func scrambledSeed(seed uintptr, k0 archsimd.Uint32x4) archsimd.Uint8x16 {
	s := [2]uint64{uint64(seed), 0x0010001000100010}
	x := archsimd.LoadUint64x2Slice(s[:]).AsUint8x16().Xor(k0.AsUint8x16())
	return x.AESEncryptOneRound(x.AsUint32x4())
}

func hash128(value [16]byte, scrambled archsimd.Uint8x16) uintptr {
	x := archsimd.LoadUint8x16Slice(value[:]).Xor(scrambled)
	x = x.AESEncryptOneRound(x.AsUint32x4())
	x = x.AESEncryptOneRound(x.AsUint32x4())
	x = x.AESEncryptOneRound(x.AsUint32x4())
	return uintptr(x.AsUint64x2().GetElem(0))
}

func Hash32(value uint32, seed uintptr) uintptr {
	k0, k1, k2 := roundKeys()
	return hash32(value, seed, k0, k1, k2)
}

func Hash64(value uint64, seed uintptr) uintptr {
	k0, k1, k2 := roundKeys()
	return hash64(value, seed, k0, k1, k2)
}

func Hash128(value [16]byte, seed uintptr) uintptr {
	k0, _, _ := roundKeys()
	return hash128(value, scrambledSeed(seed, k0))
}

func MultiHashUint32Array(hashes []uintptr, values sparse.Uint32Array, seed uintptr) {
	k0, k1, k2 := roundKeys()
	for i := range hashes {
		hashes[i] = hash32(values.Index(i), seed, k0, k1, k2)
	}
}

func MultiHashUint64Array(hashes []uintptr, values sparse.Uint64Array, seed uintptr) {
	k0, k1, k2 := roundKeys()
	for i := range hashes {
		hashes[i] = hash64(values.Index(i), seed, k0, k1, k2)
	}
}

func MultiHashUint128Array(hashes []uintptr, values sparse.Uint128Array, seed uintptr) {
	k0, _, _ := roundKeys()
	scrambled := scrambledSeed(seed, k0)
	for i := range hashes {
		hashes[i] = hash128(values.Index(i), scrambled)
	}
}
