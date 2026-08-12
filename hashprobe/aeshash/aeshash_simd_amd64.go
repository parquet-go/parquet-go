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
	// The state vector is [seed, value] built with register-only inserts:
	// materializing it through a stack array serializes iterations of the
	// Multi loops on a store-forwarding stall (two 8 bytes stores followed
	// by a 16 bytes load of the same slot cannot forward).
	var z archsimd.Uint64x2
	x := z.SetElem(0, uint64(seed)).SetElem(1, uint64(value)).AsUint8x16()
	x = x.AESEncryptOneRound(k0)
	x = x.AESEncryptOneRound(k1)
	x = x.AESEncryptOneRound(k2)
	return uintptr(x.AsUint64x2().GetElem(0))
}

func hash64(value uint64, seed uintptr, k0, k1, k2 archsimd.Uint32x4) uintptr {
	var z archsimd.Uint64x2
	x := z.SetElem(0, uint64(seed)).SetElem(1, value).AsUint8x16()
	x = x.AESEncryptOneRound(k0)
	x = x.AESEncryptOneRound(k1)
	x = x.AESEncryptOneRound(k2)
	return uintptr(x.AsUint64x2().GetElem(0))
}

// scrambledSeed computes the seed vector shared by the 128 bits hashes: the
// per-call seed with the value length (16) repeated in the high words, mixed
// with the per-process key and scrambled with one AES round against itself.
func scrambledSeed(seed uintptr, k0 archsimd.Uint32x4) archsimd.Uint8x16 {
	var z archsimd.Uint64x2
	x := z.SetElem(0, uint64(seed)).SetElem(1, 0x0010001000100010).AsUint8x16().Xor(k0.AsUint8x16())
	return x.AESEncryptOneRound(x.AsUint32x4())
}

func hash128(x archsimd.Uint8x16, scrambled archsimd.Uint8x16) uintptr {
	x = x.Xor(scrambled)
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
	return hash128(archsimd.LoadUint8x16Slice(value[:]), scrambledSeed(seed, k0))
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
	// Load the values directly from the strided array: going through the
	// [16]byte copy that Index returns adds a stack round trip on every
	// element, which defeats store forwarding.
	a := values.UnsafeArray()
	for i := range hashes {
		v := (*[16]uint8)(a.Index(i))
		hashes[i] = hash128(archsimd.LoadUint8x16Slice(v[:]), scrambled)
	}
}
