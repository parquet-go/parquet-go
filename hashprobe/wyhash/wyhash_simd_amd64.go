//go:build !purego && goexperiment.simd

package wyhash

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/sparse"
)

// The assembly versions of these kernels are scalar MULQ loops over strided
// pointers; the loops below compile to the same two MULQ per value
// (bits.Mul64 is an intrinsic), with the seed mixed into the constants once
// and the strided pointer walked without per element indexing.

func MultiHashUint32Array(hashes []uintptr, values sparse.Uint32Array, seed uintptr) {
	a := values.UnsafeArray()
	m1s := m1 ^ uint64(seed)
	for i := range hashes {
		v := uint64(*(*uint32)(a.Index(i)))
		hashes[i] = uintptr(mix(m5^4, mix(v^m2, v^m1s)))
	}
}

func MultiHashUint64Array(hashes []uintptr, values sparse.Uint64Array, seed uintptr) {
	a := values.UnsafeArray()
	m1s := m1 ^ uint64(seed)
	for i := range hashes {
		v := *(*uint64)(a.Index(i))
		hashes[i] = uintptr(mix(m5^8, mix(v^m2, v^m1s)))
	}
}

func MultiHashUint128Array(hashes []uintptr, values sparse.Uint128Array, seed uintptr) {
	a := values.UnsafeArray()
	m1s := m1 ^ uint64(seed)
	for i := range hashes {
		p := a.Index(i)
		lo := *(*uint64)(p)
		hi := *(*uint64)(unsafe.Add(p, 8))
		hashes[i] = uintptr(mix(m5^16, mix(lo^m2, hi^m1s)))
	}
}
