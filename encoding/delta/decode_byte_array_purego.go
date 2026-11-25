//go:build purego && goexperiment.simd && go1.27

package delta

import (
	"unsafe"

	"simd/archsimd"
)

// purego cannot link decode_byte_array_amd64.s. This is the same 32-byte
// copy loop the assembly replaced for the non-purego build.

func decodeByteArrayAVX2(dst, src []byte, prefix, suffix []int32) int {
	suffix = suffix[:len(prefix)]
	pd := unsafe.Pointer(unsafe.SliceData(dst))
	ps := unsafe.Pointer(unsafe.SliceData(src))
	pl := pd
	for k := range prefix {
		p := uintptr(uint32(prefix[k]))
		n := uintptr(uint32(suffix[k]))
		valueOffset := pd
		archsimd.LoadUint8x32Array((*[32]uint8)(pl)).StoreArray((*[32]uint8)(pd))
		if p > 32 {
			for m := uintptr(32); m < p; m += 32 {
				archsimd.LoadUint8x32Array((*[32]uint8)(unsafe.Add(pl, m))).StoreArray((*[32]uint8)(unsafe.Add(pd, m)))
			}
		}
		pd = unsafe.Add(pd, p)
		archsimd.LoadUint8x32Array((*[32]uint8)(ps)).StoreArray((*[32]uint8)(pd))
		if n > 32 {
			for m := uintptr(32); m < n; m += 32 {
				archsimd.LoadUint8x32Array((*[32]uint8)(unsafe.Add(ps, m))).StoreArray((*[32]uint8)(unsafe.Add(pd, m)))
			}
		}
		pd = unsafe.Add(pd, n)
		ps = unsafe.Add(ps, n)
		pl = valueOffset
	}
	archsimd.ClearAVXUpperBits()
	return int(uintptr(pd) - uintptr(unsafe.Pointer(unsafe.SliceData(dst))))
}
