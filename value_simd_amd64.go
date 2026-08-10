//go:build !purego && goexperiment.simd

package parquet

import (
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// memsetValues fills values with copies of model.
//
// Like the assembly version it replaces, the vector path stores the 24-byte
// Value structs as raw words: 4 values (96 bytes) are written per iteration
// as 3 32-byte stores, bypassing write barriers the same way the assembly
// implementation did.
func memsetValues(values []Value, model Value) {
	if archsimd.X86.AVX2() && len(values) >= 4 {
		pattern := [4]Value{model, model, model, model}
		pw := unsafecast.Slice[uint64](pattern[:])
		v0 := archsimd.LoadUint64x4Slice(pw[0:])
		v1 := archsimd.LoadUint64x4Slice(pw[4:])
		v2 := archsimd.LoadUint64x4Slice(pw[8:])
		dw := unsafecast.Slice[uint64](values)
		i := 0
		for ; i+12 <= len(dw); i += 12 {
			v0.StoreSlice(dw[i:])
			v1.StoreSlice(dw[i+4:])
			v2.StoreSlice(dw[i+8:])
		}
		for j := i / 3; j < len(values); j++ {
			values[j] = model
		}
		return
	}
	for i := range values {
		values[i] = model
	}
}
