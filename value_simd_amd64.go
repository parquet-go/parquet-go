//go:build !purego && goexperiment.simd

package parquet

// memsetValues fills values with copies of model.
//
// Value contains a pointer field, so the memory must not be written with raw
// vector stores the way the assembly version does: pointer writes to the heap
// need write barriers or a concurrent garbage collection cycle may fail to
// mark the pointee. Instead of SIMD, this uses a doubling copy: each copy of
// a []Value goes through runtime.typedslicecopy, which performs one bulk
// write barrier pass followed by a memmove, so the fill is GC-safe while
// still running at memmove speed with only O(log n) barrier passes.
func memsetValues(values []Value, model Value) {
	if len(values) > 0 {
		values[0] = model
		for n := 1; n < len(values); n *= 2 {
			copy(values[n:], values[:n])
		}
	}
}
