//go:build goexperiment.simd

package parquet

import (
	"encoding/binary"
	"simd/archsimd"

	"github.com/parquet-go/bitpack/unsafecast"
)

// This file provides implementations of the page min/max/bounds kernels based
// on the simd/archsimd package, replacing the hand-written assembly of
// page_min_amd64.s, page_max_amd64.s and page_bounds_amd64.s when
// GOEXPERIMENT=simd is set. Unlike the assembly, which only had AVX-512
// paths, these implementations also provide AVX2 paths for CPUs without
// AVX-512.
//
// Floating point kernels ignore NaN values after the first element, matching
// the behavior of the assembly (floatPage.Bounds skips leading NaNs). The
// assembly guaranteed this with the operand order of VMINPS/VMAXPS, but
// archsimd's Min/Max do not document NaN semantics and the compiler may
// canonicalize the operands of commutative operations, so the float paths
// use an explicit compare-and-merge: Less/Greater are false for NaN, keeping
// the accumulator.
//
// The AVX2 tier of the 64-bit integer kernels also uses compare-and-merge:
// VPMINSQ/VPMAXSQ only exist in AVX-512, so Int64x4.Min would fault on
// AVX2-only CPUs. VPCMPGTQ is signed, so the unsigned variant biases both
// operands by 1<<63 before comparing.

// The thresholds below are kept from the assembly implementation because
// page_bounds_amd64_test.go references them; the Go kernels use a single
// combined pass for bounds at every size.
const (
	combinedBoundsThreshold      = 1 * 1024 * 1024
	combinedBoundsInt64Threshold = (DefaultPageBufferSize*98/100 + 7) / 8
)

func reduceMinInt32x4(v archsimd.Int32x4) int32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	if x := v.GetElem(2); x < m {
		m = x
	}
	if x := v.GetElem(3); x < m {
		m = x
	}
	return m
}

func reduceMaxInt32x4(v archsimd.Int32x4) int32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	if x := v.GetElem(2); x > m {
		m = x
	}
	if x := v.GetElem(3); x > m {
		m = x
	}
	return m
}

func reduceMinInt64x2(v archsimd.Int64x2) int64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	return m
}

func reduceMaxInt64x2(v archsimd.Int64x2) int64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	return m
}

func reduceMinUint32x4(v archsimd.Uint32x4) uint32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	if x := v.GetElem(2); x < m {
		m = x
	}
	if x := v.GetElem(3); x < m {
		m = x
	}
	return m
}

func reduceMaxUint32x4(v archsimd.Uint32x4) uint32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	if x := v.GetElem(2); x > m {
		m = x
	}
	if x := v.GetElem(3); x > m {
		m = x
	}
	return m
}

func reduceMinUint64x2(v archsimd.Uint64x2) uint64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	return m
}

func reduceMaxUint64x2(v archsimd.Uint64x2) uint64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	return m
}

func reduceMinFloat32x4(v archsimd.Float32x4) float32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	if x := v.GetElem(2); x < m {
		m = x
	}
	if x := v.GetElem(3); x < m {
		m = x
	}
	return m
}

func reduceMaxFloat32x4(v archsimd.Float32x4) float32 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	if x := v.GetElem(2); x > m {
		m = x
	}
	if x := v.GetElem(3); x > m {
		m = x
	}
	return m
}

func reduceMinFloat64x2(v archsimd.Float64x2) float64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x < m {
		m = x
	}
	return m
}

func reduceMaxFloat64x2(v archsimd.Float64x2) float64 {
	m := v.GetElem(0)
	if x := v.GetElem(1); x > m {
		m = x
	}
	return m
}

func minInt32(data []int32) int32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastInt32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x16Slice(c[0:16])
			v1 := archsimd.LoadInt32x16Slice(c[16:32])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		h := acc0.GetLo().Min(acc0.GetHi())
		q := h.GetLo().Min(h.GetHi())
		m = reduceMinInt32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastInt32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x8Slice(c[0:8])
			v1 := archsimd.LoadInt32x8Slice(c[8:16])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		q := acc0.GetLo().Min(acc0.GetHi())
		m = reduceMinInt32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxInt32(data []int32) int32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastInt32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x16Slice(c[0:16])
			v1 := archsimd.LoadInt32x16Slice(c[16:32])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		h := acc0.GetLo().Max(acc0.GetHi())
		q := h.GetLo().Max(h.GetHi())
		m = reduceMaxInt32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastInt32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x8Slice(c[0:8])
			v1 := archsimd.LoadInt32x8Slice(c[8:16])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		q := acc0.GetLo().Max(acc0.GetHi())
		m = reduceMaxInt32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func minInt64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastInt64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x8Slice(c[0:8])
			v1 := archsimd.LoadInt64x8Slice(c[8:16])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		h := acc0.GetLo().Min(acc0.GetHi())
		q := h.GetLo().Min(h.GetHi())
		m = reduceMinInt64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		acc0 := archsimd.BroadcastInt64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x4Slice(c[0:4])
			v1 := archsimd.LoadInt64x4Slice(c[4:8])
			acc0 = v0.Merge(acc0, acc0.Greater(v0))
			acc1 = v1.Merge(acc1, acc1.Greater(v1))
		}
		acc0 = acc1.Merge(acc0, acc0.Greater(acc1))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetLo().Greater(acc0.GetHi()))
		m = reduceMinInt64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxInt64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastInt64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x8Slice(c[0:8])
			v1 := archsimd.LoadInt64x8Slice(c[8:16])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		h := acc0.GetLo().Max(acc0.GetHi())
		q := h.GetLo().Max(h.GetHi())
		m = reduceMaxInt64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		acc0 := archsimd.BroadcastInt64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x4Slice(c[0:4])
			v1 := archsimd.LoadInt64x4Slice(c[4:8])
			acc0 = acc0.Merge(v0, acc0.Greater(v0))
			acc1 = acc1.Merge(v1, acc1.Greater(v1))
		}
		acc0 = acc0.Merge(acc1, acc0.Greater(acc1))
		q := acc0.GetLo().Merge(acc0.GetHi(), acc0.GetLo().Greater(acc0.GetHi()))
		m = reduceMaxInt64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func minUint32(data []uint32) uint32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastUint32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x16Slice(c[0:16])
			v1 := archsimd.LoadUint32x16Slice(c[16:32])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		h := acc0.GetLo().Min(acc0.GetHi())
		q := h.GetLo().Min(h.GetHi())
		m = reduceMinUint32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastUint32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x8Slice(c[0:8])
			v1 := archsimd.LoadUint32x8Slice(c[8:16])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		q := acc0.GetLo().Min(acc0.GetHi())
		m = reduceMinUint32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxUint32(data []uint32) uint32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastUint32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x16Slice(c[0:16])
			v1 := archsimd.LoadUint32x16Slice(c[16:32])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		h := acc0.GetLo().Max(acc0.GetHi())
		q := h.GetLo().Max(h.GetHi())
		m = reduceMaxUint32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastUint32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x8Slice(c[0:8])
			v1 := archsimd.LoadUint32x8Slice(c[8:16])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		q := acc0.GetLo().Max(acc0.GetHi())
		m = reduceMaxUint32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func minUint64(data []uint64) uint64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastUint64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x8Slice(c[0:8])
			v1 := archsimd.LoadUint64x8Slice(c[8:16])
			acc0 = acc0.Min(v0)
			acc1 = acc1.Min(v1)
		}
		acc0 = acc0.Min(acc1)
		h := acc0.GetLo().Min(acc0.GetHi())
		q := h.GetLo().Min(h.GetHi())
		m = reduceMinUint64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		sign := archsimd.BroadcastUint64x4(1 << 63)
		sign2 := archsimd.BroadcastUint64x2(1 << 63)
		acc0 := archsimd.BroadcastUint64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x4Slice(c[0:4])
			v1 := archsimd.LoadUint64x4Slice(c[4:8])
			acc0 = v0.Merge(acc0, acc0.Xor(sign).AsInt64x4().Greater(v0.Xor(sign).AsInt64x4()))
			acc1 = v1.Merge(acc1, acc1.Xor(sign).AsInt64x4().Greater(v1.Xor(sign).AsInt64x4()))
		}
		acc0 = acc1.Merge(acc0, acc0.Xor(sign).AsInt64x4().Greater(acc1.Xor(sign).AsInt64x4()))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetLo().Xor(sign2).AsInt64x2().Greater(acc0.GetHi().Xor(sign2).AsInt64x2()))
		m = reduceMinUint64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxUint64(data []uint64) uint64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastUint64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x8Slice(c[0:8])
			v1 := archsimd.LoadUint64x8Slice(c[8:16])
			acc0 = acc0.Max(v0)
			acc1 = acc1.Max(v1)
		}
		acc0 = acc0.Max(acc1)
		h := acc0.GetLo().Max(acc0.GetHi())
		q := h.GetLo().Max(h.GetHi())
		m = reduceMaxUint64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		sign := archsimd.BroadcastUint64x4(1 << 63)
		sign2 := archsimd.BroadcastUint64x2(1 << 63)
		acc0 := archsimd.BroadcastUint64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x4Slice(c[0:4])
			v1 := archsimd.LoadUint64x4Slice(c[4:8])
			acc0 = acc0.Merge(v0, acc0.Xor(sign).AsInt64x4().Greater(v0.Xor(sign).AsInt64x4()))
			acc1 = acc1.Merge(v1, acc1.Xor(sign).AsInt64x4().Greater(v1.Xor(sign).AsInt64x4()))
		}
		acc0 = acc0.Merge(acc1, acc0.Xor(sign).AsInt64x4().Greater(acc1.Xor(sign).AsInt64x4()))
		q := acc0.GetLo().Merge(acc0.GetHi(), acc0.GetLo().Xor(sign2).AsInt64x2().Greater(acc0.GetHi().Xor(sign2).AsInt64x2()))
		m = reduceMaxUint64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func minFloat32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastFloat32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x16Slice(c[0:16])
			v1 := archsimd.LoadFloat32x16Slice(c[16:32])
			acc0 = v0.Merge(acc0, v0.Less(acc0))
			acc1 = v1.Merge(acc1, v1.Less(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Less(acc0))
		h := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Less(acc0.GetLo()))
		q := h.GetHi().Merge(h.GetLo(), h.GetHi().Less(h.GetLo()))
		m = reduceMinFloat32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastFloat32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x8Slice(c[0:8])
			v1 := archsimd.LoadFloat32x8Slice(c[8:16])
			acc0 = v0.Merge(acc0, v0.Less(acc0))
			acc1 = v1.Merge(acc1, v1.Less(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Less(acc0))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Less(acc0.GetLo()))
		m = reduceMinFloat32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxFloat32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		acc0 := archsimd.BroadcastFloat32x16(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[32]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x16Slice(c[0:16])
			v1 := archsimd.LoadFloat32x16Slice(c[16:32])
			acc0 = v0.Merge(acc0, v0.Greater(acc0))
			acc1 = v1.Merge(acc1, v1.Greater(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Greater(acc0))
		h := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Greater(acc0.GetLo()))
		q := h.GetHi().Merge(h.GetLo(), h.GetHi().Greater(h.GetLo()))
		m = reduceMaxFloat32x4(q)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		acc0 := archsimd.BroadcastFloat32x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x8Slice(c[0:8])
			v1 := archsimd.LoadFloat32x8Slice(c[8:16])
			acc0 = v0.Merge(acc0, v0.Greater(acc0))
			acc1 = v1.Merge(acc1, v1.Greater(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Greater(acc0))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Greater(acc0.GetLo()))
		m = reduceMaxFloat32x4(q)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func minFloat64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastFloat64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x8Slice(c[0:8])
			v1 := archsimd.LoadFloat64x8Slice(c[8:16])
			acc0 = v0.Merge(acc0, v0.Less(acc0))
			acc1 = v1.Merge(acc1, v1.Less(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Less(acc0))
		h := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Less(acc0.GetLo()))
		q := h.GetHi().Merge(h.GetLo(), h.GetHi().Less(h.GetLo()))
		m = reduceMinFloat64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		acc0 := archsimd.BroadcastFloat64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x4Slice(c[0:4])
			v1 := archsimd.LoadFloat64x4Slice(c[4:8])
			acc0 = v0.Merge(acc0, v0.Less(acc0))
			acc1 = v1.Merge(acc1, v1.Less(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Less(acc0))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Less(acc0.GetLo()))
		m = reduceMinFloat64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < m {
			m = v
		}
	}
	return m
}

func maxFloat64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	m := data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		acc0 := archsimd.BroadcastFloat64x8(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[16]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x8Slice(c[0:8])
			v1 := archsimd.LoadFloat64x8Slice(c[8:16])
			acc0 = v0.Merge(acc0, v0.Greater(acc0))
			acc1 = v1.Merge(acc1, v1.Greater(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Greater(acc0))
		h := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Greater(acc0.GetLo()))
		q := h.GetHi().Merge(h.GetLo(), h.GetHi().Greater(h.GetLo()))
		m = reduceMaxFloat64x2(q)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		acc0 := archsimd.BroadcastFloat64x4(m)
		acc1 := acc0
		chunks := unsafecast.Slice[[8]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x4Slice(c[0:4])
			v1 := archsimd.LoadFloat64x4Slice(c[4:8])
			acc0 = v0.Merge(acc0, v0.Greater(acc0))
			acc1 = v1.Merge(acc1, v1.Greater(acc1))
		}
		acc0 = acc1.Merge(acc0, acc1.Greater(acc0))
		q := acc0.GetHi().Merge(acc0.GetLo(), acc0.GetHi().Greater(acc0.GetLo()))
		m = reduceMaxFloat64x2(q)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v > m {
			m = v
		}
	}
	return m
}

func boundsInt32(data []int32) (min, max int32) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		minAcc0 := archsimd.BroadcastInt32x16(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[32]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x16Slice(c[0:16])
			v1 := archsimd.LoadInt32x16Slice(c[16:32])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minH := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxH := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		minQ := minH.GetLo().Min(minH.GetHi())
		maxQ := maxH.GetLo().Max(maxH.GetHi())
		min = reduceMinInt32x4(minQ)
		max = reduceMaxInt32x4(maxQ)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastInt32x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]int32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt32x8Slice(c[0:8])
			v1 := archsimd.LoadInt32x8Slice(c[8:16])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minQ := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxQ := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		min = reduceMinInt32x4(minQ)
		max = reduceMaxInt32x4(maxQ)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func boundsInt64(data []int64) (min, max int64) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastInt64x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x8Slice(c[0:8])
			v1 := archsimd.LoadInt64x8Slice(c[8:16])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minH := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxH := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		minQ := minH.GetLo().Min(minH.GetHi())
		maxQ := maxH.GetLo().Max(maxH.GetHi())
		min = reduceMinInt64x2(minQ)
		max = reduceMaxInt64x2(maxQ)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		minAcc0 := archsimd.BroadcastInt64x4(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[8]int64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadInt64x4Slice(c[0:4])
			v1 := archsimd.LoadInt64x4Slice(c[4:8])
			minAcc0 = v0.Merge(minAcc0, minAcc0.Greater(v0))
			minAcc1 = v1.Merge(minAcc1, minAcc1.Greater(v1))
			maxAcc0 = maxAcc0.Merge(v0, maxAcc0.Greater(v0))
			maxAcc1 = maxAcc1.Merge(v1, maxAcc1.Greater(v1))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc0.Greater(minAcc1))
		maxAcc0 = maxAcc0.Merge(maxAcc1, maxAcc0.Greater(maxAcc1))
		minQ := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetLo().Greater(minAcc0.GetHi()))
		maxQ := maxAcc0.GetLo().Merge(maxAcc0.GetHi(), maxAcc0.GetLo().Greater(maxAcc0.GetHi()))
		min = reduceMinInt64x2(minQ)
		max = reduceMaxInt64x2(maxQ)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func boundsUint32(data []uint32) (min, max uint32) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		minAcc0 := archsimd.BroadcastUint32x16(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[32]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x16Slice(c[0:16])
			v1 := archsimd.LoadUint32x16Slice(c[16:32])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minH := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxH := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		minQ := minH.GetLo().Min(minH.GetHi())
		maxQ := maxH.GetLo().Max(maxH.GetHi())
		min = reduceMinUint32x4(minQ)
		max = reduceMaxUint32x4(maxQ)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastUint32x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]uint32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint32x8Slice(c[0:8])
			v1 := archsimd.LoadUint32x8Slice(c[8:16])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minQ := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxQ := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		min = reduceMinUint32x4(minQ)
		max = reduceMaxUint32x4(maxQ)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func boundsUint64(data []uint64) (min, max uint64) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastUint64x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x8Slice(c[0:8])
			v1 := archsimd.LoadUint64x8Slice(c[8:16])
			minAcc0 = minAcc0.Min(v0)
			minAcc1 = minAcc1.Min(v1)
			maxAcc0 = maxAcc0.Max(v0)
			maxAcc1 = maxAcc1.Max(v1)
		}
		minAcc0 = minAcc0.Min(minAcc1)
		maxAcc0 = maxAcc0.Max(maxAcc1)
		minH := minAcc0.GetLo().Min(minAcc0.GetHi())
		maxH := maxAcc0.GetLo().Max(maxAcc0.GetHi())
		minQ := minH.GetLo().Min(minH.GetHi())
		maxQ := maxH.GetLo().Max(maxH.GetHi())
		min = reduceMinUint64x2(minQ)
		max = reduceMaxUint64x2(maxQ)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		sign := archsimd.BroadcastUint64x4(1 << 63)
		sign2 := archsimd.BroadcastUint64x2(1 << 63)
		minAcc0 := archsimd.BroadcastUint64x4(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[8]uint64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadUint64x4Slice(c[0:4])
			v1 := archsimd.LoadUint64x4Slice(c[4:8])
			minAcc0 = v0.Merge(minAcc0, minAcc0.Xor(sign).AsInt64x4().Greater(v0.Xor(sign).AsInt64x4()))
			minAcc1 = v1.Merge(minAcc1, minAcc1.Xor(sign).AsInt64x4().Greater(v1.Xor(sign).AsInt64x4()))
			maxAcc0 = maxAcc0.Merge(v0, maxAcc0.Xor(sign).AsInt64x4().Greater(v0.Xor(sign).AsInt64x4()))
			maxAcc1 = maxAcc1.Merge(v1, maxAcc1.Xor(sign).AsInt64x4().Greater(v1.Xor(sign).AsInt64x4()))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc0.Xor(sign).AsInt64x4().Greater(minAcc1.Xor(sign).AsInt64x4()))
		maxAcc0 = maxAcc0.Merge(maxAcc1, maxAcc0.Xor(sign).AsInt64x4().Greater(maxAcc1.Xor(sign).AsInt64x4()))
		minQ := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetLo().Xor(sign2).AsInt64x2().Greater(minAcc0.GetHi().Xor(sign2).AsInt64x2()))
		maxQ := maxAcc0.GetLo().Merge(maxAcc0.GetHi(), maxAcc0.GetLo().Xor(sign2).AsInt64x2().Greater(maxAcc0.GetHi().Xor(sign2).AsInt64x2()))
		min = reduceMinUint64x2(minQ)
		max = reduceMaxUint64x2(maxQ)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func boundsFloat32(data []float32) (min, max float32) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 32:
		minAcc0 := archsimd.BroadcastFloat32x16(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[32]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x16Slice(c[0:16])
			v1 := archsimd.LoadFloat32x16Slice(c[16:32])
			minAcc0 = v0.Merge(minAcc0, v0.Less(minAcc0))
			minAcc1 = v1.Merge(minAcc1, v1.Less(minAcc1))
			maxAcc0 = v0.Merge(maxAcc0, v0.Greater(maxAcc0))
			maxAcc1 = v1.Merge(maxAcc1, v1.Greater(maxAcc1))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc1.Less(minAcc0))
		maxAcc0 = maxAcc1.Merge(maxAcc0, maxAcc1.Greater(maxAcc0))
		minH := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetHi().Less(minAcc0.GetLo()))
		maxH := maxAcc0.GetHi().Merge(maxAcc0.GetLo(), maxAcc0.GetHi().Greater(maxAcc0.GetLo()))
		minQ := minH.GetHi().Merge(minH.GetLo(), minH.GetHi().Less(minH.GetLo()))
		maxQ := maxH.GetHi().Merge(maxH.GetLo(), maxH.GetHi().Greater(maxH.GetLo()))
		min = reduceMinFloat32x4(minQ)
		max = reduceMaxFloat32x4(maxQ)
		d = d[len(chunks)*32:]
	case archsimd.X86.AVX2() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastFloat32x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]float32](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat32x8Slice(c[0:8])
			v1 := archsimd.LoadFloat32x8Slice(c[8:16])
			minAcc0 = v0.Merge(minAcc0, v0.Less(minAcc0))
			minAcc1 = v1.Merge(minAcc1, v1.Less(minAcc1))
			maxAcc0 = v0.Merge(maxAcc0, v0.Greater(maxAcc0))
			maxAcc1 = v1.Merge(maxAcc1, v1.Greater(maxAcc1))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc1.Less(minAcc0))
		maxAcc0 = maxAcc1.Merge(maxAcc0, maxAcc1.Greater(maxAcc0))
		minQ := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetHi().Less(minAcc0.GetLo()))
		maxQ := maxAcc0.GetHi().Merge(maxAcc0.GetLo(), maxAcc0.GetHi().Greater(maxAcc0.GetLo()))
		min = reduceMinFloat32x4(minQ)
		max = reduceMaxFloat32x4(maxQ)
		d = d[len(chunks)*16:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func boundsFloat64(data []float64) (min, max float64) {
	if len(data) == 0 {
		return 0, 0
	}
	min = data[0]
	max = data[0]
	d := data
	switch {
	case archsimd.X86.AVX512() && len(d) >= 16:
		minAcc0 := archsimd.BroadcastFloat64x8(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[16]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x8Slice(c[0:8])
			v1 := archsimd.LoadFloat64x8Slice(c[8:16])
			minAcc0 = v0.Merge(minAcc0, v0.Less(minAcc0))
			minAcc1 = v1.Merge(minAcc1, v1.Less(minAcc1))
			maxAcc0 = v0.Merge(maxAcc0, v0.Greater(maxAcc0))
			maxAcc1 = v1.Merge(maxAcc1, v1.Greater(maxAcc1))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc1.Less(minAcc0))
		maxAcc0 = maxAcc1.Merge(maxAcc0, maxAcc1.Greater(maxAcc0))
		minH := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetHi().Less(minAcc0.GetLo()))
		maxH := maxAcc0.GetHi().Merge(maxAcc0.GetLo(), maxAcc0.GetHi().Greater(maxAcc0.GetLo()))
		minQ := minH.GetHi().Merge(minH.GetLo(), minH.GetHi().Less(minH.GetLo()))
		maxQ := maxH.GetHi().Merge(maxH.GetLo(), maxH.GetHi().Greater(maxH.GetLo()))
		min = reduceMinFloat64x2(minQ)
		max = reduceMaxFloat64x2(maxQ)
		d = d[len(chunks)*16:]
	case archsimd.X86.AVX2() && len(d) >= 8:
		minAcc0 := archsimd.BroadcastFloat64x4(min)
		minAcc1 := minAcc0
		maxAcc0 := minAcc0
		maxAcc1 := minAcc0
		chunks := unsafecast.Slice[[8]float64](d)
		for i := range chunks {
			c := &chunks[i]
			v0 := archsimd.LoadFloat64x4Slice(c[0:4])
			v1 := archsimd.LoadFloat64x4Slice(c[4:8])
			minAcc0 = v0.Merge(minAcc0, v0.Less(minAcc0))
			minAcc1 = v1.Merge(minAcc1, v1.Less(minAcc1))
			maxAcc0 = v0.Merge(maxAcc0, v0.Greater(maxAcc0))
			maxAcc1 = v1.Merge(maxAcc1, v1.Greater(maxAcc1))
		}
		minAcc0 = minAcc1.Merge(minAcc0, minAcc1.Less(minAcc0))
		maxAcc0 = maxAcc1.Merge(maxAcc0, maxAcc1.Greater(maxAcc0))
		minQ := minAcc0.GetHi().Merge(minAcc0.GetLo(), minAcc0.GetHi().Less(minAcc0.GetLo()))
		maxQ := maxAcc0.GetHi().Merge(maxAcc0.GetLo(), maxAcc0.GetHi().Greater(maxAcc0.GetLo()))
		min = reduceMinFloat64x2(minQ)
		max = reduceMaxFloat64x2(maxQ)
		d = d[len(chunks)*8:]
	}
	for _, v := range d {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

// The big endian 128 bits kernels are scalar; vectorizing them is complex
// (lexicographic compare with index tracking) and left for a later tier.

func minBE128(data [][16]byte) (min []byte) {
	if len(data) > 0 {
		m := binary.BigEndian.Uint64(data[0][:8])
		j := 0
		for i := 1; i < len(data); i++ {
			x := binary.BigEndian.Uint64(data[i][:8])
			switch {
			case x < m:
				m, j = x, i
			case x == m:
				y := binary.BigEndian.Uint64(data[i][8:])
				n := binary.BigEndian.Uint64(data[j][8:])
				if y < n {
					m, j = x, i
				}
			}
		}
		min = data[j][:]
	}
	return min
}

func maxBE128(data [][16]byte) (max []byte) {
	if len(data) > 0 {
		m := binary.BigEndian.Uint64(data[0][:8])
		j := 0
		for i := 1; i < len(data); i++ {
			x := binary.BigEndian.Uint64(data[i][:8])
			switch {
			case x > m:
				m, j = x, i
			case x == m:
				y := binary.BigEndian.Uint64(data[i][8:])
				n := binary.BigEndian.Uint64(data[j][8:])
				if y > n {
					m, j = x, i
				}
			}
		}
		max = data[j][:]
	}
	return max
}

func boundsBE128(data [][16]byte) (min, max []byte) {
	min = minBE128(data)
	max = maxBE128(data)
	return min, max
}
