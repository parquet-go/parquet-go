//go:build goexperiment.simd

package parquet

import "simd/archsimd"

// This file provides implementations of the orderOf kernels based on the
// simd/archsimd package, replacing the hand-written assembly of
// order_amd64.s when GOEXPERIMENT=simd is set. Unlike the assembly, which
// only had AVX-512 paths, these implementations also provide AVX2 paths.
//
// The kernels detect whether values are in ascending (+1), descending (-1)
// or undefined (0) order by comparing each vector against the same vector
// loaded one element ahead. Floating point sequences containing NaN report
// undefined order: the LessEqual/GreaterEqual comparisons are false for NaN
// lanes, failing both scans (matching the assembly's VCMPPS predicates; the
// purego generic treats NaN pairs as ordered instead).
//
// The AVX2 tier of the integer kernels tests "no lane greater" instead of
// "all lanes less-or-equal": integer LessEqual and unsigned comparisons only
// exist as EVEX encodings, while signed VPCMPGTD/VPCMPGTQ are available in
// AVX2; the unsigned variants bias both operands by the sign bit before
// comparing.

func orderAscendingInt32(data []int32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadInt32x16Slice(data[i:])
			b := archsimd.LoadInt32x16Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadInt32x8Slice(data[i:])
			b := archsimd.LoadInt32x8Slice(data[i+1:])
			if a.Greater(b).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

func orderDescendingInt32(data []int32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadInt32x16Slice(data[i:])
			b := archsimd.LoadInt32x16Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadInt32x8Slice(data[i:])
			b := archsimd.LoadInt32x8Slice(data[i+1:])
			if b.Greater(a).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] < data[i+1] {
			return false
		}
	}
	return true
}

func orderAscendingInt64(data []int64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadInt64x8Slice(data[i:])
			b := archsimd.LoadInt64x8Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadInt64x4Slice(data[i:])
			b := archsimd.LoadInt64x4Slice(data[i+1:])
			if a.Greater(b).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

func orderDescendingInt64(data []int64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadInt64x8Slice(data[i:])
			b := archsimd.LoadInt64x8Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadInt64x4Slice(data[i:])
			b := archsimd.LoadInt64x4Slice(data[i+1:])
			if b.Greater(a).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] < data[i+1] {
			return false
		}
	}
	return true
}

func orderAscendingUint32(data []uint32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadUint32x16Slice(data[i:])
			b := archsimd.LoadUint32x16Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		sign := archsimd.BroadcastUint32x8(1 << 31)
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadUint32x8Slice(data[i:])
			b := archsimd.LoadUint32x8Slice(data[i+1:])
			if a.Xor(sign).AsInt32x8().Greater(b.Xor(sign).AsInt32x8()).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

func orderDescendingUint32(data []uint32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadUint32x16Slice(data[i:])
			b := archsimd.LoadUint32x16Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		sign := archsimd.BroadcastUint32x8(1 << 31)
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadUint32x8Slice(data[i:])
			b := archsimd.LoadUint32x8Slice(data[i+1:])
			if b.Xor(sign).AsInt32x8().Greater(a.Xor(sign).AsInt32x8()).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] < data[i+1] {
			return false
		}
	}
	return true
}

func orderAscendingUint64(data []uint64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadUint64x8Slice(data[i:])
			b := archsimd.LoadUint64x8Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		sign := archsimd.BroadcastUint64x4(1 << 63)
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadUint64x4Slice(data[i:])
			b := archsimd.LoadUint64x4Slice(data[i+1:])
			if a.Xor(sign).AsInt64x4().Greater(b.Xor(sign).AsInt64x4()).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

func orderDescendingUint64(data []uint64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadUint64x8Slice(data[i:])
			b := archsimd.LoadUint64x8Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		sign := archsimd.BroadcastUint64x4(1 << 63)
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadUint64x4Slice(data[i:])
			b := archsimd.LoadUint64x4Slice(data[i+1:])
			if b.Xor(sign).AsInt64x4().Greater(a.Xor(sign).AsInt64x4()).ToBits() != 0 {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if data[i] < data[i+1] {
			return false
		}
	}
	return true
}

func orderAscendingFloat32(data []float32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadFloat32x16Slice(data[i:])
			b := archsimd.LoadFloat32x16Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadFloat32x8Slice(data[i:])
			b := archsimd.LoadFloat32x8Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xff {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if !(data[i] <= data[i+1]) {
			return false
		}
	}
	return true
}

func orderDescendingFloat32(data []float32) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 17:
		for ; i+17 <= len(data); i += 16 {
			a := archsimd.LoadFloat32x16Slice(data[i:])
			b := archsimd.LoadFloat32x16Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xffff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadFloat32x8Slice(data[i:])
			b := archsimd.LoadFloat32x8Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xff {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if !(data[i] >= data[i+1]) {
			return false
		}
	}
	return true
}

func orderAscendingFloat64(data []float64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadFloat64x8Slice(data[i:])
			b := archsimd.LoadFloat64x8Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadFloat64x4Slice(data[i:])
			b := archsimd.LoadFloat64x4Slice(data[i+1:])
			if a.LessEqual(b).ToBits() != 0xf {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if !(data[i] <= data[i+1]) {
			return false
		}
	}
	return true
}

func orderDescendingFloat64(data []float64) bool {
	i := 0
	switch {
	case archsimd.X86.AVX512() && len(data) >= 9:
		for ; i+9 <= len(data); i += 8 {
			a := archsimd.LoadFloat64x8Slice(data[i:])
			b := archsimd.LoadFloat64x8Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xff {
				return false
			}
		}
	case archsimd.X86.AVX2() && len(data) >= 5:
		for ; i+5 <= len(data); i += 4 {
			a := archsimd.LoadFloat64x4Slice(data[i:])
			b := archsimd.LoadFloat64x4Slice(data[i+1:])
			if a.GreaterEqual(b).ToBits() != 0xf {
				return false
			}
		}
	}
	for ; i+1 < len(data); i++ {
		if !(data[i] >= data[i+1]) {
			return false
		}
	}
	return true
}

func orderOfInt32(data []int32) int {
	if len(data) > 1 {
		if orderAscendingInt32(data) {
			return +1
		}
		if orderDescendingInt32(data) {
			return -1
		}
	}
	return 0
}

func orderOfInt64(data []int64) int {
	if len(data) > 1 {
		if orderAscendingInt64(data) {
			return +1
		}
		if orderDescendingInt64(data) {
			return -1
		}
	}
	return 0
}

func orderOfUint32(data []uint32) int {
	if len(data) > 1 {
		if orderAscendingUint32(data) {
			return +1
		}
		if orderDescendingUint32(data) {
			return -1
		}
	}
	return 0
}

func orderOfUint64(data []uint64) int {
	if len(data) > 1 {
		if orderAscendingUint64(data) {
			return +1
		}
		if orderDescendingUint64(data) {
			return -1
		}
	}
	return 0
}

func orderOfFloat32(data []float32) int {
	if len(data) > 1 {
		if orderAscendingFloat32(data) {
			return +1
		}
		if orderDescendingFloat32(data) {
			return -1
		}
	}
	return 0
}

func orderOfFloat64(data []float64) int {
	if len(data) > 1 {
		if orderAscendingFloat64(data) {
			return +1
		}
		if orderDescendingFloat64(data) {
			return -1
		}
	}
	return 0
}
