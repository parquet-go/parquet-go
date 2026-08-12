//go:build amd64 && !purego

package parquet

import (
	"math"
	"testing"
)

// The float min/max/bounds kernels must ignore NaN values appearing after
// the first element: floatPage.Bounds skips leading NaNs and relies on the
// kernels for interior ones. The purego kernels (slices.Min/Max) propagate
// NaN instead, which is why this test only covers the amd64 assembly and
// archsimd implementations. The cases in page_bounds_nan_test.go use arrays
// small enough to be handled entirely by the scalar tails; these use sizes
// large enough to exercise the vector paths of every implementation.
func TestFloatBoundsVectorNaN(t *testing.T) {
	const size = 1000
	nan32 := float32(math.NaN())
	nan64 := math.NaN()

	f32 := make([]float32, size)
	f64 := make([]float64, size)
	for i := range f32 {
		f32[i] = float32(i%997) - 498.5
		f64[i] = float64(i%997) - 498.5
		if i > 0 && i%7 == 0 {
			f32[i] = nan32
			f64[i] = nan64
		}
	}

	wantMin32, wantMax32 := f32[0], f32[0]
	for _, v := range f32[1:] {
		if v < wantMin32 {
			wantMin32 = v
		}
		if v > wantMax32 {
			wantMax32 = v
		}
	}
	wantMin64, wantMax64 := f64[0], f64[0]
	for _, v := range f64[1:] {
		if v < wantMin64 {
			wantMin64 = v
		}
		if v > wantMax64 {
			wantMax64 = v
		}
	}

	if got := minFloat32(f32); got != wantMin32 {
		t.Errorf("minFloat32 = %v, want %v", got, wantMin32)
	}
	if got := maxFloat32(f32); got != wantMax32 {
		t.Errorf("maxFloat32 = %v, want %v", got, wantMax32)
	}
	if gotMin, gotMax := boundsFloat32(f32); gotMin != wantMin32 || gotMax != wantMax32 {
		t.Errorf("boundsFloat32 = (%v, %v), want (%v, %v)", gotMin, gotMax, wantMin32, wantMax32)
	}
	if got := minFloat64(f64); got != wantMin64 {
		t.Errorf("minFloat64 = %v, want %v", got, wantMin64)
	}
	if got := maxFloat64(f64); got != wantMax64 {
		t.Errorf("maxFloat64 = %v, want %v", got, wantMax64)
	}
	if gotMin, gotMax := boundsFloat64(f64); gotMin != wantMin64 || gotMax != wantMax64 {
		t.Errorf("boundsFloat64 = (%v, %v), want (%v, %v)", gotMin, gotMax, wantMin64, wantMax64)
	}
}
