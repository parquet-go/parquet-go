//go:build !purego && goexperiment.simd

package sparse

// The assembly versions of these kernels are scalar loops; the Go
// implementations below compile to comparable code, so the GOEXPERIMENT=simd
// build uses them instead of the assembly. The AVX2 gather kernels keep
// their assembly (no gather in archsimd).

func gatherBitsDefault(dst []byte, src Uint8Array) {
	n := src.Len() / 8
	for j := range n {
		i := j * 8
		dst[j] = (src.Index(i+0) & 1) |
			((src.Index(i+1) & 1) << 1) |
			((src.Index(i+2) & 1) << 2) |
			((src.Index(i+3) & 1) << 3) |
			((src.Index(i+4) & 1) << 4) |
			((src.Index(i+5) & 1) << 5) |
			((src.Index(i+6) & 1) << 6) |
			((src.Index(i+7) & 1) << 7)
	}
}

func gather128(dst [][16]byte, src Uint128Array) int {
	n := min(len(dst), src.Len())
	for i := range dst[:n] {
		dst[i] = src.Index(i)
	}
	return n
}
