//go:build !purego && goexperiment.simd && go1.27

package delta

// decodeByteArrayAVX2 is the assembly reconstruction. The archsimd copy
// loop was slower on the prefix-heavy page. The declaration stays out of
// the purego build, which supplies a Go body instead.

//go:noescape
func decodeByteArrayAVX2(dst, src []byte, prefix, suffix []int32) int
