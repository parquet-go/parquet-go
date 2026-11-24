//go:build !amd64 || (purego && (!goexperiment.simd || go1.27))

package bytealg

func Broadcast(dst []byte, src byte) {
	for i := range dst {
		dst[i] = src
	}
}
