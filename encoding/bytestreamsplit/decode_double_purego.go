//go:build purego && goexperiment.simd && go1.27

package bytestreamsplit

import "encoding/binary"

// purego cannot link decode_double_amd64.s. The scalar gather is the same
// tail the assembly uses below its AVX-512 loop.

func decodeDouble(dst, src []byte) {
	n := len(src) / 8
	if n == 0 {
		return
	}
	b0 := src[0*n : 1*n]
	b1 := src[1*n : 2*n]
	b2 := src[2*n : 3*n]
	b3 := src[3*n : 4*n]
	b4 := src[4*n : 5*n]
	b5 := src[5*n : 6*n]
	b6 := src[6*n : 7*n]
	b7 := src[7*n : 8*n]
	for j := range n {
		binary.LittleEndian.PutUint64(dst[8*j:], uint64(b0[j])|
			uint64(b1[j])<<8|
			uint64(b2[j])<<16|
			uint64(b3[j])<<24|
			uint64(b4[j])<<32|
			uint64(b5[j])<<40|
			uint64(b6[j])<<48|
			uint64(b7[j])<<56)
	}
}
