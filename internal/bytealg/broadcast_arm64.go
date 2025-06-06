//go:build !purego && arm64

package bytealg

//go:noescape
func broadcastNEON(dst []byte, src byte)

// Broadcast writes the src value to all bytes of dst.
func Broadcast(dst []byte, src byte) {
	if len(dst) >= 8 {
		broadcastNEON(dst, src)
	} else {
		for i := range dst {
			dst[i] = src
		}
	}
}