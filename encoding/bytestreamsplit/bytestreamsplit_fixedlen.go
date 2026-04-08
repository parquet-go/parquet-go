package bytestreamsplit

func encodeFixedLenByteArray(dst, src []byte, size int) {
	switch size {
	case 4:
		encodeFloat(dst, src)
	case 8:
		encodeDouble(dst, src)
	default:
		n := len(src) / size
		for s := range size {
			stream := dst[s*n : (s+1)*n]
			for i := range n {
				stream[i] = src[i*size+s]
			}
		}
	}
}

func decodeFixedLenByteArray(dst, src []byte, size int) {
	switch size {
	case 4:
		decodeFloat(dst, src)
	case 8:
		decodeDouble(dst, src)
	default:
		n := len(src) / size
		for s := range size {
			stream := src[s*n : (s+1)*n]
			for i := range n {
				dst[i*size+s] = stream[i]
			}
		}
	}
}
