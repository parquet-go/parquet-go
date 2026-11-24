//go:build !amd64 || !goexperiment.simd || go1.27

package delta

func searchPrefixLength(base, data []byte) int {
	return wordSearchPrefixLength(base, data)
}
