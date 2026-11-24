//go:build !amd64 || (purego && (!goexperiment.simd || go1.27))

package aeshash

func testingInitAesKeySched() {}
