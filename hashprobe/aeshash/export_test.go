//go:build !amd64 || (purego && !goexperiment.simd)

package aeshash

func testingInitAesKeySched() {}
