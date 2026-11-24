//go:build purego && goexperiment.simd && go1.27

package rle

func useBytesBitpackBMI2() {}
