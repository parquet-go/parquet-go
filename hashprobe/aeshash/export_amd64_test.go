//go:build amd64 && (!purego || (goexperiment.simd && !go1.27))

package aeshash

// testingInitAesKeySched initializes the aes seed to a deterministic value for
// test purposes.
func testingInitAesKeySched() {
	for i := range aeskeysched {
		aeskeysched[i] = byte(i)
	}
}
