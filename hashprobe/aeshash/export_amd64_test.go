//go:build !purego && amd64

package aeshash

// testingInitAesKeySched initializes the aes seed to a deterministic value for
// test purposes.
func testingInitAesKeySched() {
	for i := range aeskeysched {
		aeskeysched[i] = byte(i)
	}
}
