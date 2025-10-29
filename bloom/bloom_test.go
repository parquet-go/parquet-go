package bloom

import (
	"math/rand"
	"testing"
)

// Test file for internal functions of the bloom package.
var global4x64 [4]uint64

func TestFasthash(t *testing.T) {
	r := rand.NewSource(0).(rand.Source64)

	src := [4]uint64{r.Uint64(), r.Uint64(), r.Uint64(), r.Uint64()}
	dst := [4]uint64{0, 0, 0, 0}
	exp := [4]uint64{483, 125, 335, 539}
	mod := int32(1024)

	fasthash4x64(&dst, &src, mod)

	if dst != exp {
		t.Errorf("got=%v want=%v", dst, exp)
	}
}

func BenchmarkFasthash(b *testing.B) {
	src := [4]uint64{}
	dst := [4]uint64{}
	mod := int32(1024)

	for b.Loop() {
		fasthash4x64(&dst, &src, mod)
	}

	b.SetBytes(32)
	global4x64 = dst // use it so the loop isn't optimized away
}
