package lz4

import (
	"math"
	"testing"
)

func TestMaxDecodedLen(t *testing.T) {
	samples := []struct {
		n, want int
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 19},
		{5, 274},
		{6, 529},
		{1000, 255*1000 - 1001},
	}
	for _, sample := range samples {
		if got := maxDecodedLen(sample.n); got != sample.want {
			t.Fatalf("maxDecodedLen(%d) = %d, want %d", sample.n, got, sample.want)
		}
	}

	if got := maxDecodedLen(math.MaxInt); got != math.MaxInt {
		t.Fatalf("maxDecodedLen(MaxInt) = %d, want MaxInt", got)
	}
	if got := maxDecodedLen(math.MaxInt/255 + 1); got != math.MaxInt {
		t.Fatalf("maxDecodedLen just past the overflow point = %d, want MaxInt", got)
	}
}
