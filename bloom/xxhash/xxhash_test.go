package xxhash_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/bloom/xxhash"
)

func TestSum64(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  uint64
	}{
		{"empty", "", 0xef46db3751d8e999},
		{"a", "a", 0xd24ec4f1a98c6e5b},
		{"as", "as", 0x1c330fb2d66be179},
		{"asd", "asd", 0x631c37ce72a97393},
		{"asdf", "asdf", 0x415872f599cea71e},
		{
			"len=63",
			// Exactly 63 characters, which exercises all code paths.
			"Call me Ishmael. Some years ago--never mind how long precisely-",
			0x02a2e85470d6fd96,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := xxhash.Sum64([]byte(tt.input)); got != tt.want {
				t.Fatalf("Sum64: got 0x%x; want 0x%x", got, tt.want)
			}
		})
	}
}

var benchmarks = []struct {
	name string
	n    int64
}{
	{"4B", 4},
	{"16B", 16},
	{"100B", 100},
	{"4KB", 4e3},
	{"10MB", 10e6},
}

func BenchmarkSum64(b *testing.B) {
	for _, bb := range benchmarks {
		in := make([]byte, bb.n)
		for i := range in {
			in[i] = byte(i)
		}
		b.Run(bb.name, func(b *testing.B) {
			b.SetBytes(bb.n)
			for b.Loop() {
				_ = xxhash.Sum64(in)
			}
		})
	}
}
