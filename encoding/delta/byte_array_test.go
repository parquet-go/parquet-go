package delta

import (
	"bytes"
	"fmt"
	"testing"
)

func TestLinearSearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, linearSearchPrefixLength)
}

func TestBinarySearchPrefixLength(t *testing.T) {
	testSearchPrefixLength(t, func(base, data []byte) int {
		return binarySearchPrefixLength(base, data)
	})
}

func testSearchPrefixLength(t *testing.T, prefixLength func(base, data []byte) int) {
	tests := []struct {
		base string
		data string
		len  int
	}{
		{
			base: "",
			data: "",
			len:  0,
		},

		{
			base: "A",
			data: "B",
			len:  0,
		},

		{
			base: "",
			data: "Hello World!",
			len:  0,
		},

		{
			base: "H",
			data: "Hello World!",
			len:  1,
		},

		{
			base: "He",
			data: "Hello World!",
			len:  2,
		},

		{
			base: "Hel",
			data: "Hello World!",
			len:  3,
		},

		{
			base: "Hell",
			data: "Hello World!",
			len:  4,
		},

		{
			base: "Hello",
			data: "Hello World!",
			len:  5,
		},

		{
			base: "Hello ",
			data: "Hello World!",
			len:  6,
		},

		{
			base: "Hello W",
			data: "Hello World!",
			len:  7,
		},

		{
			base: "Hello Wo",
			data: "Hello World!",
			len:  8,
		},

		{
			base: "Hello Wor",
			data: "Hello World!",
			len:  9,
		},

		{
			base: "Hello Worl",
			data: "Hello World!",
			len:  10,
		},

		{
			base: "Hello World",
			data: "Hello World!",
			len:  11,
		},

		{
			base: "Hello World!",
			data: "Hello World!",
			len:  12,
		},

		{
			base: "Hell.......",
			data: "Hello World!",
			len:  4,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			n := prefixLength([]byte(test.base), []byte(test.data))
			if n != test.len {
				t.Errorf("prefixLength(%q,%q): want=%d got=%d", test.base, test.data, test.len, n)
			}
		})
	}
}

func BenchmarkLinearSearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, linearSearchPrefixLength)
}

func BenchmarkBinarySearchPrefixLength(b *testing.B) {
	benchmarkSearchPrefixLength(b, func(base, data []byte) int {
		return binarySearchPrefixLength(base, data)
	})
}

func benchmarkSearchPrefixLength(b *testing.B, prefixLength func(base, data []byte) int) {
	buffer := bytes.Repeat([]byte("0123456789"), 100)

	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			base := buffer[:size]
			data := buffer[:size/2]

			for b.Loop() {
				_ = prefixLength(base, data)
			}
		})
	}
}
