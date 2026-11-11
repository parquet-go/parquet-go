package parquet

import (
	"fmt"
	"testing"
)

func TestFindRowStart(t *testing.T) {
	for _, size := range []int{0, 1, 255, 256, 257, 511, 512, 513} {
		for _, period := range []int{0, 1, 3, 256} {
			t.Run(fmt.Sprintf("size=%d/period=%d", size, period), func(t *testing.T) {
				levels := make([]byte, size)
				for i := range levels {
					levels[i] = 1
					if period > 0 && i%period == 0 {
						levels[i] = 0
					}
				}

				numRows := countLevelsEqual(levels, 0)
				for row := 0; row <= numRows; row++ {
					want := scalarFindRowStart(levels, int64(row))
					if got := findRowStart(levels, int64(row)); got != want {
						t.Fatalf("row %d: got index %d, want %d", row, got, want)
					}
				}
			})
		}
	}
}

func scalarFindRowStart(levels []byte, rowIndex int64) int {
	for i, level := range levels {
		if level == 0 {
			if rowIndex == 0 {
				return i
			}
			rowIndex--
		}
	}
	return len(levels)
}

func BenchmarkFindRowStart(b *testing.B) {
	levels := make([]byte, 1024*1024)
	for i := range levels {
		levels[i] = 1
		if i%4 == 0 {
			levels[i] = 0
		}
	}

	implementations := []struct {
		name string
		fn   func([]byte, int64) int
	}{
		{"scalar", scalarFindRowStart},
		{"blocked", findRowStart},
	}
	for _, implementation := range implementations {
		for _, row := range []int64{0, 64, 1024, 262144} {
			b.Run(fmt.Sprintf("%s/row=%d", implementation.name, row), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = implementation.fn(levels, row)
				}
			})
		}
	}
}
