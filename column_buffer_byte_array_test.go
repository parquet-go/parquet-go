package parquet

import (
	"io"
	"math"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/plain"
)

// TestColumnBufferByteArrayWriteTypes tests that byteArrayColumnBuffer
// correctly converts various types to text representation
func TestColumnBufferByteArrayWriteTypes(t *testing.T) {
	tests := []struct {
		name     string
		writeOp  func(*byteArrayColumnBuffer)
		expected string
	}{
		{
			name: "writeBoolean_true",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeBoolean(columnLevels{}, true)
			},
			expected: "true",
		},
		{
			name: "writeBoolean_false",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeBoolean(columnLevels{}, false)
			},
			expected: "false",
		},
		{
			name: "writeInt32_positive",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt32(columnLevels{}, 42)
			},
			expected: "42",
		},
		{
			name: "writeInt32_negative",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt32(columnLevels{}, -123)
			},
			expected: "-123",
		},
		{
			name: "writeInt32_zero",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt32(columnLevels{}, 0)
			},
			expected: "0",
		},
		{
			name: "writeInt32_max",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MaxInt32)
			},
			expected: "2147483647",
		},
		{
			name: "writeInt32_min",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt32(columnLevels{}, math.MinInt32)
			},
			expected: "-2147483648",
		},
		{
			name: "writeInt64_positive",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt64(columnLevels{}, 123456789012345)
			},
			expected: "123456789012345",
		},
		{
			name: "writeInt64_negative",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt64(columnLevels{}, -987654321098765)
			},
			expected: "-987654321098765",
		},
		{
			name: "writeInt64_zero",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeInt64(columnLevels{}, 0)
			},
			expected: "0",
		},
		{
			name: "writeFloat_positive",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeFloat(columnLevels{}, 3.14)
			},
			expected: "3.14",
		},
		{
			name: "writeFloat_negative",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeFloat(columnLevels{}, -2.5)
			},
			expected: "-2.5",
		},
		{
			name: "writeFloat_zero",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeFloat(columnLevels{}, 0.0)
			},
			expected: "0",
		},
		{
			name: "writeDouble_positive",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeDouble(columnLevels{}, 2.718281828459045)
			},
			expected: "2.718281828459045",
		},
		{
			name: "writeDouble_negative",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeDouble(columnLevels{}, -1.414213562373095)
			},
			expected: "-1.414213562373095",
		},
		{
			name: "writeDouble_zero",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeDouble(columnLevels{}, 0.0)
			},
			expected: "0",
		},
		{
			name: "writeByteArray",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeByteArray(columnLevels{}, []byte("hello"))
			},
			expected: "hello",
		},
		{
			name: "writeNull",
			writeOp: func(col *byteArrayColumnBuffer) {
				col.writeNull(columnLevels{})
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newByteArrayColumnBuffer(ByteArrayType, 0, 10)
			tt.writeOp(col)

			if col.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", col.Len())
			}

			actual := string(col.index(0))
			if actual != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, actual)
			}
		})
	}
}

// TestColumnBufferByteArrayWriteInt96 tests writeInt96 which converts
// to big.Int text representation
func TestColumnBufferByteArrayWriteInt96(t *testing.T) {
	col := newByteArrayColumnBuffer(ByteArrayType, 0, 10)

	val := [3]uint32{100, 200, 300}
	col.writeInt96(columnLevels{}, val)

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	actual := string(col.index(0))
	if len(actual) == 0 {
		t.Errorf("expected non-empty string representation")
	}
}

// TestColumnBufferByteArrayWriteMultipleValues tests writing multiple values
func TestColumnBufferByteArrayWriteMultipleValues(t *testing.T) {
	col := newByteArrayColumnBuffer(ByteArrayType, 0, 10)

	col.writeInt32(columnLevels{}, 42)
	col.writeInt64(columnLevels{}, 123456)
	col.writeFloat(columnLevels{}, 3.14)
	col.writeDouble(columnLevels{}, 2.718)
	col.writeBoolean(columnLevels{}, true)
	col.writeNull(columnLevels{})

	if col.Len() != 6 {
		t.Fatalf("expected 6 values, got %d", col.Len())
	}

	expected := []string{"42", "123456", "3.14", "2.718", "true", ""}
	for i, exp := range expected {
		actual := string(col.index(i))
		if actual != exp {
			t.Errorf("index %d: expected %q, got %q", i, exp, actual)
		}
	}
}

// TestColumnBufferByteArrayReadWriteRoundTrip tests that values written
// can be read back correctly
func TestColumnBufferByteArrayReadWriteRoundTrip(t *testing.T) {
	col := newByteArrayColumnBuffer(ByteArrayType, 0, 10)

	col.writeInt32(columnLevels{}, 42)
	col.writeFloat(columnLevels{}, 3.14)
	col.writeBoolean(columnLevels{}, true)

	values := make([]Value, 3)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil {
		t.Fatalf("failed to read values: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected to read 3 values, got %d", n)
	}

	if string(values[0].ByteArray()) != "42" {
		t.Errorf("expected %q, got %q", "42", string(values[0].ByteArray()))
	}
	if string(values[1].ByteArray()) != "3.14" {
		t.Errorf("expected %q, got %q", "3.14", string(values[1].ByteArray()))
	}
	if string(values[2].ByteArray()) != "true" {
		t.Errorf("expected %q, got %q", "true", string(values[2].ByteArray()))
	}
}

func TestColumnBufferByteArrayAppendAfterPage(t *testing.T) {
	tests := []struct {
		name  string
		write func(*byteArrayColumnBuffer)
		want  []string
	}{
		{"plain", func(col *byteArrayColumnBuffer) {
			col.WriteByteArrays(plain.AppendByteArray(plain.AppendByteArray(nil, []byte("longer")), []byte("x")))
		}, []string{"longer", "x"}},
		{"null", func(col *byteArrayColumnBuffer) {
			col.writeNull(columnLevels{})
			col.writeNull(columnLevels{})
		}, []string{"", ""}},
		{"boolean", func(col *byteArrayColumnBuffer) {
			col.writeBoolean(columnLevels{}, true)
			col.writeBoolean(columnLevels{}, false)
		}, []string{"true", "false"}},
		{"int32", func(col *byteArrayColumnBuffer) {
			col.writeInt32(columnLevels{}, 123)
			col.writeInt32(columnLevels{}, -4)
		}, []string{"123", "-4"}},
		{"int64", func(col *byteArrayColumnBuffer) {
			col.writeInt64(columnLevels{}, 4567)
			col.writeInt64(columnLevels{}, -89)
		}, []string{"4567", "-89"}},
		{"int96", func(col *byteArrayColumnBuffer) {
			col.writeInt96(columnLevels{}, deprecated.Int96{1, 0, 0})
			col.writeInt96(columnLevels{}, deprecated.Int96{23, 0, 0})
		}, []string{"1", "23"}},
		{"float", func(col *byteArrayColumnBuffer) {
			col.writeFloat(columnLevels{}, 1.25)
			col.writeFloat(columnLevels{}, -2)
		}, []string{"1.25", "-2"}},
		{"double", func(col *byteArrayColumnBuffer) {
			col.writeDouble(columnLevels{}, 12.5)
			col.writeDouble(columnLevels{}, -2)
		}, []string{"12.5", "-2"}},
		{"byteArray", func(col *byteArrayColumnBuffer) {
			col.writeByteArray(columnLevels{}, []byte("longer"))
			col.writeByteArray(columnLevels{}, []byte("x"))
		}, []string{"longer", "x"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			col := newByteArrayColumnBuffer(ByteArrayType, 0, 0)
			col.Page()
			test.write(col)
			if col.offsets.Len() != col.lengths.Len() {
				t.Errorf("offset count: got %d, want %d", col.offsets.Len(), col.lengths.Len())
			}
			want := slices.Clone(test.want)
			col.Swap(0, col.Len()-1)
			want[0], want[len(want)-1] = want[len(want)-1], want[0]
			col = col.Clone().(*byteArrayColumnBuffer)
			values := make([]Value, len(want))
			for _, read := range []func([]Value) (int, error){
				func(v []Value) (int, error) { return col.ReadValuesAt(v, 0) },
				func(v []Value) (int, error) { return col.Page().Values().ReadValues(v) },
			} {
				n, err := read(values)
				if err != nil && err != io.EOF {
					t.Fatal(err)
				}
				got := make([]string, n)
				for i := range got {
					got[i] = string(values[i].ByteArray())
				}
				if !slices.Equal(got, want) {
					t.Errorf("values mismatch: got %q, want %q", got, want)
				}
			}
		})
	}
}

func TestColumnBufferByteArraySwapEmpty(t *testing.T) {
	col := newByteArrayColumnBuffer(ByteArrayType, 0, 0)
	for _, s := range []string{"", "a", "bc"} {
		col.writeByteArray(columnLevels{}, []byte(s))
	}
	col.Page()
	// Exchange the equal-offset values without swapping them directly.
	col.Swap(0, 2)
	col.Swap(0, 1)
	col.Swap(1, 2)
	col = col.Clone().(*byteArrayColumnBuffer)
	values := make([]Value, 3)
	if _, err := col.Page().Values().ReadValues(values); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	for i, want := range []string{"a", "", "bc"} {
		if got := string(values[i].ByteArray()); got != want {
			t.Errorf("value %d: got %q, want %q", i, got, want)
		}
	}
}

func BenchmarkByteArraySwap(b *testing.B) {
	for _, self := range []bool{true, false} {
		name := "SwapDistinct"
		if self {
			name = "SwapSelf"
		}
		b.Run(name, func(b *testing.B) {
			col := newByteArrayColumnBuffer(ByteArrayType, 0, 0)
			col.writeByteArray(columnLevels{}, []byte("a"))
			col.writeByteArray(columnLevels{}, []byte("bc"))
			j := 1
			if self {
				j = 0
			}
			for b.Loop() {
				col.Swap(0, j)
			}
			col.Reset()
		})
	}
}

func BenchmarkByteArrayPageAfterUndo(b *testing.B) {
	col := newByteArrayColumnBuffer(ByteArrayType, 0, 0)
	for range 10000 {
		col.writeByteArray(columnLevels{}, []byte("abcdefghij"))
	}
	col.Page()
	for b.Loop() {
		col.Swap(0, col.Len()-1)
		col.Swap(0, col.Len()-1)
		col.Page()
	}
	col.Reset()
}
