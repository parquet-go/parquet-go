package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// TestFixedLenByteArrayColumnBufferWriteTypes tests that fixedLenByteArrayColumnBuffer
// correctly converts various types to fixed-length byte arrays with big-endian encoding
func TestColumnBufferFixedLenByteArrayWriteTypes(t *testing.T) {
	t.Run("writeBoolean_size1", func(t *testing.T) {
		typ := FixedLenByteArrayType(1)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeBoolean(columnLevels{}, true)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := []byte{1}
		actual := col.data.Slice()[0:1]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("writeInt32_size4", func(t *testing.T) {
		typ := FixedLenByteArrayType(4)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeInt32(columnLevels{}, 42)

		expected := make([]byte, 4)
		binary.BigEndian.PutUint32(expected, 42)
		actual := col.data.Slice()[0:4]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("writeInt32_size8_with_padding", func(t *testing.T) {
		typ := FixedLenByteArrayType(8)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeInt32(columnLevels{}, 42)

		expected := []byte{0, 0, 0, 0, 0, 0, 0, 42}
		actual := col.data.Slice()[0:8]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("writeInt64_size8", func(t *testing.T) {
		typ := FixedLenByteArrayType(8)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeInt64(columnLevels{}, 123456789)

		expected := make([]byte, 8)
		binary.BigEndian.PutUint64(expected, 123456789)
		actual := col.data.Slice()[0:8]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("writeFloat_size4", func(t *testing.T) {
		typ := FixedLenByteArrayType(4)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeFloat(columnLevels{}, 3.14)

		expected := make([]byte, 4)
		binary.BigEndian.PutUint32(expected, math.Float32bits(3.14))
		actual := col.data.Slice()[0:4]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("writeDouble_size8", func(t *testing.T) {
		typ := FixedLenByteArrayType(8)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeDouble(columnLevels{}, 2.718)

		expected := make([]byte, 8)
		binary.BigEndian.PutUint64(expected, math.Float64bits(2.718))
		actual := col.data.Slice()[0:8]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
}

// TestFixedLenByteArrayColumnBufferWriteInt96 tests writeInt96 conversion
func TestColumnBufferFixedLenByteArrayWriteInt96(t *testing.T) {
	typ := FixedLenByteArrayType(12)
	col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

	col.writeInt96(columnLevels{}, [3]uint32{100, 200, 300})

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	expected := make([]byte, 12)
	binary.BigEndian.PutUint32(expected[0:4], 300)
	binary.BigEndian.PutUint32(expected[4:8], 200)
	binary.BigEndian.PutUint32(expected[8:12], 100)

	actual := col.data.Slice()[0:12]
	if !bytes.Equal(actual, expected) {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

// TestFixedLenByteArrayColumnBufferWriteByteArray tests exact-size byte array writes
func TestColumnBufferFixedLenByteArrayWriteByteArray(t *testing.T) {
	t.Run("exact_size", func(t *testing.T) {
		typ := FixedLenByteArrayType(5)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeByteArray(columnLevels{}, []byte("hello"))

		expected := []byte("hello")
		actual := col.data.Slice()[0:5]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("wrong_size_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for wrong size")
			}
		}()

		typ := FixedLenByteArrayType(5)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)
		col.writeByteArray(columnLevels{}, []byte("hi"))
	})
}

// TestFixedLenByteArrayColumnBufferWriteNull tests null writes
func TestColumnBufferFixedLenByteArrayWriteNull(t *testing.T) {
	typ := FixedLenByteArrayType(4)
	col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

	col.writeNull(columnLevels{})

	expected := []byte{0, 0, 0, 0}
	actual := col.data.Slice()[0:4]
	if !bytes.Equal(actual, expected) {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

// TestFixedLenByteArrayColumnBufferPadding documents left-padding behavior
func TestColumnBufferFixedLenByteArrayPadding(t *testing.T) {
	t.Run("int32_in_8byte_column", func(t *testing.T) {
		typ := FixedLenByteArrayType(8)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeInt32(columnLevels{}, 0x12345678)

		expected := []byte{0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78}
		actual := col.data.Slice()[0:8]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("float_in_8byte_column", func(t *testing.T) {
		typ := FixedLenByteArrayType(8)
		col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

		col.writeFloat(columnLevels{}, 1.0)

		floatBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(floatBytes, math.Float32bits(1.0))
		expected := append([]byte{0, 0, 0, 0}, floatBytes...)

		actual := col.data.Slice()[0:8]
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
}

// TestFixedLenByteArrayColumnBufferWriteMultipleValues tests writing multiple values
func TestColumnBufferFixedLenByteArrayWriteMultipleValues(t *testing.T) {
	typ := FixedLenByteArrayType(4)
	col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt32(columnLevels{}, 2)
	col.writeInt32(columnLevels{}, 3)
	col.writeNull(columnLevels{})

	if col.Len() != 4 {
		t.Fatalf("expected 4 values, got %d", col.Len())
	}

	for i, expectedVal := range []uint32{1, 2, 3, 0} {
		offset := i * 4
		expected := make([]byte, 4)
		binary.BigEndian.PutUint32(expected, expectedVal)

		actual := col.data.Slice()[offset : offset+4]
		if !bytes.Equal(actual, expected) {
			t.Errorf("index %d: expected %v, got %v", i, expected, actual)
		}
	}
}

// TestFixedLenByteArrayColumnBufferValueTooLarge tests panic when value is too large
func TestColumnBufferFixedLenByteArrayValueTooLarge(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when value is too large for column")
		}
	}()

	typ := FixedLenByteArrayType(2)
	col := newFixedLenByteArrayColumnBuffer(typ, 0, 10)

	col.writeInt32(columnLevels{}, 42)
}

func TestFixedLenByteArrayWithSliceField(t *testing.T) {
	// Test if FixedLenByteArrayType works with []byte fields (slices) instead
	// of [N]byte (arrays). By default, SchemaOf maps []byte to ByteArrayType,
	// so we need to manually construct a schema.
	type FixedLenByteArrayWithSlice struct {
		Value []byte
	}

	schema := NewSchema("test", Group{
		"Value": Leaf(FixedLenByteArrayType(10)),
	})

	testData := []FixedLenByteArrayWithSlice{
		{Value: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{Value: []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}},
		{Value: []byte{20, 21, 22, 23, 24, 25, 26, 27, 28, 29}},
	}

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test_fixed.parquet")

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	writer := NewGenericWriter[FixedLenByteArrayWithSlice](f, schema)
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected to write %d rows, wrote %d", len(testData), n)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	f2, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	stat, err := f2.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	pf, err := OpenFile(f2, stat.Size())
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	reader := NewGenericReader[FixedLenByteArrayWithSlice](pf)
	defer reader.Close()

	readData := make([]FixedLenByteArrayWithSlice, len(testData))
	n, err = reader.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read data: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected to read %d rows, read %d", len(testData), n)
	}

	for i, expected := range testData {
		if !bytes.Equal(readData[i].Value, expected.Value) {
			t.Errorf("row %d: expected %v, got %v", i, expected.Value, readData[i].Value)
		}
	}
}
