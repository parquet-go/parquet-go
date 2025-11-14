package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
)

func TestColumnBufferBE128WriteTypes(t *testing.T) {
	t.Run("writeBoolean_false", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeBoolean(columnLevels{}, false)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("writeBoolean_true", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeBoolean(columnLevels{}, true)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		expected[15] = 1
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("writeInt32", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeInt32(columnLevels{}, 42)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		binary.BigEndian.PutUint32(expected[12:16], 42)
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("writeInt64", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeInt64(columnLevels{}, 123456789)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		binary.BigEndian.PutUint64(expected[8:16], 123456789)
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("writeFloat", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeFloat(columnLevels{}, 3.14)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		binary.BigEndian.PutUint32(expected[12:16], math.Float32bits(3.14))
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("writeDouble", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeDouble(columnLevels{}, 2.718)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		binary.BigEndian.PutUint64(expected[8:16], math.Float64bits(2.718))
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})
}

func TestColumnBufferBE128WriteInt96(t *testing.T) {
	col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

	col.writeInt96(columnLevels{}, deprecated.Int96{100, 200, 300})

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	expected := [16]byte{}
	binary.BigEndian.PutUint32(expected[4:8], 300)
	binary.BigEndian.PutUint32(expected[8:12], 200)
	binary.BigEndian.PutUint32(expected[12:16], 100)

	if col.values[0] != expected {
		t.Errorf("expected %v, got %v", expected, col.values[0])
	}
}

func TestColumnBufferBE128WriteByteArray(t *testing.T) {
	t.Run("exact_size", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		input := []byte("0123456789ABCDEF")
		col.writeByteArray(columnLevels{}, input)

		if col.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", col.Len())
		}

		expected := [16]byte{}
		copy(expected[:], input)
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("wrong_size_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for wrong size")
			}
		}()

		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)
		col.writeByteArray(columnLevels{}, []byte("short"))
	})
}

func TestColumnBufferBE128WriteNull(t *testing.T) {
	col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

	col.writeNull(columnLevels{})

	if col.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", col.Len())
	}

	expected := [16]byte{}
	if col.values[0] != expected {
		t.Errorf("expected %v, got %v", expected, col.values[0])
	}
}

func TestColumnBufferBE128Padding(t *testing.T) {
	t.Run("int32_left_padding", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeInt32(columnLevels{}, 0x12345678)

		expected := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78}
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("int64_left_padding", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeInt64(columnLevels{}, 0x0102030405060708)

		expected := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})

	t.Run("float_left_padding", func(t *testing.T) {
		col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

		col.writeFloat(columnLevels{}, 1.0)

		floatBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(floatBytes, math.Float32bits(1.0))

		expected := [16]byte{}
		copy(expected[12:16], floatBytes)

		if col.values[0] != expected {
			t.Errorf("expected %v, got %v", expected, col.values[0])
		}
	})
}

func TestColumnBufferBE128WriteMultipleValues(t *testing.T) {
	col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

	col.writeInt32(columnLevels{}, 1)
	col.writeInt64(columnLevels{}, 2)
	col.writeFloat(columnLevels{}, 3.0)
	col.writeNull(columnLevels{})

	if col.Len() != 4 {
		t.Fatalf("expected 4 values, got %d", col.Len())
	}

	expected := make([][16]byte, 4)
	binary.BigEndian.PutUint32(expected[0][12:16], 1)
	binary.BigEndian.PutUint64(expected[1][8:16], 2)
	binary.BigEndian.PutUint32(expected[2][12:16], math.Float32bits(3.0))

	for i := range 4 {
		if col.values[i] != expected[i] {
			t.Errorf("index %d: expected %v, got %v", i, expected[i], col.values[i])
		}
	}
}

func TestColumnBufferBE128ByteArrayWrongSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when byte array is wrong size")
		}
	}()

	col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

	largeValue := make([]byte, 17)
	col.writeByteArray(columnLevels{}, largeValue)
}

func TestColumnBufferBE128ReadWriteRoundTrip(t *testing.T) {
	col := newBE128ColumnBuffer(ByteArrayType, 0, 10)

	col.writeInt32(columnLevels{}, 42)
	col.writeInt64(columnLevels{}, 123456789)

	values := make([]Value, 2)
	n, err := col.ReadValuesAt(values, 0)
	if err != nil || n != 2 {
		t.Fatalf("failed to read values: %v", err)
	}

	expected1 := [16]byte{}
	binary.BigEndian.PutUint32(expected1[12:16], 42)

	expected2 := [16]byte{}
	binary.BigEndian.PutUint64(expected2[8:16], 123456789)

	if !bytes.Equal(values[0].byteArray(), expected1[:]) {
		t.Errorf("value 0: expected %v, got %v", expected1[:], values[0].byteArray())
	}

	if !bytes.Equal(values[1].byteArray(), expected2[:]) {
		t.Errorf("value 1: expected %v, got %v", expected2[:], values[1].byteArray())
	}
}
