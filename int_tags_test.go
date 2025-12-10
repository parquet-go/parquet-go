package parquet

import (
	"bytes"
	"reflect"
	"testing"
)

// TestNativeIntWithSmallerBitWidth tests using int(8), int(16), int(32) tags
// on native int/uint types. This is a regression test for s390x test failures
// where native int/uint are 64-bit but the logical type specifies a smaller
// bit width.
//
// The bug was that intType.AssignValue() and int32Type.AssignValue() didn't
// handle reflect.Int and reflect.Uint types, causing panics on 64-bit
// architectures when using smaller bit width tags.
func TestNativeIntWithSmallerBitWidth(t *testing.T) {
	t.Run("int with int(8)", func(t *testing.T) {
		type Record struct {
			Value int `parquet:"value,int(8)"`
		}
		testRoundTrip(t, []Record{{Value: -128}, {Value: 0}, {Value: 127}})
	})

	t.Run("int with int(16)", func(t *testing.T) {
		type Record struct {
			Value int `parquet:"value,int(16)"`
		}
		testRoundTrip(t, []Record{{Value: -32768}, {Value: 0}, {Value: 32767}})
	})

	t.Run("int with int(32)", func(t *testing.T) {
		type Record struct {
			Value int `parquet:"value,int(32)"`
		}
		testRoundTrip(t, []Record{{Value: -2147483648}, {Value: 0}, {Value: 2147483647}})
	})

	t.Run("uint with uint(8)", func(t *testing.T) {
		type Record struct {
			Value uint `parquet:"value,uint(8)"`
		}
		testRoundTrip(t, []Record{{Value: 0}, {Value: 128}, {Value: 255}})
	})

	t.Run("uint with uint(16)", func(t *testing.T) {
		type Record struct {
			Value uint `parquet:"value,uint(16)"`
		}
		testRoundTrip(t, []Record{{Value: 0}, {Value: 32768}, {Value: 65535}})
	})

	t.Run("uint with uint(32)", func(t *testing.T) {
		type Record struct {
			Value uint `parquet:"value,uint(32)"`
		}
		// Test values that have the sign bit set to ensure proper unsigned handling
		testRoundTrip(t, []Record{{Value: 0}, {Value: 2147483648}, {Value: 4294967295}})
	})
}

func testRoundTrip[T any](t *testing.T, original []T) {
	t.Helper()

	buf := new(bytes.Buffer)
	writer := NewGenericWriter[T](buf)
	if _, err := writer.Write(original); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	result, err := Read[T](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if len(result) != len(original) {
		t.Fatalf("expected %d records, got %d", len(original), len(result))
	}

	if !reflect.DeepEqual(result, original) {
		t.Errorf("mismatch:\nexpected: %+v\ngot:      %+v", original, result)
	}
}
