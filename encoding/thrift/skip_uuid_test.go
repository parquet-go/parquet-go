package thrift_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

func TestSkipUUIDField(t *testing.T) {
	// Compact protocol bytes for a flat struct:
	//   field 1 (I32) = 42
	//   field 5 (UUID) = 16 bytes
	//   field 6 (I32) = 99
	//   STOP
	data := []byte{
		0x15,       // field 1, delta=1, type=I32(5)
		0x54,       // zigzag(42) = 84
		0x4D,       // field 5, delta=4, type=UUID(13)
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
		0x15,       // field 6, delta=1, type=I32(5)
		0xC6, 0x01, // zigzag(99) = 198
		0x00,       // STOP
	}

	type S struct {
		A int32 `thrift:"1"`
		// field 5 (UUID) is NOT defined → must be skipped
		B int32 `thrift:"6"`
	}

	var result S
	if err := thrift.Unmarshal(&thrift.CompactProtocol{}, data, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if result.A != 42 {
		t.Errorf("A = %d, want 42", result.A)
	}
	if result.B != 99 {
		t.Errorf("B = %d, want 99 (stream corrupted by UUID skip)", result.B)
	}
}
