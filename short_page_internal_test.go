package parquet

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

// TestCheckPageDataCapacityUsesLength checks a decoded buffer whose length is
// shorter than the header's value count and whose capacity is not. The spare
// capacity is what a pooled buffer still holds from an earlier page.
func TestCheckPageDataCapacityUsesLength(t *testing.T) {
	tests := []struct {
		kind       Kind
		typeLength int
		numValues  int
		length     int
		capacity   int
		wantErr    bool
	}{
		{kind: Boolean, numValues: 24, length: 1, capacity: 64, wantErr: true},
		{kind: Boolean, numValues: 24, length: 3, capacity: 64, wantErr: false},
		{kind: Boolean, numValues: 24, length: 3, capacity: 3, wantErr: false},
		{kind: Boolean, numValues: 1, length: 1, capacity: 1, wantErr: false},
		{kind: Boolean, numValues: 0, length: 0, capacity: 64, wantErr: false},
		{kind: Int32, numValues: 4, length: 4, capacity: 64, wantErr: true},
		{kind: Int32, numValues: 4, length: 16, capacity: 64, wantErr: false},
		{kind: Int64, numValues: 2, length: 8, capacity: 64, wantErr: true},
		{kind: Int64, numValues: 2, length: 16, capacity: 16, wantErr: false},
		{kind: Int96, numValues: 2, length: 12, capacity: 64, wantErr: true},
		{kind: Int96, numValues: 2, length: 24, capacity: 24, wantErr: false},
		{kind: Float, numValues: 2, length: 4, capacity: 64, wantErr: true},
		{kind: Float, numValues: 2, length: 8, capacity: 8, wantErr: false},
		{kind: Double, numValues: 2, length: 8, capacity: 64, wantErr: true},
		{kind: Double, numValues: 2, length: 16, capacity: 16, wantErr: false},
		{kind: FixedLenByteArray, typeLength: 16, numValues: 2, length: 16, capacity: 64, wantErr: true},
		{kind: FixedLenByteArray, typeLength: 16, numValues: 2, length: 32, capacity: 32, wantErr: false},
		// Byte array widths live in the offset slice, not this check.
		{kind: ByteArray, numValues: 10, length: 0, capacity: 64, wantErr: false},
	}

	for _, test := range tests {
		name := test.kind.String() + "/n=" + strconv.Itoa(test.numValues) + "/len=" + strconv.Itoa(test.length) + "/cap=" + strconv.Itoa(test.capacity)
		t.Run(name, func(t *testing.T) {
			buf := bytes.Repeat([]byte{0xFF}, test.capacity)
			values := valuesWithLen(test.kind, test.typeLength, buf[:test.length:test.capacity])
			err := checkPageDataCapacity(test.kind, test.typeLength, test.numValues, values)
			if test.wantErr {
				if !errors.Is(err, ErrCorrupted) {
					t.Fatalf("got %v, want an error wrapping ErrCorrupted", err)
				}
				if !strings.Contains(err.Error(), "holds only "+strconv.Itoa(test.length)) {
					t.Fatalf("error %v, want it to report the decoded length %d", err, test.length)
				}
				return
			}
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

func valuesWithLen(kind Kind, typeLength int, data []byte) encoding.Values {
	switch kind {
	case Boolean:
		return encoding.BooleanValues(data)
	case Int32:
		return encoding.Int32ValuesFromBytes(data)
	case Int64:
		return encoding.Int64ValuesFromBytes(data)
	case Int96:
		return encoding.Int96ValuesFromBytes(data)
	case Float:
		return encoding.FloatValuesFromBytes(data)
	case Double:
		return encoding.DoubleValuesFromBytes(data)
	case FixedLenByteArray:
		return encoding.FixedLenByteArrayValues(data, typeLength)
	case ByteArray:
		return encoding.ByteArrayValues(data, nil)
	default:
		return encoding.BooleanValues(data)
	}
}
