package parquet

import (
	"testing"
	"unsafe"
)

func TestMemsetValues(t *testing.T) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	model.columnIndex = ^uint16(7)
	model.definitionLevel = 3
	model.repetitionLevel = 1

	for _, n := range []int{0, 1, 3, 4, 5, 63, 64, 65} {
		values := make([]Value, n)
		memsetValues(values, model)
		for i := range values {
			if values[i] != model {
				t.Fatalf("len=%d: values[%d] = %+v, want %+v", n, i, values[i], model)
			}
		}
	}
}

func TestStoreValue(t *testing.T) {
	if unsafe.Sizeof(Value{}) != 24 ||
		unsafe.Offsetof(Value{}.u64) != 8 ||
		unsafe.Offsetof(Value{}.kind) != 16 ||
		unsafe.Offsetof(Value{}.definitionLevel) != 17 ||
		unsafe.Offsetof(Value{}.repetitionLevel) != 18 ||
		unsafe.Offsetof(Value{}.columnIndex) != 20 {
		t.Fatal("Value layout changed; valueWords must be updated")
	}

	payload := []byte("hello")
	want := ByteArrayValue(payload).Level(0, 4, 9)
	var got Value
	// Dirty the destination first so unset bytes would show up.
	got = Int64Value(-1).Level(3, 3, 3)
	storeBytesValue(&got, payload, ^int8(ByteArray), 4, ^uint16(9))
	if got != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}

	want = Int32Value(-12345).Level(0, 2, 1)
	got = ByteArrayValue([]byte("x"))
	negative := int32(-12345)
	storeValue(&got, nil, uint64(negative), ^int8(Int32), 2, ^uint16(1))
	if got != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func BenchmarkMemsetValues(b *testing.B) {
	model := makeValueBytes(ByteArray, []byte("0123456789"))
	values := make([]Value, 1024)
	b.SetBytes(int64(len(values) * 24))
	for b.Loop() {
		memsetValues(values, model)
	}
}
