package parquet_test

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

func TestGoogleUUIDPointerSlice(t *testing.T) {
	id := uuid.MustParse("123456ab-0000-0000-0000-000000000000")
	testUUIDPointerSlice(t, id, uuid.Nil)
}

// testUUIDPointerSlice round-trips a slice of pointers to a UUID array type.
// Nil elements stay nil, and a non-nil pointer to the zero UUID stays non-nil.
// A nil slice reads back empty. A plain repeated field has one definition
// level, so a slice whose only element is nil is stored like an empty slice;
// the list tag preserves that single nil element.
func testUUIDPointerSlice[T ~[16]byte](t *testing.T, id, zero T) {
	t.Helper()

	t.Run("repeated", func(t *testing.T) {
		type row struct {
			IDs      []*T
			Optional *T
		}
		rows := []row{
			{IDs: []*T{&id, nil, &zero, &id}, Optional: &id},
			{IDs: []*T{nil, &id}, Optional: &zero},
			{IDs: []*T{nil, nil}},
			{IDs: []*T{}, Optional: nil},
			{IDs: nil},
		}
		got := roundTripRows(t, rows)
		for i := range rows {
			assertUUIDPtrSlice(t, rows[i].IDs, got[i].IDs)
			assertUUIDPtr(t, rows[i].Optional, got[i].Optional)
		}
	})

	t.Run("list", func(t *testing.T) {
		type row struct {
			IDs []*T `parquet:"ids,list"`
		}
		rows := []row{
			{IDs: []*T{&id, nil, &zero, &id}},
			{IDs: []*T{nil, &id}},
			{IDs: []*T{nil}},
			{IDs: []*T{nil, nil}},
			{IDs: []*T{}},
			{IDs: nil},
		}
		got := roundTripRows(t, rows)
		for i := range rows {
			assertUUIDPtrSlice(t, rows[i].IDs, got[i].IDs)
		}
	})
}

func roundTripRows[T any](t *testing.T, rows []T) []T {
	t.Helper()
	var buf bytes.Buffer
	if err := parquet.Write(&buf, rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := parquet.Read[T](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	return got
}

func assertUUIDPtrSlice[T ~[16]byte](t *testing.T, want, got []*T) {
	t.Helper()
	if len(want) == 0 {
		if len(got) != 0 {
			t.Fatalf("slice length = %d, want 0", len(got))
		}
		return
	}
	if len(got) != len(want) {
		t.Fatalf("slice length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		assertUUIDPtr(t, want[i], got[i])
	}
}

func assertUUIDPtr[T ~[16]byte](t *testing.T, want, got *T) {
	t.Helper()
	switch {
	case want == nil && got == nil:
	case want == nil || got == nil:
		t.Fatalf("got %v, want %v", got, want)
	case *want != *got:
		t.Fatalf("got %v, want %v", *got, *want)
	}
}
