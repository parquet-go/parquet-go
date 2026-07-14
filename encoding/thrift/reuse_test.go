package thrift_test

import (
	"errors"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

// These tests exercise the decode-target reuse semantics documented on
// Unmarshal: when decoding into a value that already holds data from a
// previous decode, pointer fields absent from the input are set to nil, and
// reused pointees are zeroed before decoding so no stale fields leak through.

type reuseInner struct {
	A int64  `thrift:"1,optional"`
	B string `thrift:"2,optional"`
}

type reuseOuter struct {
	Name  string      `thrift:"1"`
	Inner *reuseInner `thrift:"2,optional"`
	Count *int64      `thrift:"3,optional"`
}

func TestUnmarshalReuseNilsUnseenPointers(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			count := int64(42)
			first, err := thrift.Marshal(p.proto, &reuseOuter{
				Name:  "first",
				Inner: &reuseInner{A: 1, B: "one"},
				Count: &count,
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			second, err := thrift.Marshal(p.proto, &reuseOuter{Name: "second"})
			if err != nil {
				t.Fatal("marshal:", err)
			}

			v := new(reuseOuter)
			if err := thrift.Unmarshal(p.proto, first, v); err != nil {
				t.Fatal("unmarshal first:", err)
			}
			if v.Inner == nil || v.Count == nil {
				t.Fatalf("first decode did not populate pointer fields: %+v", v)
			}

			if err := thrift.Unmarshal(p.proto, second, v); err != nil {
				t.Fatal("unmarshal second:", err)
			}
			if v.Name != "second" {
				t.Errorf("Name = %q, want %q", v.Name, "second")
			}
			if v.Inner != nil {
				t.Errorf("Inner = %+v, want nil after decoding input without field 2", v.Inner)
			}
			if v.Count != nil {
				t.Errorf("Count = %d, want nil after decoding input without field 3", *v.Count)
			}
		})
	}
}

func TestUnmarshalReuseZeroesPointee(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			first, err := thrift.Marshal(p.proto, &reuseOuter{
				Name:  "first",
				Inner: &reuseInner{A: 1, B: "one"},
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			// Second input sets Inner.A but not Inner.B.
			second, err := thrift.Marshal(p.proto, &reuseOuter{
				Name:  "second",
				Inner: &reuseInner{A: 2},
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}

			v := new(reuseOuter)
			if err := thrift.Unmarshal(p.proto, first, v); err != nil {
				t.Fatal("unmarshal first:", err)
			}
			firstInner := v.Inner

			if err := thrift.Unmarshal(p.proto, second, v); err != nil {
				t.Fatal("unmarshal second:", err)
			}
			if v.Inner != firstInner {
				t.Error("Inner pointer was reallocated instead of reused")
			}
			if v.Inner.A != 2 {
				t.Errorf("Inner.A = %d, want 2", v.Inner.A)
			}
			if v.Inner.B != "" {
				t.Errorf("Inner.B = %q, want empty: stale value leaked from previous decode", v.Inner.B)
			}
		})
	}
}

// TestUnmarshalReuseTypeMismatchedField checks the non-strict handling of
// fields whose wire type does not match the target: the value must be
// consumed (keeping the stream aligned) and the field left untouched, in
// particular pointer fields populated by a previous decode must be niled
// exactly as if the field were absent from the input.
func TestUnmarshalReuseTypeMismatchedField(t *testing.T) {
	// Same field IDs as reuseOuter but with different wire types.
	type mismatchedOuter struct {
		Name  string `thrift:"1"`
		Inner int32  `thrift:"2,optional"` // STRUCT expected, I32 sent
		Count string `thrift:"3,optional"` // I64 expected, BINARY sent
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			count := int64(42)
			first, err := thrift.Marshal(p.proto, &reuseOuter{
				Name:  "first",
				Inner: &reuseInner{A: 1, B: "one"},
				Count: &count,
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			second, err := thrift.Marshal(p.proto, &mismatchedOuter{
				Name:  "second",
				Inner: 7,
				Count: "not an integer",
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}

			v := new(reuseOuter)
			if err := thrift.Unmarshal(p.proto, first, v); err != nil {
				t.Fatal("unmarshal first:", err)
			}
			if err := thrift.Unmarshal(p.proto, second, v); err != nil {
				t.Fatal("unmarshal second:", err)
			}
			if v.Name != "second" {
				t.Errorf("Name = %q, want %q (stream desynchronized?)", v.Name, "second")
			}
			if v.Inner != nil {
				t.Errorf("Inner = %+v, want nil: stale pointer survived a type-mismatched field", v.Inner)
			}
			if v.Count != nil {
				t.Errorf("Count = %d, want nil: stale pointer survived a type-mismatched field", *v.Count)
			}
		})
	}
}

// TestUnmarshalRequiredTypeMismatch checks that a required field whose wire
// type does not match is reported as missing rather than silently accepted
// with a zero value.
func TestUnmarshalRequiredTypeMismatch(t *testing.T) {
	type point struct {
		X float64 `thrift:"1,required"`
		Y float64 `thrift:"2,required"`
	}
	type mismatchedPoint struct {
		X string  `thrift:"1"` // DOUBLE expected, BINARY sent
		Y float64 `thrift:"2"`
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			b, err := thrift.Marshal(p.proto, &mismatchedPoint{X: "oops", Y: 2})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			err = thrift.Unmarshal(p.proto, b, new(point))
			missing := new(thrift.MissingField)
			if !errors.As(err, &missing) {
				t.Fatalf("expected MissingField error, got %v", err)
			}
			if missing.Field.ID != 1 {
				t.Errorf("missing field ID = %d, want 1", missing.Field.ID)
			}
		})
	}
}

// TestUnmarshalContainerElementTypeMismatch checks the non-strict handling
// of containers whose element wire type does not match the target: the
// elements must still be consumed so that fields following the container
// decode correctly. Without the skip, the binary protocol fails with
// trailing bytes and the compact protocol silently misparses the next
// field.
func TestUnmarshalContainerElementTypeMismatch(t *testing.T) {
	type source struct {
		List  []string             `thrift:"1"`
		Slice thrift.Slice[string] `thrift:"2"`
		Map   map[string]string    `thrift:"3"`
		Set   map[string]struct{}  `thrift:"4"`
		After int64                `thrift:"5"`
	}
	type target struct {
		List  []int64             `thrift:"1"`
		Slice thrift.Slice[int64] `thrift:"2"`
		Map   map[int64]int64     `thrift:"3"`
		Set   map[int64]struct{}  `thrift:"4"`
		After int64               `thrift:"5"`
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			b, err := thrift.Marshal(p.proto, &source{
				List:  []string{"a", "bc"},
				Slice: thrift.Slice[string]{"def"},
				Map:   map[string]string{"key": "value"},
				Set:   map[string]struct{}{"member": {}},
				After: 42,
			})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			v := new(target)
			if err := thrift.Unmarshal(p.proto, b, v); err != nil {
				t.Fatal("unmarshal:", err)
			}
			if len(v.List) != 0 || len(v.Slice) != 0 || len(v.Map) != 0 || len(v.Set) != 0 {
				t.Errorf("mismatched containers should decode empty, got %+v", v)
			}
			if v.After != 42 {
				t.Errorf("After = %d, want 42 (stream desynchronized by a mismatched container)", v.After)
			}
		})
	}
}

// TestUnmarshalSkipsUnknownBinaryField checks that skipping an unknown
// string/binary field advances the reader correctly, so that known fields
// following it still decode. This exercises the Discard path of the
// bytes-backed readers, whose Reader() method returns a positionless view.
func TestUnmarshalSkipsUnknownBinaryField(t *testing.T) {
	type withString struct {
		S string `thrift:"1"`
		B int64  `thrift:"2"`
	}
	type withoutString struct {
		B int64 `thrift:"2"`
	}

	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			b, err := thrift.Marshal(p.proto, &withString{S: "skip me entirely", B: 42})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			v := new(withoutString)
			if err := thrift.Unmarshal(p.proto, b, v); err != nil {
				t.Fatal("unmarshal:", err)
			}
			if v.B != 42 {
				t.Errorf("B = %d, want 42 (unknown binary field was not skipped correctly)", v.B)
			}
		})
	}
}

// sparseIDs has a field ID span (1..100) wider than one 64-bit bitmap word
// while declaring only two fields, exercising the sparse sizing of the seen
// and required bitmaps in the struct decoder.
type sparseIDs struct {
	First int64 `thrift:"1,required"`
	Last  int64 `thrift:"100,required"`
}

type sparseIDsPartial struct {
	First int64 `thrift:"1"`
}

func TestUnmarshalSparseFieldIDs(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			b, err := thrift.Marshal(p.proto, &sparseIDs{First: 1, Last: 100})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			v := new(sparseIDs)
			if err := thrift.Unmarshal(p.proto, b, v); err != nil {
				t.Fatal("unmarshal:", err)
			}
			if v.First != 1 || v.Last != 100 {
				t.Errorf("got %+v, want {First:1 Last:100}", v)
			}

			// Input missing required field 100 must be rejected, which
			// requires the required bitmap to cover the full ID span.
			partial, err := thrift.Marshal(p.proto, &sparseIDsPartial{First: 1})
			if err != nil {
				t.Fatal("marshal:", err)
			}
			err = thrift.Unmarshal(p.proto, partial, new(sparseIDs))
			missing := new(thrift.MissingField)
			if !errors.As(err, &missing) {
				t.Fatalf("expected MissingField error, got %v", err)
			}
			if missing.Field.ID != 100 {
				t.Errorf("missing field ID = %d, want 100", missing.Field.ID)
			}
		})
	}
}
