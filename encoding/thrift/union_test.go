package thrift_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

// UnionUnregistered satisfies UnionValue but is deliberately absent from
// unionMembers, which is the failure the FieldID method cannot catch on its own.
type UnionUnregistered struct {
	V int64 `thrift:"1"`
}

func (*UnionUnregistered) FieldID() int16 { return 9 }

func compactRoundTrip(t *testing.T, in, out any) {
	t.Helper()
	b, err := thrift.Marshal(new(thrift.CompactProtocol), in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), b, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

// TestUnionMembersRoundTrip walks every member declared by UnionMembers and
// asserts it survives a round trip with its concrete type intact. This is what
// catches a member that was added to the interface but never registered.
func TestUnionMembersRoundTrip(t *testing.T) {
	for _, member := range (&Union{}).UnionMembers() {
		typ := reflect.TypeOf(member).Elem()

		t.Run(typ.Name(), func(t *testing.T) {
			in := Union{Value: reflect.New(typ).Interface().(UnionValue)}

			var out Union
			compactRoundTrip(t, in, &out)

			if out.Value == nil {
				t.Fatalf("member %s decoded to a nil union", typ)
			}
			if got := reflect.TypeOf(out.Value).Elem(); got != typ {
				t.Fatalf("member decoded as %s, want %s", got, typ)
			}
			if out.Value.FieldID() != member.FieldID() {
				t.Fatalf("field id = %d, want %d", out.Value.FieldID(), member.FieldID())
			}
		})
	}
}

// TestUnionFieldIDsAreUnique keeps the id -> type mapping of the union readable
// in one place, standing in for the table we gave up by moving ids onto members.
func TestUnionFieldIDsAreUnique(t *testing.T) {
	want := map[int16]string{
		1: "UnionBool",
		2: "UnionInt",
		3: "UnionString",
		4: "UnionEmpty",
	}

	got := make(map[int16]string)
	for _, member := range (&Union{}).UnionMembers() {
		id := member.FieldID()
		name := reflect.TypeOf(member).Elem().Name()
		if prev, ok := got[id]; ok {
			t.Fatalf("field id %d claimed by both %s and %s", id, prev, name)
		}
		got[id] = name
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("union members = %v, want %v", got, want)
	}
}

func TestUnionNotSet(t *testing.T) {
	b, err := thrift.Marshal(new(thrift.CompactProtocol), Union{})
	if err != nil {
		t.Fatal(err)
	}
	// An unset union is a bare STOP.
	if !bytes.Equal(b, []byte{0}) {
		t.Errorf("unset union encoded as % x, want 00", b)
	}

	out := Union{Value: &UnionBool{V: true}}
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Value != nil {
		t.Errorf("decoding an unset union left a stale member: %#v", out.Value)
	}
}

// TestUnionInternsZeroSizedMembers verifies the allocation claim: a member that
// carries no state is handed out from a shared instance.
func TestUnionInternsZeroSizedMembers(t *testing.T) {
	b, err := thrift.Marshal(new(thrift.CompactProtocol), Union{Value: &UnionEmpty{}})
	if err != nil {
		t.Fatal(err)
	}

	var first, second Union
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), b, &first); err != nil {
		t.Fatal(err)
	}
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), b, &second); err != nil {
		t.Fatal(err)
	}

	if first.Value != second.Value {
		t.Errorf("zero-sized member was not interned: %p != %p", first.Value, second.Value)
	}
}

func TestUnionUnregisteredMemberFailsToEncode(t *testing.T) {
	_, err := thrift.Marshal(new(thrift.CompactProtocol), Union{Value: &UnionUnregistered{V: 1}})
	if err == nil {
		t.Fatal("expected an error encoding an unregistered union member")
	}
	if !strings.Contains(err.Error(), "UnionUnregistered") || !strings.Contains(err.Error(), "Union") {
		t.Errorf("error should name both the member and the union, got: %v", err)
	}
}

// TestUnionSkipsUnknownFieldID simulates a newer writer that added a member we
// do not know about: the field is skipped and the union decodes as unset.
func TestUnionSkipsUnknownFieldID(t *testing.T) {
	buf := new(bytes.Buffer)
	w := new(thrift.CompactProtocol).NewWriter(buf)

	if err := w.WriteField(thrift.Field{ID: 99, Type: thrift.STRUCT}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteField(thrift.Field{Type: thrift.STOP}); err != nil { // end of member
		t.Fatal(err)
	}
	if err := w.WriteField(thrift.Field{Type: thrift.STOP}); err != nil { // end of union
		t.Fatal(err)
	}

	out := Union{Value: &UnionBool{V: true}}
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), buf.Bytes(), &out); err != nil {
		t.Fatalf("unknown member id should be skipped, got: %v", err)
	}
	if out.Value != nil {
		t.Errorf("unknown member id should leave the union unset, got %#v", out.Value)
	}
}

// Malformed unions, each of which must panic when its codec is built.

type unionTwoFields struct {
	Value UnionValue
	Extra int
}

func (*unionTwoFields) UnionMembers() []thrift.UnionMember { return unionMembers }

type unionNotAnInterface struct {
	Value int
}

func (*unionNotAnInterface) UnionMembers() []thrift.UnionMember { return unionMembers }

type unionNoMembers struct {
	Value UnionValue
}

func (*unionNoMembers) UnionMembers() []thrift.UnionMember { return nil }

type unionDuplicateID struct {
	Value UnionValue
}

func (*unionDuplicateID) UnionMembers() []thrift.UnionMember {
	return []thrift.UnionMember{(*UnionBool)(nil), (*UnionBoolAlias)(nil)}
}

type UnionBoolAlias struct {
	V bool `thrift:"1"`
}

func (*UnionBoolAlias) FieldID() int16 { return 1 } // collides with UnionBool

// narrowUnionValue demands a method UnionBool does not have, so UnionBool is
// not assignable to the member field.
type narrowUnionValue interface {
	thrift.UnionMember
	narrow()
}

type unionNotAssignable struct {
	Value narrowUnionValue
}

func (*unionNotAssignable) UnionMembers() []thrift.UnionMember {
	return []thrift.UnionMember{(*UnionBool)(nil)}
}

func TestUnionValidationPanics(t *testing.T) {
	tests := []struct {
		scenario string
		value    any
		message  string
	}{
		{"more than one field", unionTwoFields{}, "exactly one field"},
		{"field is not an interface", unionNotAnInterface{}, "must be an interface"},
		{"no members", unionNoMembers{}, "declares no members"},
		{"duplicate field id", unionDuplicateID{}, "present multiple times"},
		{"member not assignable", unionNotAssignable{}, "not assignable"},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected a panic when building the codec")
				}
				msg, ok := r.(error)
				if !ok {
					t.Fatalf("panic value is not an error: %v", r)
				}
				if !strings.Contains(msg.Error(), test.message) {
					t.Errorf("panic %q does not contain %q", msg, test.message)
				}
			}()

			thrift.EncodeFuncOf(reflect.TypeOf(test.value), make(thrift.EncodeFuncCache))
		})
	}
}
