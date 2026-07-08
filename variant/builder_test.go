package variant

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/google/uuid"
)

// builderTestValues is a pool of values covering every primitive type,
// nesting, empty containers, the short-string boundary, and out-of-order
// object fields (the builder keeps values in event order while sorting
// header entries by name).
func builderTestValues() []Value {
	long := bytes.Repeat([]byte("x"), 100)
	return []Value{
		Null(),
		Bool(true),
		Bool(false),
		Int8(-5),
		Int16(1234),
		Int32(-123456),
		Int64(1 << 40),
		Float(1.5),
		Double(-2.75),
		String("short"),
		String(string(long)),
		Binary([]byte{0, 1, 2, 255}),
		Date(20000),
		Time(86399_000_000),
		Timestamp(1700000000_000_000),
		TimestampNTZ(1700000000_000_000),
		TimestampNanos(1700000000_000_000_000),
		TimestampNTZNanos(1700000000_000_000_000),
		UUID(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")),
		Decimal4(1234, 2),
		Decimal8(-123456789, 5),
		Decimal16([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 10),
		MakeObject(nil),
		MakeArray(nil),
		MakeObject([]Field{
			// Deliberately not in name order.
			{Name: "z", Value: Int32(1)},
			{Name: "a", Value: String("v")},
			{Name: "m", Value: MakeArray([]Value{Null(), Bool(true)})},
		}),
		MakeArray([]Value{
			MakeObject([]Field{{Name: "nested", Value: MakeArray([]Value{Int64(9)})}}),
			String("elem"),
			MakeObject(nil),
		}),
	}
}

// TestBuilderDifferential verifies that streaming a value through Builder
// produces (metadata, value) bytes that decode back to a value Equal to the
// original.
func TestBuilderDifferential(t *testing.T) {
	values := builderTestValues()

	r := rand.New(rand.NewPCG(7, 13))
	for range 50 {
		values = append(values, randomBuilderValue(r, 0))
	}

	for i, v := range values {
		t.Run(fmt.Sprintf("value_%02d", i), func(t *testing.T) {
			var b Builder
			v.Write(&b)
			metadata, value, err := b.Finish()
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}
			m, err := DecodeMetadata(metadata)
			if err != nil {
				t.Fatalf("decoding metadata: %v", err)
			}
			got, err := Decode(m, value)
			if err != nil {
				t.Fatalf("decoding value: %v", err)
			}
			if !got.Equal(v) {
				t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got.GoValue(), v.GoValue())
			}
		})
	}
}

// TestBuilderMatchesEncode asserts the streaming builder produces exactly
// the bytes of the tree encoder for the same value when object fields
// arrive in sorted order. (For unsorted arrival the builder keeps values in
// event order with non-monotonic offsets — spec-valid but not canonical —
// so values are canonicalized before comparing.) This pins layout, size
// selection, and metadata dictionary contents to the tree encoder's.
func TestBuilderMatchesEncode(t *testing.T) {
	values := builderTestValues()
	r := rand.New(rand.NewPCG(21, 42))
	for range 50 {
		values = append(values, randomBuilderValue(r, 0))
	}

	for i, v := range values {
		v := sortObjectFields(v)
		t.Run(fmt.Sprintf("value_%02d", i), func(t *testing.T) {
			var b Builder
			v.Write(&b)
			gotMeta, gotValue, err := b.Finish()
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}

			var mb MetadataBuilder
			wantValue := Encode(&mb, v)
			wantMeta := mb.AppendTo(nil)

			if !bytes.Equal(gotValue, wantValue) {
				t.Errorf("value bytes mismatch:\n got: %x\nwant: %x", gotValue, wantValue)
			}
			if !bytes.Equal(gotMeta, wantMeta) {
				t.Errorf("metadata bytes mismatch:\n got: %x\nwant: %x", gotMeta, wantMeta)
			}
		})
	}
}

// TestBuilderBoundaries pins the builder's size selection at the exact
// is_large and offset-width transitions by byte-comparing against the tree
// encoder: 255/256 container elements (1- vs 4-byte counts) and content
// sizes crossing the 1-, 2-, and 3-byte offset widths.
func TestBuilderBoundaries(t *testing.T) {
	object := func(n int) Value {
		fields := make([]Field, n)
		for i := range fields {
			fields[i] = Field{Name: fmt.Sprintf("f%04d", i), Value: Int64(int64(i))}
		}
		return MakeObject(fields)
	}
	array := func(n int) Value {
		elems := make([]Value, n)
		for i := range elems {
			elems[i] = Int64(int64(i))
		}
		return MakeArray(elems)
	}
	// One string element of the given length dominates the array's content
	// size, driving the offset width.
	sized := func(contentLen int) Value {
		return MakeArray([]Value{String(strings.Repeat("x", contentLen))})
	}

	tests := []struct {
		name  string
		value Value
	}{
		{name: "object_255_fields", value: object(255)},
		{name: "object_256_fields", value: object(256)},
		{name: "array_255_elements", value: array(255)},
		{name: "array_256_elements", value: array(256)},
		{name: "offsets_1_byte_max", value: sized(250)},
		{name: "offsets_2_bytes", value: sized(300)},
		{name: "offsets_2_bytes_max", value: sized(65500)},
		{name: "offsets_3_bytes", value: sized(65600)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var b Builder
			test.value.Write(&b)
			gotMeta, gotValue, err := b.Finish()
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}

			var mb MetadataBuilder
			wantValue := Encode(&mb, test.value)
			wantMeta := mb.AppendTo(nil)

			if !bytes.Equal(gotValue, wantValue) {
				t.Errorf("value bytes mismatch (%d vs %d bytes)", len(gotValue), len(wantValue))
			}
			if !bytes.Equal(gotMeta, wantMeta) {
				t.Errorf("metadata bytes mismatch (%d vs %d bytes)", len(gotMeta), len(wantMeta))
			}

			// And the bytes must round-trip.
			m, err := DecodeMetadata(gotMeta)
			if err != nil {
				t.Fatalf("decoding metadata: %v", err)
			}
			got, err := Decode(m, gotValue)
			if err != nil {
				t.Fatalf("decoding value: %v", err)
			}
			if !got.Equal(test.value) {
				t.Fatal("round trip mismatch")
			}
		})
	}
}

// TestMetadataBuilderSlabGrowth forces the dictionary slab to reallocate
// while entries already exist, verifying that lookups and encoding still
// see the original strings after the backing array moves.
func TestMetadataBuilderSlabGrowth(t *testing.T) {
	var b MetadataBuilder
	const n = 2000
	for i := range n {
		name := fmt.Sprintf("k%04d", i)
		if got := b.Add(name); got != i {
			t.Fatalf("Add(%q) = %d, want %d", name, got, i)
		}
	}
	for i := range n {
		name := fmt.Sprintf("k%04d", i)
		if got := b.Add(name); got != i {
			t.Fatalf("re-Add(%q) = %d, want %d (slab reindex lost the entry)", name, got, i)
		}
	}
	meta, encoded := b.Build()
	if len(meta.Strings) != n {
		t.Fatalf("Build: %d strings, want %d", len(meta.Strings), n)
	}
	for i := range n {
		want := fmt.Sprintf("k%04d", i)
		if meta.Strings[i] != want {
			t.Fatalf("Build string %d = %q, want %q", i, meta.Strings[i], want)
		}
	}
	m, err := DecodeMetadata(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Strings) != n {
		t.Fatalf("decoded %d strings, want %d", len(m.Strings), n)
	}
	for i := range n {
		want := fmt.Sprintf("k%04d", i)
		if m.Strings[i] != want {
			t.Fatalf("decoded string %d = %q, want %q", i, m.Strings[i], want)
		}
	}
}

// TestBuilderFieldCopiesName verifies Field copies the name argument so a
// caller can reuse a scratch buffer (the JSON-transcoder pattern) without
// corrupting the metadata dictionary or the object's field-id sort.
func TestBuilderFieldCopiesName(t *testing.T) {
	var b Builder
	b.BeginObject()
	buf := []byte("name")
	b.Field(unsafeString(buf))
	copy(buf, "xxxx") // mutate the scratch after Field returns
	b.String("alice")
	b.EndObject()
	metadata, value, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(m, value)
	if err != nil {
		t.Fatal(err)
	}
	want := MakeObject([]Field{{Name: "name", Value: String("alice")}})
	if !got.Equal(want) {
		t.Fatalf("got %#v, want %#v (field name was corrupted by scratch reuse)", got.GoValue(), want.GoValue())
	}
	if !slices.Contains(m.Strings, "name") {
		t.Errorf("metadata dictionary %q missing original field name", m.Strings)
	}
	if slices.Contains(m.Strings, "xxxx") {
		t.Errorf("metadata dictionary retained the mutated scratch: %q", m.Strings)
	}
}

func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// TestBuilderSparseFieldIDs encodes a small object against a shared
// dictionary already holding 300 names, forcing 2-byte field IDs for an
// object whose own field count would fit 1-byte IDs. The field-ID width is
// selected from the maximum ID, not the field count — exactly the shape a
// shredded-row residual takes when the row dictionary is wide.
func TestBuilderSparseFieldIDs(t *testing.T) {
	var meta MetadataBuilder
	for i := range 300 {
		meta.Add(fmt.Sprintf("name%03d", i))
	}

	b := NewBuilderWithMetadata(&meta)
	want := MakeObject([]Field{{Name: "name299", Value: Int64(1)}, {Name: "wide", Value: String("x")}})
	want.Write(b)
	value, err := b.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}

	m, err := DecodeMetadata(meta.AppendTo(nil))
	if err != nil {
		t.Fatalf("decoding metadata: %v", err)
	}
	got, err := Decode(m, value)
	if err != nil {
		t.Fatalf("decoding value: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got.GoValue(), want.GoValue())
	}

	// field_id_size must be 2 bytes: header info bits (offset_size_minus_one
	// at bits 2-3, field_id_size_minus_one at bits 4-5).
	if fieldIDSize := int(value[0]>>4)&0x03 + 1; fieldIDSize != 2 {
		t.Errorf("field_id_size = %d, want 2 (max field id 300)", fieldIDSize)
	}
}

// sortObjectFields returns a copy of v with all object fields recursively
// sorted by name, the canonical layout the tree encoder emits.
func sortObjectFields(v Value) Value {
	switch v.basic {
	case BasicObject:
		fields := slices.Clone(v.object.Fields)
		for i := range fields {
			fields[i].Value = sortObjectFields(fields[i].Value)
		}
		slices.SortFunc(fields, func(a, b Field) int { return strings.Compare(a.Name, b.Name) })
		return MakeObject(fields)
	case BasicArray:
		elems := slices.Clone(v.array.Elements)
		for i := range elems {
			elems[i] = sortObjectFields(elems[i])
		}
		return MakeArray(elems)
	default:
		return v
	}
}

func randomBuilderValue(r *rand.Rand, depth int) Value {
	if depth < 3 && r.IntN(3) == 0 {
		if r.IntN(2) == 0 {
			n := r.IntN(5)
			fields := make([]Field, 0, n)
			for i := range n {
				fields = append(fields, Field{
					Name:  fmt.Sprintf("f%d", (i*7+r.IntN(3))%11),
					Value: randomBuilderValue(r, depth+1),
				})
			}
			// Deduplicate names (objects require unique field names).
			seen := map[string]bool{}
			uniq := fields[:0]
			for _, f := range fields {
				if !seen[f.Name] {
					seen[f.Name] = true
					uniq = append(uniq, f)
				}
			}
			return MakeObject(uniq)
		}
		n := r.IntN(5)
		elems := make([]Value, n)
		for i := range elems {
			elems[i] = randomBuilderValue(r, depth+1)
		}
		return MakeArray(elems)
	}
	switch r.IntN(8) {
	case 0:
		return Null()
	case 1:
		return Bool(r.IntN(2) == 0)
	case 2:
		return Int64(int64(r.Uint64()))
	case 3:
		return Double(r.NormFloat64())
	case 4:
		return String(string(bytes.Repeat([]byte("s"), r.IntN(80))))
	case 5:
		return Decimal8(int64(r.Uint64()), byte(r.IntN(20)))
	case 6:
		return Date(int32(r.IntN(40000)))
	default:
		return Binary([]byte{byte(r.Uint32())})
	}
}

// TestBuilderLargeContainers exercises the is_large header paths (more than
// 255 fields / elements) and multi-byte offsets.
func TestBuilderLargeContainers(t *testing.T) {
	var b Builder
	b.BeginObject()
	want := make([]Field, 300)
	for i := range want {
		name := fmt.Sprintf("field_%03d", i)
		b.Field(name)
		b.String(fmt.Sprintf("value_%03d", i))
		want[i] = Field{Name: name, Value: String(fmt.Sprintf("value_%03d", i))}
	}
	b.EndObject()
	metadata, value, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	m, err := DecodeMetadata(metadata)
	if err != nil {
		t.Fatalf("decoding metadata: %v", err)
	}
	got, err := Decode(m, value)
	if err != nil {
		t.Fatalf("decoding value: %v", err)
	}
	if !got.Equal(MakeObject(want)) {
		t.Fatal("large object round trip mismatch")
	}

	b.Reset()
	b.BeginArray()
	elems := make([]Value, 400)
	for i := range elems {
		elems[i] = Int32(int32(i))
		b.Int32(int32(i))
	}
	b.EndArray()
	metadata, value, err = b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	m, err = DecodeMetadata(metadata)
	if err != nil {
		t.Fatalf("decoding metadata: %v", err)
	}
	got, err = Decode(m, value)
	if err != nil {
		t.Fatalf("decoding value: %v", err)
	}
	if !got.Equal(MakeArray(elems)) {
		t.Fatal("large array round trip mismatch")
	}
}

// TestBuilderSharedMetadata checks that multiple builders interning into one
// shared dictionary produce values decodable against the single dictionary.
func TestBuilderSharedMetadata(t *testing.T) {
	var meta MetadataBuilder
	b1 := NewBuilderWithMetadata(&meta)
	b2 := NewBuilderWithMetadata(&meta)

	MakeObject([]Field{{Name: "shared", Value: Int32(1)}, {Name: "one", Value: Null()}}).Write(b1)
	MakeObject([]Field{{Name: "shared", Value: Int32(2)}, {Name: "two", Value: Null()}}).Write(b2)

	v1, err := b1.Bytes()
	if err != nil {
		t.Fatalf("b1.Bytes: %v", err)
	}
	v2, err := b2.Bytes()
	if err != nil {
		t.Fatalf("b2.Bytes: %v", err)
	}
	m, err := DecodeMetadata(meta.AppendTo(nil))
	if err != nil {
		t.Fatalf("decoding metadata: %v", err)
	}
	got1, err := Decode(m, v1)
	if err != nil {
		t.Fatalf("decoding v1: %v", err)
	}
	got2, err := Decode(m, v2)
	if err != nil {
		t.Fatalf("decoding v2: %v", err)
	}
	if !got1.Equal(MakeObject([]Field{{Name: "shared", Value: Int32(1)}, {Name: "one", Value: Null()}})) {
		t.Errorf("v1 mismatch: %#v", got1.GoValue())
	}
	if !got2.Equal(MakeObject([]Field{{Name: "shared", Value: Int32(2)}, {Name: "two", Value: Null()}})) {
		t.Errorf("v2 mismatch: %#v", got2.GoValue())
	}

	// Resetting a shared-metadata builder must not clear the shared
	// dictionary.
	b1.Reset()
	if len(meta.offs) == 0 {
		t.Error("Reset cleared the shared metadata dictionary")
	}
}

// TestBuilderMisuse checks that malformed event sequences produce sticky
// errors instead of corrupt output.
func TestBuilderMisuse(t *testing.T) {
	tests := []struct {
		name  string
		drive func(b *Builder)
	}{
		{"two top-level values", func(b *Builder) { b.Int32(1); b.Int32(2) }},
		{"value without Field", func(b *Builder) { b.BeginObject(); b.Int32(1) }},
		{"Field outside object", func(b *Builder) { b.Field("x") }},
		{"Field in array", func(b *Builder) { b.BeginArray(); b.Field("x") }},
		{"double Field", func(b *Builder) { b.BeginObject(); b.Field("x"); b.Field("y") }},
		{"EndObject with pending field", func(b *Builder) { b.BeginObject(); b.Field("x"); b.EndObject() }},
		{"EndObject without Begin", func(b *Builder) { b.EndObject() }},
		{"EndArray without Begin", func(b *Builder) { b.EndArray() }},
		{"EndArray closes object", func(b *Builder) { b.BeginObject(); b.EndArray() }},
		{"EndObject closes array", func(b *Builder) { b.BeginArray(); b.EndObject() }},
		{"duplicate field name", func(b *Builder) {
			b.BeginObject()
			b.Field("x")
			b.Int32(1)
			b.Field("x")
			b.Int32(2)
			b.EndObject()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b Builder
			tc.drive(&b)
			if b.Err() == nil {
				// Errors like unclosed containers only surface at Finish.
				if _, _, err := b.Finish(); err == nil {
					t.Fatal("expected an error")
				}
			}
			// Sticky: further events must not clear it.
			b.Int32(3)
			if _, _, err := b.Finish(); err == nil {
				t.Fatal("expected the error to be sticky")
			}
		})
	}

	t.Run("unclosed container", func(t *testing.T) {
		var b Builder
		b.BeginArray()
		if _, _, err := b.Finish(); err == nil {
			t.Fatal("expected an error for unclosed array")
		}
	})
	t.Run("no value", func(t *testing.T) {
		var b Builder
		if _, _, err := b.Finish(); err == nil {
			t.Fatal("expected an error for empty builder")
		}
	})
}

// TestBuilderReset checks that a reused builder produces clean output with
// no residue from the previous value.
func TestBuilderReset(t *testing.T) {
	var b Builder
	MakeObject([]Field{{Name: "first", Value: Int32(1)}}).Write(&b)
	if _, _, err := b.Finish(); err != nil {
		t.Fatalf("first Finish: %v", err)
	}

	b.Reset()
	MakeArray([]Value{String("second")}).Write(&b)
	metadata, value, err := b.Finish()
	if err != nil {
		t.Fatalf("second Finish: %v", err)
	}
	m, err := DecodeMetadata(metadata)
	if err != nil {
		t.Fatalf("decoding metadata: %v", err)
	}
	if len(m.Strings) != 0 {
		t.Errorf("metadata after Reset contains stale strings: %v", m.Strings)
	}
	got, err := Decode(m, value)
	if err != nil {
		t.Fatalf("decoding value: %v", err)
	}
	if !got.Equal(MakeArray([]Value{String("second")})) {
		t.Errorf("mismatch after Reset: %#v", got.GoValue())
	}
}

// TestBuilderAllocations checks that a warmed-up builder encodes values with
// zero allocations.
func TestBuilderAllocations(t *testing.T) {
	v := MakeObject([]Field{
		{Name: "a", Value: Int64(1)},
		{Name: "b", Value: MakeArray([]Value{String("x"), String("y")})},
	})
	var b Builder
	v.Write(&b)
	if _, _, err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		b.Reset()
		v.Write(&b)
		if _, err := b.Bytes(); err != nil {
			t.Fatal(err)
		}
	})
	// Reset clears the owned dictionary, so each run re-interns the two
	// field names; map writes may allocate. Everything else must not.
	if allocs > 2 {
		t.Errorf("allocations per encode: %v, want <= 2", allocs)
	}
}
