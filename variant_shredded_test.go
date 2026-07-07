package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// The testdata/shredded_variant directory contains a subset of the canonical
// shredded VARIANT test files from apache/parquet-testing (written by
// parquet-java). Each case-*.parquet file has columns (id, var) where var is
// a shredded variant group; the expected reconstruction of row N is stored
// in case-*_row-N.variant.bin as serialized variant metadata directly
// followed by the serialized value.

type rawVariant struct {
	Metadata []byte `parquet:"metadata"`
	Value    []byte `parquet:"value"`
}

type rawVariantRow struct {
	ID  int32      `parquet:"id"`
	Var rawVariant `parquet:"var,variant"`
}

// splitVariantBin splits a .variant.bin golden (metadata directly followed
// by value) into its two parts by computing the metadata's encoded size
// from its header.
func splitVariantBin(data []byte) (metadata, value []byte, err error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty variant binary")
	}
	offsetSize := int(data[0]>>6)&0x03 + 1
	if len(data) < 1+offsetSize {
		return nil, nil, fmt.Errorf("variant binary too short for dictionary size")
	}
	readUint := func(b []byte) int {
		var v uint32
		for i := range offsetSize {
			v |= uint32(b[i]) << (8 * i)
		}
		return int(v)
	}
	dictSize := readUint(data[1:])
	lastOffsetPos := 1 + offsetSize + dictSize*offsetSize
	if len(data) < lastOffsetPos+offsetSize {
		return nil, nil, fmt.Errorf("variant binary too short for offsets")
	}
	stringsLen := readUint(data[lastOffsetPos:])
	metadataLen := lastOffsetPos + offsetSize + stringsLen
	if len(data) < metadataLen {
		return nil, nil, fmt.Errorf("variant binary too short for dictionary strings")
	}
	return data[:metadataLen], data[metadataLen:], nil
}

func decodeVariantGolden(t *testing.T, path string) variant.Value {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden: %v", err)
	}
	metaBytes, valBytes, err := splitVariantBin(data)
	if err != nil {
		t.Fatalf("splitting golden: %v", err)
	}
	m, err := variant.DecodeMetadata(metaBytes)
	if err != nil {
		t.Fatalf("decoding golden metadata: %v", err)
	}
	v, err := variant.Decode(m, valBytes)
	if err != nil {
		t.Fatalf("decoding golden value: %v", err)
	}
	return v
}

func decodeRawVariant(raw rawVariant) (variant.Value, error) {
	m, err := variant.DecodeMetadata(raw.Metadata)
	if err != nil {
		return variant.Null(), fmt.Errorf("metadata: %w", err)
	}
	v, err := variant.Decode(m, raw.Value)
	if err != nil {
		return variant.Null(), fmt.Errorf("value: %w", err)
	}
	return v, nil
}

// TestShreddedVariantSpecRead reads canonical parquet-java-written shredded
// variant files with an unshredded reader schema and verifies that
// construct_variant reconstruction (see variant_shredded_read.go and
// convert_variant.go) produces the documented golden values. Before the
// conversion learned to reconstruct shredded variant columns, these files
// were read back as nil/empty values without error, because the generic
// column mapping dropped the typed_value columns.
func TestShreddedVariantSpecRead(t *testing.T) {
	cases := []struct {
		file    string
		desc    string
		goldens []string // expected reconstruction per row
		wantErr string   // non-empty: reading the file must fail
	}{
		{
			file:    "case-004.parquet",
			desc:    "fully shredded primitive: boolean typed_value, null value column",
			goldens: []string{"case-004_row-0.variant.bin"},
		},
		{
			file: "case-045.parquet",
			desc: "3-level LIST of shredded strings, plus rows falling back to the value column (scalar, partially shredded object)",
			goldens: []string{
				"case-045_row-0.variant.bin",
				"case-045_row-1.variant.bin",
				"case-045_row-2.variant.bin",
				"case-045_row-3.variant.bin",
			},
		},
		{
			file:    "case-134.parquet",
			desc:    "partially shredded object: shredded fields a (variant null in field value) and b (typed), residual field d in the object's value column",
			goldens: []string{"case-134_row-0.variant.bin"},
		},
		{
			file:    "case-042.parquet",
			desc:    "value and typed_value both non-null at the same position is invalid per the spec case table and must error",
			wantErr: "both non-null",
		},
	}

	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			path := filepath.Join("testdata", "shredded_variant", c.file)
			rows, err := parquet.ReadFile[rawVariantRow](path)

			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("%s: expected error containing %q, read %d rows", c.desc, c.wantErr, len(rows))
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("%s: error = %q, want error containing %q", c.desc, err, c.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("%s: reading file: %v", c.desc, err)
			}
			if len(rows) != len(c.goldens) {
				t.Fatalf("%s: read %d rows, want %d", c.desc, len(rows), len(c.goldens))
			}
			for i, golden := range c.goldens {
				want := decodeVariantGolden(t, filepath.Join("testdata", "shredded_variant", golden))
				got, err := decodeRawVariant(rows[i].Var)
				if err != nil {
					t.Errorf("%s: row %d: decoding reconstructed variant: %v", c.desc, i, err)
					continue
				}
				if !got.Equal(want) {
					t.Errorf("%s: row %d:\n got: %#v\nwant: %#v", c.desc, i, got.GoValue(), want.GoValue())
				}
			}
		})
	}
}

// TestShreddedVariantReadNestedOptionalGroup reads a shredded variant column
// nested inside an optional group with an unshredded reader schema. The
// conversion must map the levels of null occurrences through the enclosing
// optional wrapper; getting the mapping wrong shifts every definition level
// in the subtree and corrupts the read.
func TestShreddedVariantReadNestedOptionalGroup(t *testing.T) {
	shredded, err := parquet.ShreddedVariant(parquet.String())
	if err != nil {
		t.Fatal(err)
	}
	writeSchema := parquet.NewSchema("root", parquet.Group{
		"outer": parquet.Optional(parquet.Group{
			"var": shredded,
		}),
	})

	type outer struct {
		Var any `parquet:"var,variant"`
	}
	type row struct {
		Outer *outer `parquet:"outer"`
	}
	rows := []row{
		{Outer: &outer{Var: "hello"}},
		{Outer: nil},
		{Outer: &outer{Var: int64(42)}},
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, writeSchema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := parquet.Read[row](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("reading nested shredded variant: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d rows, want 3", len(got))
	}
	if got[0].Outer == nil || got[0].Outer.Var != "hello" {
		t.Errorf("row 0 = %+v, want outer.var=hello", got[0].Outer)
	}
	if got[1].Outer != nil {
		t.Errorf("row 1 outer = %+v, want nil", got[1].Outer)
	}
	if got[2].Outer == nil || got[2].Outer.Var != int64(42) {
		t.Errorf("row 2 = %+v, want outer.var=42", got[2].Outer)
	}
}

// TestShreddedVariantReadOptionalColumn reads an optional shredded variant
// column (rows where the whole variant group is null) with an optional
// unshredded reader schema. Null occurrences carry the enclosing level, not
// the group's own level, so the conversion must map them through the
// optionality of both schemas.
func TestShreddedVariantReadOptionalColumn(t *testing.T) {
	shredded, err := parquet.ShreddedVariant(parquet.String())
	if err != nil {
		t.Fatal(err)
	}
	writeSchema := parquet.NewSchema("root", parquet.Group{
		"var": parquet.Optional(shredded),
	})

	type row struct {
		Var any `parquet:"var,variant"`
	}
	rows := []row{{Var: "hello"}, {Var: nil}, {Var: int64(42)}}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, writeSchema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	readSchema := parquet.NewSchema("root", parquet.Group{
		"var": parquet.Optional(parquet.Variant()),
	})
	r := parquet.NewGenericReader[row](bytes.NewReader(buf.Bytes()), readSchema)
	defer r.Close()
	got := make([]row, 4)
	n, err := r.Read(got)
	if err != nil && err != io.EOF {
		t.Fatalf("reading optional shredded variant: %v", err)
	}
	if n != 3 {
		t.Fatalf("read %d rows, want 3", n)
	}
	if got[0].Var != "hello" {
		t.Errorf("row 0 = %#v, want hello", got[0].Var)
	}
	if got[1].Var != nil {
		t.Errorf("row 1 = %#v, want nil", got[1].Var)
	}
	if got[2].Var != int64(42) {
		t.Errorf("row 2 = %#v, want 42", got[2].Var)
	}
}

// TestShreddedVariantReadLegacyReader reads a shredded file through the
// non-generic Reader with an explicit unshredded schema. The Reader routes
// schema mismatches through Convert like the generic readers, so shredded
// reconstruction applies to it with no reader-specific code.
func TestShreddedVariantReadLegacyReader(t *testing.T) {
	shredded, err := parquet.ShreddedVariant(parquet.String())
	if err != nil {
		t.Fatal(err)
	}
	writeSchema := parquet.NewSchema("root", parquet.Group{"var": shredded})

	type row struct {
		Var any `parquet:"var,variant"`
	}
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, writeSchema)
	if _, err := w.Write([]row{{Var: "hello"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	readSchema := parquet.NewSchema("root", parquet.Group{"var": parquet.Variant()})
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()), readSchema)
	defer r.Close()

	var got row
	if err := r.Read(&got); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if got.Var != "hello" {
		t.Errorf("var = %#v, want hello", got.Var)
	}
}

// TestShreddedVariantConvertRepeatedColumn converts a repeated shredded
// variant column to an unshredded reader schema. Each occurrence of the
// group carries its own metadata and reconstructed value at the occurrence's
// repetition level.
func TestShreddedVariantConvertRepeatedColumn(t *testing.T) {
	shredded, err := parquet.ShreddedVariant(parquet.Int(64))
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.NewSchema("root", parquet.Group{
		"vars": parquet.Repeated(shredded),
	})

	type row struct {
		Vars []any `parquet:"vars,variant"`
	}
	rows := []row{
		{Vars: []any{int64(1), "fallback", int64(3)}},
		{Vars: []any{}},
		{Vars: []any{int64(9)}},
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	readSchema := parquet.NewSchema("root", parquet.Group{
		"vars": parquet.Repeated(parquet.Variant()),
	})
	r := parquet.NewGenericReader[row](bytes.NewReader(buf.Bytes()), readSchema)
	defer r.Close()
	got := make([]row, 4)
	n, err := r.Read(got)
	if err != nil && err != io.EOF {
		t.Fatalf("reading repeated shredded variant: %v", err)
	}
	got = got[:n]
	if len(got) != 3 {
		t.Fatalf("read %d rows, want 3", len(got))
	}
	want0 := []any{int64(1), "fallback", int64(3)}
	if len(got[0].Vars) != len(want0) {
		t.Fatalf("row 0 = %#v, want %v", got[0].Vars, want0)
	}
	for i := range want0 {
		if got[0].Vars[i] != want0[i] {
			t.Errorf("row 0 elem %d = %#v, want %#v", i, got[0].Vars[i], want0[i])
		}
	}
	if len(got[1].Vars) != 0 {
		t.Errorf("row 1 = %#v, want empty", got[1].Vars)
	}
	if len(got[2].Vars) != 1 || got[2].Vars[0] != int64(9) {
		t.Errorf("row 2 = %#v, want [9]", got[2].Vars)
	}
}

// TestShreddedVariantWriteLayout pins the physical column layout produced by
// the shredded variant writer against the case table of the Variant
// Shredding specification. Round-trip tests cannot catch layout bugs — a
// writer and reader that agree on a non-spec layout still round-trip (the
// original writer stored per-field fallbacks in a homegrown length-prefixed
// framing that only this package could read) — so this test asserts the
// column contents directly:
//
//   - values matching the shredded type go to typed_value, value is null
//   - a partially shredded object stores residual fields as a variant
//     object in value; shredded field names never appear in value
//   - a missing object field has both of its columns null
//   - a field whose value mismatches its shredded type falls back to the
//     field's value column, encoded against the row's shared metadata
//   - a non-object row leaves the whole typed_value subtree null
//   - the row metadata dictionary contains every field name, shredded or not
func TestShreddedVariantWriteLayout(t *testing.T) {
	shreddedNode, err := parquet.ShreddedVariant(parquet.Group{
		"a": parquet.Int(64),
		"b": parquet.String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": shreddedNode,
	})

	type row struct {
		ID  int32 `parquet:"id"`
		Var any   `parquet:"var,variant"`
	}
	rows := []row{
		{0, map[string]any{"a": int64(1), "b": "x", "extra": true}}, // partially shredded
		{1, map[string]any{"a": int64(2)}},                          // field b missing
		{2, "scalar"},                                               // non-object
		{3, map[string]any{"a": "wrong", "b": "y"}},                 // field a type mismatch
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[row](buf, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	// Collect the values of every leaf column under var, in row order.
	columns := make(map[string][]parquet.Value)
	err = forEachColumnValue(pf.Root(), func(leaf *parquet.Column, value parquet.Value) error {
		path := strings.Join(leaf.Path(), ".")
		columns[path] = append(columns[path], value.Clone())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Every row's metadata dictionary must contain all of the row's object
	// field names, including unshredded ones ("extra" in row 0).
	metadatas := make([]variant.Metadata, len(rows))
	for i, v := range columns["var.metadata"] {
		m, err := variant.DecodeMetadata(v.ByteArray())
		if err != nil {
			t.Fatalf("row %d: decoding metadata column: %v", i, err)
		}
		metadatas[i] = m
	}
	if got := metadatas[0].Strings; len(got) != 3 {
		t.Errorf("row 0 metadata dictionary = %q, want the field names a, b, extra", got)
	}

	// decodeAt decodes the variant value stored in a value column at a row,
	// using that row's metadata dictionary.
	decodeAt := func(path string, row int) variant.Value {
		t.Helper()
		v := columns[path][row]
		if v.IsNull() {
			t.Fatalf("%s row %d: expected a value, found null", path, row)
		}
		decoded, err := variant.Decode(metadatas[row], v.ByteArray())
		if err != nil {
			t.Fatalf("%s row %d: %v", path, row, err)
		}
		return decoded
	}
	assertNull := func(path string, row int) {
		t.Helper()
		if v := columns[path][row]; !v.IsNull() {
			t.Errorf("%s row %d = %v, want null", path, row, v)
		}
	}

	// Row 0: a and b shredded into typed_value, residual object {extra:
	// true} in value. The shredded names must not appear in value.
	if got := columns["var.typed_value.a.typed_value"][0].Int64(); got != 1 {
		t.Errorf("row 0 shredded a = %d, want 1", got)
	}
	if got := string(columns["var.typed_value.b.typed_value"][0].ByteArray()); got != "x" {
		t.Errorf("row 0 shredded b = %q, want %q", got, "x")
	}
	assertNull("var.typed_value.a.value", 0)
	assertNull("var.typed_value.b.value", 0)
	residual := decodeAt("var.value", 0)
	if fields := residual.ObjectValue().Fields; len(fields) != 1 || fields[0].Name != "extra" {
		t.Errorf("row 0 residual object = %#v, want only field %q", residual.GoValue(), "extra")
	}

	// Row 1: field b is missing, so both of its columns are null; nothing
	// goes to the object's value column.
	if got := columns["var.typed_value.a.typed_value"][1].Int64(); got != 2 {
		t.Errorf("row 1 shredded a = %d, want 2", got)
	}
	assertNull("var.typed_value.b.typed_value", 1)
	assertNull("var.typed_value.b.value", 1)
	assertNull("var.value", 1)

	// Row 2: a non-object cannot use the object typed_value; the whole
	// typed subtree is null and the value column holds the encoded string.
	for _, path := range []string{
		"var.typed_value.a.value", "var.typed_value.a.typed_value",
		"var.typed_value.b.value", "var.typed_value.b.typed_value",
	} {
		assertNull(path, 2)
	}
	if got := decodeAt("var.value", 2); got.Str() != "scalar" {
		t.Errorf("row 2 value = %#v, want the string %q", got.GoValue(), "scalar")
	}

	// Row 3: field a's string does not match its int64 shredded type, so it
	// falls back to field a's own value column (not the object's).
	assertNull("var.typed_value.a.typed_value", 3)
	if got := decodeAt("var.typed_value.a.value", 3); got.Str() != "wrong" {
		t.Errorf("row 3 field a fallback = %#v, want the string %q", got.GoValue(), "wrong")
	}
	if got := string(columns["var.typed_value.b.typed_value"][3].ByteArray()); got != "y" {
		t.Errorf("row 3 shredded b = %q, want %q", got, "y")
	}
	assertNull("var.value", 3)
}

// benchmarkShreddedRows builds a mixed workload for the shredding
// benchmarks: fully shredded objects, partially shredded objects with
// residual fields, and scalar fallbacks.
func benchmarkShreddedRows(n int) []struct {
	ID  int32 `parquet:"id"`
	Var any   `parquet:"var,variant"`
} {
	rows := make([]struct {
		ID  int32 `parquet:"id"`
		Var any   `parquet:"var,variant"`
	}, n)
	for i := range rows {
		rows[i].ID = int32(i)
		switch i % 3 {
		case 0:
			rows[i].Var = map[string]any{"a": int64(i), "b": "value"}
		case 1:
			rows[i].Var = map[string]any{"a": int64(i), "b": "value", "extra": float64(i)}
		default:
			rows[i].Var = "scalar fallback"
		}
	}
	return rows
}

func benchmarkShreddedSchema(b *testing.B) *parquet.Schema {
	shredded, err := parquet.ShreddedVariant(parquet.Group{
		"a": parquet.Int(64),
		"b": parquet.String(),
	})
	if err != nil {
		b.Fatal(err)
	}
	return parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": shredded,
	})
}

func BenchmarkShreddedVariantWrite(b *testing.B) {
	schema := benchmarkShreddedSchema(b)
	rows := benchmarkShreddedRows(1000)
	buf := new(bytes.Buffer)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		w := parquet.NewGenericWriter[struct {
			ID  int32 `parquet:"id"`
			Var any   `parquet:"var,variant"`
		}](buf, schema)
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(rows)*b.N)/b.Elapsed().Seconds(), "rows/s")
}

func BenchmarkShreddedVariantRead(b *testing.B) {
	schema := benchmarkShreddedSchema(b)
	rows := benchmarkShreddedRows(1000)
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[struct {
		ID  int32 `parquet:"id"`
		Var any   `parquet:"var,variant"`
	}](buf, schema)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	data := bytes.NewReader(buf.Bytes())
	b.ReportAllocs()
	for b.Loop() {
		got, err := parquet.Read[struct {
			ID  int32 `parquet:"id"`
			Var any   `parquet:"var,variant"`
		}](data, int64(buf.Len()))
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(rows) {
			b.Fatalf("read %d rows, want %d", len(got), len(rows))
		}
	}
	b.ReportMetric(float64(len(rows)*b.N)/b.Elapsed().Seconds(), "rows/s")
}
