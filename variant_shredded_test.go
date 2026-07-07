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
