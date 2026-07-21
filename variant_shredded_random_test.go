package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// This file property-tests the shredded write→read round trip over
// randomly generated shredding schemas and randomly generated variant
// values. Schemas and values draw field names and primitive types from the
// same small pools, so every relationship of the VariantShredding.md case
// table arises statistically: exact matches into typed_value, type
// mismatches falling back to value, partially shredded objects with
// residual fields, missing fields, empty objects and arrays, variant
// nulls, and deeply nested combinations of all of these.
//
// The property: for any shredding schema S and any variant value v,
// writing v through ShreddedVariant(S) and reading it back yields a value
// structurally equal to v. Values enter and leave as raw variant binary,
// so every variant type participates, not just the ones with Go-native
// mappings.

// shreddedVariantWritePaths writes rows through every write path the
// library has: the column-buffer path used by GenericWriter.Write, the
// buffer path used by GenericBuffer + WriteRowGroup, and the row-based
// deconstruct path used when callers build parquet.Row values themselves.
// All three share the shredding logic but drive it through different
// plumbing, so each must produce a file that reads back identically.
var shreddedVariantWritePaths = map[string]func(schema *parquet.Schema, rows []shreddedVariantRow) (*bytes.Buffer, error){
	"generic_writer": func(schema *parquet.Schema, rows []shreddedVariantRow) (*bytes.Buffer, error) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
		if _, err := w.Write(rows); err != nil {
			return nil, err
		}
		return buf, w.Close()
	},
	"buffer_row_group": func(schema *parquet.Schema, rows []shreddedVariantRow) (*bytes.Buffer, error) {
		buffer := parquet.NewGenericBuffer[shreddedVariantRow](schema)
		if _, err := buffer.Write(rows); err != nil {
			return nil, err
		}
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
		if _, err := w.WriteRowGroup(buffer); err != nil {
			return nil, err
		}
		return buf, w.Close()
	},
	"deconstruct_rows": func(schema *parquet.Schema, rows []shreddedVariantRow) (*bytes.Buffer, error) {
		buf := new(bytes.Buffer)
		w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
		deconstructed := make([]parquet.Row, len(rows))
		for i := range rows {
			deconstructed[i] = schema.Deconstruct(nil, &rows[i])
		}
		if _, err := w.WriteRows(deconstructed); err != nil {
			return nil, err
		}
		return buf, w.Close()
	},
}

// shreddedVariantReadPaths reads a written file back as raw variant rows:
// through schema conversion to an unshredded target (Convert, see
// convert_variant.go), and directly through the file's own shredded schema
// (reconstructFuncOfShreddedVariant).
var shreddedVariantReadPaths = map[string]func(buf *bytes.Buffer, fileSchema *parquet.Schema, n int) ([]rawVariantRow, error){
	"convert": func(buf *bytes.Buffer, _ *parquet.Schema, _ int) ([]rawVariantRow, error) {
		return parquet.Read[rawVariantRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	},
	"direct": func(buf *bytes.Buffer, fileSchema *parquet.Schema, n int) ([]rawVariantRow, error) {
		type row struct {
			ID  int32      `parquet:"id"`
			Var rawVariant `parquet:"var,variant"`
		}
		r := parquet.NewGenericReader[row](bytes.NewReader(buf.Bytes()), fileSchema)
		defer r.Close()
		rows := make([]row, n)
		if _, err := r.Read(rows); err != nil && err != io.EOF {
			return nil, err
		}
		out := make([]rawVariantRow, n)
		for i, rw := range rows {
			out[i] = rawVariantRow{ID: rw.ID, Var: rw.Var}
		}
		return out, nil
	},
	"legacy_reader": func(buf *bytes.Buffer, _ *parquet.Schema, n int) ([]rawVariantRow, error) {
		readSchema := parquet.NewSchema("table", parquet.Group{
			"id":  parquet.Int(32),
			"var": parquet.Variant(),
		})
		r := parquet.NewReader(bytes.NewReader(buf.Bytes()), readSchema)
		defer r.Close()
		out := make([]rawVariantRow, n)
		for i := range out {
			if err := r.Read(&out[i]); err != nil {
				return nil, err
			}
		}
		return out, nil
	},
}

// shreddedRoundTrip writes the given variant values through a shredded
// variant column with the given typed_value schema (or an unshredded
// variant column when shred is nil) and asserts that every row reads back
// structurally equal, for every write path × read path combination.
func shreddedRoundTrip(t *testing.T, shred parquet.Node, values []variant.Value) {
	t.Helper()
	variantNode := parquet.Variant()
	if shred != nil {
		shredded, err := parquet.ShreddedVariant(shred)
		if err != nil {
			t.Fatalf("building shredded variant node: %v", err)
		}
		variantNode = shredded
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": variantNode,
	})

	rows := make([]shreddedVariantRow, len(values))
	for i, v := range values {
		rows[i] = shreddedVariantRow{ID: int32(i), Var: encodeRawVariant(v)}
	}

	for writeName, write := range shreddedVariantWritePaths {
		buf, err := write(schema, rows)
		if err != nil {
			t.Errorf("schema:\n%s\nwrite path %s: %v", schema, writeName, err)
			continue
		}
		for readName, read := range shreddedVariantReadPaths {
			got, err := read(buf, schema, len(values))
			if err != nil {
				t.Errorf("schema:\n%s\n%s→%s: reading: %v", schema, writeName, readName, err)
				continue
			}
			if len(got) != len(values) {
				t.Errorf("schema:\n%s\n%s→%s: read %d rows, want %d", schema, writeName, readName, len(got), len(values))
				continue
			}
			for i, want := range values {
				decoded, err := decodeRawVariant(got[i].Var)
				if err != nil {
					t.Errorf("schema:\n%s\n%s→%s: row %d (%#v): decoding read-back value: %v",
						schema, writeName, readName, i, want.GoValue(), err)
					continue
				}
				if !decoded.Equal(want) {
					t.Errorf("schema:\n%s\n%s→%s: row %d round trip mismatch:\n got: %#v\nwant: %#v",
						schema, writeName, readName, i, decoded.GoValue(), want.GoValue())
				}
			}
		}
	}
}

func TestShreddedVariantRandomizedRoundTrip(t *testing.T) {
	const numSchemas, rowsPerSchema = 64, 8
	r := rand.New(rand.NewPCG(0, 1))
	for i := range numSchemas {
		t.Run(fmt.Sprintf("schema_%02d", i), func(t *testing.T) {
			shred := randomShredNode(r, 0)
			values := make([]variant.Value, rowsPerSchema)
			for j := range values {
				values[j] = randomVariant(r, 0)
			}
			shreddedRoundTrip(t, shred, values)
		})
	}
}

// TestUnshreddedVariantRandomizedRoundTrip runs the same values through a
// plain unshredded variant column, covering the unshredded write and read
// paths of every write path × read path combination.
func TestUnshreddedVariantRandomizedRoundTrip(t *testing.T) {
	const numBatches, rowsPerBatch = 16, 8
	r := rand.New(rand.NewPCG(2, 3))
	for i := range numBatches {
		t.Run(fmt.Sprintf("batch_%02d", i), func(t *testing.T) {
			values := make([]variant.Value, rowsPerBatch)
			for j := range values {
				values[j] = randomVariant(r, 0)
			}
			shreddedRoundTrip(t, nil, values)
		})
	}
}

// FuzzShreddedVariantRoundTrip lets the fuzzer explore the same property
// over arbitrary generator seeds.
func FuzzShreddedVariantRoundTrip(f *testing.F) {
	for seed := range uint64(8) {
		f.Add(seed, seed*2654435761)
	}
	f.Fuzz(func(t *testing.T, s1, s2 uint64) {
		r := rand.New(rand.NewPCG(s1, s2))
		shred := randomShredNode(r, 0)
		values := make([]variant.Value, 4)
		for i := range values {
			values[i] = randomVariant(r, 0)
		}
		shreddedRoundTrip(t, shred, values)
	})
}
