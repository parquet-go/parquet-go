package parquet_test

import (
	"bytes"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// TestShreddedVariantTypedValueMatrix exhaustively exercises the shredded
// types table of the Variant Shredding specification: for every permitted
// typed_value leaf type it writes values that must shred (exact type match)
// and values that must not (everything else falls back to the value
// column), then verifies both the physical column placement and the
// read-back reconstruction.
//
// The randomized round-trip draws schemas and values independently, so
// exact type matches — especially for types with unit and adjustment
// parameters like timestamps and decimals — are rare there; this table
// pins every row of the spec's type table deterministically. Placement is
// asserted against the physical columns because a writer and reader that
// agree on the wrong column still round-trip.
func TestShreddedVariantTypedValueMatrix(t *testing.T) {
	longString := strings.Repeat("s", 100)
	u := uuid.MustParse("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")

	// Little-endian two's complement 16-byte decimals.
	le16 := func(lo uint64, negative bool) (out [16]byte) {
		fill := byte(0)
		if negative {
			fill = 0xFF
		}
		for i := range out {
			out[i] = fill
		}
		v := lo
		if negative {
			v = ^lo + 1 // two's complement magnitude for small negatives
		}
		for i := range 8 {
			out[i] = byte(v >> (8 * i))
		}
		if negative {
			for i := 8; i < 16; i++ {
				out[i] = 0xFF
			}
		}
		return out
	}

	cases := []struct {
		name     string
		node     parquet.Node
		match    []variant.Value // must land in typed_value
		mismatch []variant.Value // must fall back to the value column
	}{
		{
			name:     "boolean",
			node:     parquet.Leaf(parquet.BooleanType),
			match:    []variant.Value{variant.Bool(true), variant.Bool(false)},
			mismatch: []variant.Value{variant.Int32(1), variant.String("true")},
		},
		{
			name:  "int8",
			node:  parquet.Int(8),
			match: []variant.Value{variant.Int8(math.MinInt8), variant.Int8(math.MaxInt8)},
			// Exact matching: no widening of narrower values, no narrowing
			// of wider values even when they would fit.
			mismatch: []variant.Value{variant.Int16(1), variant.Int32(1), variant.Int64(1)},
		},
		{
			name:     "int16",
			node:     parquet.Int(16),
			match:    []variant.Value{variant.Int16(math.MinInt16), variant.Int16(math.MaxInt16)},
			mismatch: []variant.Value{variant.Int8(1), variant.Int32(1)},
		},
		{
			name:     "int32",
			node:     parquet.Int(32),
			match:    []variant.Value{variant.Int32(math.MinInt32), variant.Int32(math.MaxInt32)},
			mismatch: []variant.Value{variant.Int16(1), variant.Int64(1)},
		},
		{
			name:     "int64",
			node:     parquet.Int(64),
			match:    []variant.Value{variant.Int64(math.MinInt64), variant.Int64(math.MaxInt64)},
			mismatch: []variant.Value{variant.Int32(1), variant.Double(1)},
		},
		{
			// INT32/INT64 physical types without a logical annotation are
			// equivalent to INT(32)/INT(64) per the Parquet spec.
			name:     "int32_plain",
			node:     parquet.Leaf(parquet.Int32Type),
			match:    []variant.Value{variant.Int32(-5)},
			mismatch: []variant.Value{variant.Int64(5)},
		},
		{
			name:     "int64_plain",
			node:     parquet.Leaf(parquet.Int64Type),
			match:    []variant.Value{variant.Int64(-6)},
			mismatch: []variant.Value{variant.Int32(6)},
		},
		{
			name:     "float",
			node:     parquet.Leaf(parquet.FloatType),
			match:    []variant.Value{variant.Float(1.5), variant.Float(float32(math.NaN()))},
			mismatch: []variant.Value{variant.Double(1.5), variant.Int32(1)},
		},
		{
			name:     "double",
			node:     parquet.Leaf(parquet.DoubleType),
			match:    []variant.Value{variant.Double(-2.5), variant.Double(math.Inf(1))},
			mismatch: []variant.Value{variant.Float(2.5), variant.Int64(1)},
		},
		{
			name:     "string",
			node:     parquet.String(),
			match:    []variant.Value{variant.String(""), variant.String(longString)},
			mismatch: []variant.Value{variant.Binary([]byte("bytes")), variant.Int32(1)},
		},
		{
			name:     "binary",
			node:     parquet.Leaf(parquet.ByteArrayType),
			match:    []variant.Value{variant.Binary(nil), variant.Binary([]byte{0, 1, 2})},
			mismatch: []variant.Value{variant.String("text"), variant.UUID(u)},
		},
		{
			name:     "date",
			node:     parquet.Date(),
			match:    []variant.Value{variant.Date(-719162), variant.Date(19000)},
			mismatch: []variant.Value{variant.Int32(19000), variant.Timestamp(1)},
		},
		{
			name:     "time_micros",
			node:     parquet.TimeAdjusted(parquet.Microsecond, false),
			match:    []variant.Value{variant.Time(0), variant.Time(86399999999)},
			mismatch: []variant.Value{variant.Int64(1), variant.Timestamp(1)},
		},
		{
			name:  "timestamp_micros_utc",
			node:  parquet.TimestampAdjusted(parquet.Microsecond, true),
			match: []variant.Value{variant.Timestamp(math.MinInt64), variant.Timestamp(1)},
			// Unit and adjustment must both match exactly.
			mismatch: []variant.Value{variant.TimestampNTZ(1), variant.TimestampNanos(1), variant.Int64(1)},
		},
		{
			name:     "timestamp_micros_ntz",
			node:     parquet.TimestampAdjusted(parquet.Microsecond, false),
			match:    []variant.Value{variant.TimestampNTZ(-1), variant.TimestampNTZ(1)},
			mismatch: []variant.Value{variant.Timestamp(1), variant.TimestampNTZNanos(1)},
		},
		{
			name:     "timestamp_nanos_utc",
			node:     parquet.TimestampAdjusted(parquet.Nanosecond, true),
			match:    []variant.Value{variant.TimestampNanos(math.MaxInt64), variant.TimestampNanos(0)},
			mismatch: []variant.Value{variant.Timestamp(1), variant.TimestampNTZNanos(1)},
		},
		{
			name:     "timestamp_nanos_ntz",
			node:     parquet.TimestampAdjusted(parquet.Nanosecond, false),
			match:    []variant.Value{variant.TimestampNTZNanos(-1), variant.TimestampNTZNanos(1)},
			mismatch: []variant.Value{variant.TimestampNanos(1), variant.TimestampNTZ(1)},
		},
		{
			name:     "uuid",
			node:     parquet.UUID(),
			match:    []variant.Value{variant.UUID(u), variant.UUID(uuid.UUID{})},
			mismatch: []variant.Value{variant.Binary(u[:]), variant.String(u.String())},
		},
		{
			name:  "decimal4",
			node:  parquet.Decimal(2, 9, parquet.Int32Type),
			match: []variant.Value{variant.Decimal4(999_999_999, 2), variant.Decimal4(-999_999_999, 2)},
			// Scale mismatches, physical-type mismatches, and values whose
			// digit count exceeds the declared precision all fall back.
			mismatch: []variant.Value{
				variant.Decimal4(1234, 3),
				variant.Decimal8(1234, 2),
				variant.Decimal4(1_000_000_000, 2),
			},
		},
		{
			name:  "decimal8",
			node:  parquet.Decimal(2, 18, parquet.Int64Type),
			match: []variant.Value{variant.Decimal8(999_999_999_999_999_999, 2), variant.Decimal8(-999_999_999_999_999_999, 2)},
			mismatch: []variant.Value{
				variant.Decimal4(1234, 2),
				variant.Decimal8(1234, 3),
				variant.Decimal8(1_000_000_000_000_000_000, 2),
			},
		},
		{
			name: "decimal16",
			node: parquet.Decimal(2, 38, parquet.FixedLenByteArrayType(16)),
			match: []variant.Value{
				variant.Decimal16(le16(12345, false), 2),
				variant.Decimal16(le16(12345, true), 2), // negative: sign extension both ways
			},
			mismatch: []variant.Value{
				variant.Decimal8(1234, 2),
				variant.Decimal16(le16(1, false), 3),
			},
		},
		{
			// DECIMAL may also be backed by a variable-length BYTE_ARRAY.
			name: "decimal16_byte_array",
			node: parquet.Decimal(2, 38, parquet.ByteArrayType),
			match: []variant.Value{
				variant.Decimal16(le16(12345, false), 2),
				variant.Decimal16(le16(12345, true), 2),
			},
			mismatch: []variant.Value{
				variant.Decimal8(1234, 2),
				variant.Decimal16(le16(1, false), 3),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shredded, err := parquet.ShreddedVariant(tc.node)
			if err != nil {
				t.Fatalf("building shredded variant node: %v", err)
			}
			schema := parquet.NewSchema("table", parquet.Group{
				"id":  parquet.Int(32),
				"var": shredded,
			})

			values := slices.Concat(tc.match, tc.mismatch)
			rows := make([]shreddedVariantRow, len(values))
			for i, v := range values {
				rows[i] = shreddedVariantRow{ID: int32(i), Var: encodeRawVariant(v)}
			}

			buf := new(bytes.Buffer)
			w := parquet.NewGenericWriter[shreddedVariantRow](buf, schema)
			if _, err := w.Write(rows); err != nil {
				t.Fatalf("writing: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("closing: %v", err)
			}

			// Placement: matching values must occupy typed_value with a
			// null value column; mismatching values the other way around.
			pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatal(err)
			}
			columns := make(map[string][]parquet.Value)
			err = forEachColumnValue(pf.Root(), func(leaf *parquet.Column, value parquet.Value) error {
				path := strings.Join(leaf.Path(), ".")
				columns[path] = append(columns[path], value.Clone())
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for i, v := range values {
				wantShredded := i < len(tc.match)
				typedNull := columns["var.typed_value"][i].IsNull()
				valueNull := columns["var.value"][i].IsNull()
				if wantShredded && (typedNull || !valueNull) {
					t.Errorf("row %d (%#v): expected shredding, got typed_value null=%v value null=%v",
						i, v.GoValue(), typedNull, valueNull)
				}
				if !wantShredded && (!typedNull || valueNull) {
					t.Errorf("row %d (%#v): expected value-column fallback, got typed_value null=%v value null=%v",
						i, v.GoValue(), typedNull, valueNull)
				}
			}

			// Reconstruction: every row reads back structurally equal.
			got, err := parquet.Read[rawVariantRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("reading: %v", err)
			}
			for i, want := range values {
				decoded, err := decodeRawVariant(got[i].Var)
				if err != nil {
					t.Errorf("row %d: decoding read-back value: %v", i, err)
					continue
				}
				if !decoded.Equal(want) {
					t.Errorf("row %d round trip mismatch:\n got: %#v\nwant: %#v",
						i, decoded.GoValue(), want.GoValue())
				}
			}
		})
	}
}
