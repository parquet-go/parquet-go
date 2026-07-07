package parquet

import (
	"math/big"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/variant"
)

// TestBuildShreddedVariantGroupRejectsInvalidSchemas pins the schema-shape
// validation of the shredded variant reconstruction tree. Each schema here
// would silently misbehave if accepted: the level arithmetic depends on
// metadata being required and value/typed_value being optional, and an
// empty object typed_value group has no columns to carry its presence.
func TestBuildShreddedVariantGroupRejectsInvalidSchemas(t *testing.T) {
	binary := Leaf(ByteArrayType)
	cases := []struct {
		name    string
		group   Group
		wantErr string
	}{
		{
			name: "optional metadata",
			group: Group{
				"metadata":    Optional(binary),
				"value":       Optional(binary),
				"typed_value": Optional(Int(64)),
			},
			wantErr: "metadata field must be a required binary leaf",
		},
		{
			name: "required value",
			group: Group{
				"metadata":    Required(binary),
				"value":       Required(binary),
				"typed_value": Optional(Int(64)),
			},
			wantErr: "value field of a shredded variant group must be optional",
		},
		{
			name: "required typed_value",
			group: Group{
				"metadata":    Required(binary),
				"value":       Optional(binary),
				"typed_value": Required(Int(64)),
			},
			wantErr: "typed_value must be optional",
		},
		{
			name: "empty object typed_value",
			group: Group{
				"metadata":    Required(binary),
				"value":       Optional(binary),
				"typed_value": Optional(Group{}),
			},
			wantErr: "must have at least one field",
		},
		{
			name: "unsigned integer typed_value",
			group: Group{
				"metadata":    Required(binary),
				"value":       Optional(binary),
				"typed_value": Optional(Uint(32)),
			},
			wantErr: "unsupported shredded type",
		},
		{
			name: "unexpected field",
			group: Group{
				"metadata":    Required(binary),
				"value":       Optional(binary),
				"typed_value": Optional(Int(64)),
				"bogus":       Optional(binary),
			},
			wantErr: `unexpected field "bogus"`,
		},
		{
			name: "neither value nor typed_value",
			group: Group{
				"metadata": Required(binary),
			},
			wantErr: "neither value nor typed_value",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := buildShreddedVariantGroup(c.group, 0, 0, 0)
			if err == nil {
				t.Fatalf("expected an error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, c.wantErr)
			}
		})
	}
}

// TestVariantDecimalPrecisionMatching pins the typed-column matching rule
// for decimals: a DECIMAL(P, S) column declares at most P digits to
// readers, so an unscaled value with more than P digits must fall back to
// the value column even though it fits the physical type. (Found by
// randomized round-trip testing: DuckDB renders a 10-digit unscaled value
// in a DECIMAL(9, 2) column as a different decimal type.)
func TestVariantDecimalPrecisionMatching(t *testing.T) {
	decimal92 := Decimal(2, 9, Int32Type).Type()
	if _, ok := variantToParquetValue(variant.Decimal4(999_999_999, 2), decimal92); !ok {
		t.Errorf("9-digit decimal4 must shred into DECIMAL(9, 2)")
	}
	if _, ok := variantToParquetValue(variant.Decimal4(-999_999_999, 2), decimal92); !ok {
		t.Errorf("negative 9-digit decimal4 must shred into DECIMAL(9, 2)")
	}
	if _, ok := variantToParquetValue(variant.Decimal4(1_000_000_000, 2), decimal92); ok {
		t.Errorf("10-digit decimal4 must not shred into DECIMAL(9, 2)")
	}
	if _, ok := variantToParquetValue(variant.Decimal4(1234, 3), decimal92); ok {
		t.Errorf("scale mismatch must not shred")
	}

	decimal382 := Decimal(2, 38, FixedLenByteArrayType(16)).Type()
	le16 := func(n *big.Int) (out [16]byte) { // two's complement little-endian
		v := new(big.Int).Set(n)
		if v.Sign() < 0 {
			v.Add(v, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		v.FillBytes(out[:])
		for i := range 8 {
			out[i], out[15-i] = out[15-i], out[i]
		}
		return out
	}
	pow10_38 := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if _, ok := variantToParquetValue(variant.Decimal16(le16(pow10_38), 2), decimal382); ok {
		t.Errorf("39-digit decimal16 must not shred into DECIMAL(38, 2)")
	}
	maxP38 := new(big.Int).Sub(pow10_38, big.NewInt(1))
	if _, ok := variantToParquetValue(variant.Decimal16(le16(maxP38), 2), decimal382); !ok {
		t.Errorf("38-digit decimal16 must shred into DECIMAL(38, 2)")
	}
	if _, ok := variantToParquetValue(variant.Decimal16(le16(new(big.Int).Neg(maxP38)), 2), decimal382); !ok {
		t.Errorf("negative 38-digit decimal16 must shred into DECIMAL(38, 2)")
	}
	if _, ok := variantToParquetValue(variant.Decimal16(le16(big.NewInt(-1)), 2), decimal382); !ok {
		t.Errorf("-1 decimal16 must shred into DECIMAL(38, 2)")
	}
}
