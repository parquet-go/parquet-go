package parquet

import (
	"strings"
	"testing"
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
