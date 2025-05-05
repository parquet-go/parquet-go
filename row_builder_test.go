package parquet_test

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func ExampleRowBuilder() {
	builder := parquet.NewRowBuilder(parquet.GroupOfNodes(
		"birth_date", parquet.Optional(parquet.Date()),
		"first_name", parquet.String(),
		"last_name", parquet.String(),
	))

	builder.Add(1, parquet.ByteArrayValue([]byte("Luke")))
	builder.Add(2, parquet.ByteArrayValue([]byte("Skywalker")))

	row := builder.Row()
	row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
		fmt.Printf("%+v\n", columnValues[0])
		return true
	})

	// Output:
	// C:0 D:0 R:0 V:<null>
	// C:1 D:0 R:0 V:Luke
	// C:2 D:0 R:0 V:Skywalker
}

func TestRowBuilder(t *testing.T) {
	type (
		operation  = func(*parquet.RowBuilder)
		operations = []operation
	)

	add := func(columnIndex int, columnValue parquet.Value) operation {
		return func(b *parquet.RowBuilder) { b.Add(columnIndex, columnValue) }
	}

	next := func(columnIndex int) operation {
		return func(b *parquet.RowBuilder) { b.Next(columnIndex) }
	}

	tests := []struct {
		scenario   string
		operations operations
		want       parquet.Row
		schema     parquet.Node
	}{
		{
			scenario: "add missing required column value",
			want: parquet.Row{
				parquet.Int64Value(0).Level(0, 0, 0),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
			),
		},

		{
			scenario: "set required column value",
			operations: operations{
				add(0, parquet.Int64Value(1)),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
			),
		},

		{
			scenario: "set repeated column values",
			operations: operations{
				add(0, parquet.Int64Value(1)),
				add(1, parquet.ByteArrayValue([]byte(`1`))),
				add(1, parquet.ByteArrayValue([]byte(`2`))),
				add(1, parquet.ByteArrayValue([]byte(`3`))),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.ByteArrayValue([]byte(`1`)).Level(0, 1, 1),
				parquet.ByteArrayValue([]byte(`2`)).Level(1, 1, 1),
				parquet.ByteArrayValue([]byte(`3`)).Level(1, 1, 1),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
				"names", parquet.Repeated(parquet.String()),
			),
		},

		{
			scenario: "add missing repeated column value",
			operations: operations{
				add(0, parquet.Int64Value(1)),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.NullValue().Level(0, 0, 1),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
				"names", parquet.Repeated(parquet.String()),
			),
		},

		{
			scenario: "add missing optional column value",
			operations: operations{
				add(0, parquet.Int64Value(1)),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.NullValue().Level(0, 0, 1),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
				"name", parquet.Optional(parquet.String()),
			),
		},

		{
			scenario: "add missing nested column values",
			operations: operations{
				add(0, parquet.Int64Value(1)),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.NullValue().Level(0, 0, 1),
				parquet.ByteArrayValue(nil).Level(0, 0, 2),
				parquet.ByteArrayValue(nil).Level(0, 0, 3),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
				"profile", parquet.GroupOfNodes(
					"birth_date", parquet.Optional(parquet.Date()),
					"first_name", parquet.String(),
					"last_name", parquet.String(),
				),
			),
		},

		{
			scenario: "add missing repeated column group",
			operations: operations{
				add(0, parquet.Int64Value(1)),
				add(2, parquet.ByteArrayValue([]byte(`me`))),
				add(1, parquet.Int32Value(0)),
				add(1, parquet.Int32Value(123456)),
				add(2, parquet.ByteArrayValue([]byte(`you`))),
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),

				parquet.Int32Value(0).Level(0, 2, 1),
				parquet.Int32Value(123456).Level(1, 2, 1),

				parquet.ByteArrayValue([]byte(`me`)).Level(0, 1, 2),
				parquet.ByteArrayValue([]byte(`you`)).Level(1, 1, 2),

				parquet.NullValue().Level(0, 1, 3),
				parquet.NullValue().Level(1, 1, 3),
			},
			schema: parquet.GroupOfNodes(
				"id", parquet.Int(64),
				"profiles", parquet.Repeated(parquet.GroupOfNodes(
					"birth_date", parquet.Optional(parquet.Date()),
					"first_name", parquet.String(),
					"last_name", parquet.String(),
				)),
			),
		},

		{
			scenario: "empty map",
			want: parquet.Row{
				parquet.Value{}.Level(0, 0, 0),
				parquet.Value{}.Level(0, 0, 1),
			},
			schema: parquet.GroupOfNodes(
				"map", parquet.Repeated(parquet.GroupOfNodes(
					"key_value", parquet.GroupOfNodes(
						"key", parquet.String(),
						"value", parquet.Optional(parquet.String()),
					),
				)),
			),
		},

		{
			scenario: "one nested maps",
			operations: operations{
				add(0, parquet.ByteArrayValue([]byte(`A`))),
				add(1, parquet.ByteArrayValue([]byte(`1`))),
				add(0, parquet.ByteArrayValue([]byte(`B`))),
				add(1, parquet.ByteArrayValue([]byte(`2`))),
			},
			want: parquet.Row{
				// objects.attributes.key_value.key
				parquet.ByteArrayValue([]byte(`A`)).Level(0, 2, 0),
				parquet.ByteArrayValue([]byte(`B`)).Level(2, 2, 0),
				// objects.attributes.key_value.value
				parquet.ByteArrayValue([]byte(`1`)).Level(0, 3, 1),
				parquet.ByteArrayValue([]byte(`2`)).Level(2, 3, 1),
			},
			schema: parquet.GroupOfNodes(
				"objects", parquet.Repeated(parquet.GroupOfNodes(
					"attributes", parquet.Repeated(parquet.GroupOfNodes(
						"key_value", parquet.GroupOfNodes(
							"key", parquet.String(),
							"value", parquet.Optional(parquet.String()),
						),
					)),
				)),
			),
		},

		{
			scenario: "multiple nested maps",
			operations: operations{
				add(0, parquet.ByteArrayValue([]byte(`A`))),
				add(1, parquet.ByteArrayValue([]byte(`1`))),
				add(0, parquet.ByteArrayValue([]byte(`B`))),
				add(1, parquet.ByteArrayValue([]byte(`2`))),
				next(1), // same as next(0) because the columns are in the same group
				add(0, parquet.ByteArrayValue([]byte(`C`))),
				add(1, parquet.ByteArrayValue([]byte(`3`))),
			},
			want: parquet.Row{
				// objects.attributes.key_value.key
				parquet.ByteArrayValue([]byte(`A`)).Level(0, 2, 0),
				parquet.ByteArrayValue([]byte(`B`)).Level(2, 2, 0),
				parquet.ByteArrayValue([]byte(`C`)).Level(1, 2, 0),
				// objects.attributes.key_value.value
				parquet.ByteArrayValue([]byte(`1`)).Level(0, 3, 1),
				parquet.ByteArrayValue([]byte(`2`)).Level(2, 3, 1),
				parquet.ByteArrayValue([]byte(`3`)).Level(1, 3, 1),
			},
			schema: parquet.GroupOfNodes(
				"objects", parquet.Repeated(parquet.GroupOfNodes(
					"attributes", parquet.Repeated(parquet.GroupOfNodes(
						"key_value", parquet.GroupOfNodes(
							"key", parquet.String(),
							"value", parquet.Optional(parquet.String()),
						),
					)),
				)),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			b := parquet.NewRowBuilder(test.schema)

			for i := range 2 {
				for _, op := range test.operations {
					op(b)
				}

				if got := b.Row(); !got.Equal(test.want) {
					t.Fatalf("test %d: rows are not equal\nwant = %+v\ngot  = %+v", i+1, test.want, got)
				}

				b.Reset()
			}
		})
	}
}

func BenchmarkRowBuilderAdd(b *testing.B) {
	builder := parquet.NewRowBuilder(parquet.GroupOfNodes(
		"ids", parquet.Repeated(parquet.Int(64)),
	))

	for i := 0; i < b.N; i++ {
		builder.Add(0, parquet.Int64Value(int64(i)))

		if (i % 128) == 0 {
			builder.Reset() // so don't run out of memory ;)
		}
	}
}
