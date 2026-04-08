package parquet_test

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// ExampleVariant demonstrates reading and writing unshredded variant data
// using Go structs with the "variant" struct tag. The variant field accepts
// any Go value, which is automatically marshaled to/from the Parquet variant
// binary format.
func ExampleVariant() {
	type Event struct {
		ID   int64 `parquet:"id"`
		Data any   `parquet:"data,variant"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Event](buf)
	_, _ = writer.Write([]Event{
		{ID: 1, Data: "hello"},
		{ID: 2, Data: int32(42)},
		{ID: 3, Data: map[string]any{"key": "value"}},
	})
	_ = writer.Close()

	reader := parquet.NewGenericReader[Event](bytes.NewReader(buf.Bytes()))
	events := make([]Event, 3)
	_, _ = reader.Read(events)
	_ = reader.Close()

	for _, e := range events {
		fmt.Printf("Event %d: %v (%T)\n", e.ID, e.Data, e.Data)
	}
	// Output:
	// Event 1: hello (string)
	// Event 2: 42 (int32)
	// Event 3: map[key:value] (map[string]interface {})
}

// ExampleVariant_schema demonstrates creating and printing variant schemas.
func ExampleVariant_schema() {
	// Unshredded variant: group with metadata + value byte arrays.
	unshredded := parquet.NewSchema("Unshredded", parquet.Group{
		"data": parquet.Optional(parquet.Variant()),
	})
	fmt.Println(unshredded)

	// Shredded variant: adds a typed_value column for efficient typed access.
	shreddedType, _ := parquet.ShreddedVariant(parquet.String())
	shredded := parquet.NewSchema("Shredded", parquet.Group{
		"data": parquet.Optional(shreddedType),
	})
	fmt.Println(shredded)
	// Output:
	// message Unshredded {
	// 	optional group data (VARIANT) {
	// 		required binary metadata;
	// 		required binary value;
	// 	}
	// }
	// message Shredded {
	// 	optional group data (VARIANT) {
	// 		required binary metadata;
	// 		optional binary typed_value (STRING);
	// 		optional binary value;
	// 	}
	// }
}

// ExampleShreddedVariant demonstrates reading and writing shredded variant data.
// When a Go value matches the typed_value schema, it is stored efficiently in
// the typed column. Otherwise, it falls back to the variant-encoded value column.
func ExampleShreddedVariant() {
	type Record struct {
		ID   int64 `parquet:"id"`
		Data any   `parquet:"data"`
	}

	shreddedType, _ := parquet.ShreddedVariant(parquet.String())
	schema := parquet.NewSchema("Record", parquet.Group{
		"id":   parquet.Int(64),
		"data": shreddedType,
	})

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)
	_, _ = writer.Write([]Record{
		{ID: 1, Data: "typed-value"},          // Shredded: stored in typed_value column
		{ID: 2, Data: int32(42)},              // Not shredded: stored in value column
		{ID: 3, Data: map[string]any{"k": 1}}, // Not shredded: stored in value column
	})
	_ = writer.Close()

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	records := make([]Record, 3)
	n, _ := reader.Read(records)
	_ = reader.Close()

	for _, r := range records[:n] {
		fmt.Printf("Record %d: %v (%T)\n", r.ID, r.Data, r.Data)
	}
	// Output:
	// Record 1: typed-value (string)
	// Record 2: 42 (int32)
	// Record 3: map[k:1] (map[string]interface {})
}
