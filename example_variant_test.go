package parquet_test

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// ExampleVariant demonstrates reading and writing unshredded variant data
// using Go structs. An unshredded variant is a group with two byte array
// fields: "metadata" (describes the type/structure) and "value" (the encoded
// data).
//
// Since there is no "variant" struct tag, the schema must be constructed
// programmatically with parquet.Variant() and passed explicitly to the
// writer and reader.
func ExampleVariant() {
	// Define a Go struct matching the variant group structure.
	type VariantData struct {
		Metadata []byte `parquet:"metadata"`
		Value    []byte `parquet:"value"`
	}
	type Event struct {
		ID   int64       `parquet:"id"`
		Data VariantData `parquet:"data"`
	}

	// Build a schema with the VARIANT logical type annotation.
	schema := parquet.NewSchema("Event", parquet.Group{
		"id":   parquet.Int(64),
		"data": parquet.Variant(),
	})

	// Write rows using the explicit schema.
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Event](buf, schema)
	_, _ = writer.Write([]Event{
		{ID: 1, Data: VariantData{Metadata: []byte{0x01, 0x00, 0x00}, Value: []byte(`"hello"`)}},
		{ID: 2, Data: VariantData{Metadata: []byte{0x01, 0x00, 0x00}, Value: []byte(`42`)}},
	})
	_ = writer.Close()

	// Read rows back.
	reader := parquet.NewGenericReader[Event](bytes.NewReader(buf.Bytes()), schema)
	events := make([]Event, 2)
	_, _ = reader.Read(events)
	_ = reader.Close()

	fmt.Printf("Event %d: value=%s\n", events[0].ID, events[0].Data.Value)
	fmt.Printf("Event %d: value=%s\n", events[1].ID, events[1].Data.Value)
	// Output:
	// Event 1: value="hello"
	// Event 2: value=42
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

// ExampleShreddedVariant demonstrates reading and writing shredded variant
// data. A shredded variant adds a typed "typed_value" column alongside the
// raw "value" field, enabling efficient typed access without decoding the
// variant bytes.
func ExampleShreddedVariant() {
	type ShreddedData struct {
		Metadata   []byte  `parquet:"metadata"`
		Value      []byte  `parquet:"value"`
		TypedValue *string `parquet:"typed_value"`
	}
	type Record struct {
		ID   int64        `parquet:"id"`
		Data ShreddedData `parquet:"data"`
	}

	shreddedType, _ := parquet.ShreddedVariant(parquet.String())
	schema := parquet.NewSchema("Record", parquet.Group{
		"id":   parquet.Int(64),
		"data": shreddedType,
	})

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf, schema)

	str := "typed-value"
	_, _ = writer.Write([]Record{
		// Row with typed (shredded) value: typed_value is set, value is empty.
		{ID: 1, Data: ShreddedData{Metadata: []byte{0x01, 0x00, 0x00}, TypedValue: &str}},
		// Row with unshredded fallback: value holds raw bytes, typed_value is nil.
		{ID: 2, Data: ShreddedData{Metadata: []byte{0x01, 0x00, 0x00}, Value: []byte(`{"complex": true}`)}},
	})
	_ = writer.Close()

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()), schema)
	records := make([]Record, 2)
	_, _ = reader.Read(records)
	_ = reader.Close()

	for _, r := range records {
		if r.Data.TypedValue != nil {
			fmt.Printf("Record %d: typed_value=%s\n", r.ID, *r.Data.TypedValue)
		} else {
			fmt.Printf("Record %d: value=%s\n", r.ID, r.Data.Value)
		}
	}
	// Output:
	// Record 1: typed_value=typed-value
	// Record 2: value={"complex": true}
}
