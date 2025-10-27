package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// Test for issue #332 - slice fields should use LIST logical type by default
func TestSliceFieldsUseListLogicalType(t *testing.T) {
	type Row struct {
		Data []int64 `parquet:"data"`
	}

	schema := parquet.SchemaOf(&Row{})

	// The schema should contain a LIST logical type, not just a repeated field
	schemaStr := schema.String()
	t.Logf("Schema: %s", schemaStr)

	// Write some data
	buf := new(bytes.Buffer)
	writer := parquet.NewWriter(buf, schema)

	err := writer.Write(&Row{Data: []int64{1, 2}})
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Try to read it back
	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	rows := make([]Row, 0, 1)
	err = reader.Read(&rows)
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if len(rows[0].Data) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(rows[0].Data))
	}

	if rows[0].Data[0] != 1 || rows[0].Data[1] != 2 {
		t.Fatalf("expected [1, 2], got %v", rows[0].Data)
	}

	// Verify the file has proper LIST structure
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	// Check that the schema has the proper LIST logical type
	rootSchema := file.Schema()
	fields := rootSchema.Fields()
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}

	dataField := fields[0]
	dataFieldType := dataField.Type()
	t.Logf("Data field type: %v", dataFieldType)

	// The field should have LIST logical type
	logicalType := dataFieldType.LogicalType()
	if logicalType == nil {
		t.Fatal("expected LIST logical type, got nil")
	}

	if logicalType.List == nil {
		t.Fatalf("expected LIST logical type, got %v", logicalType)
	}
}

func TestSliceFieldsWithListTag(t *testing.T) {
	type Row struct {
		Data []int64 `parquet:"data,list"`
	}

	schema := parquet.SchemaOf(&Row{})

	// Write some data
	buf := new(bytes.Buffer)
	writer := parquet.NewWriter(buf, schema)

	err := writer.Write(&Row{Data: []int64{1, 2}})
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Try to read it back
	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	rows := make([]Row, 0, 1)
	err = reader.Read(&rows)
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if len(rows[0].Data) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(rows[0].Data))
	}

	if rows[0].Data[0] != 1 || rows[0].Data[1] != 2 {
		t.Fatalf("expected [1, 2], got %v", rows[0].Data)
	}
}
