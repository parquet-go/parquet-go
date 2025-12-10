package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestIssue40 tests reading nested map values with a schema generated from NewSchema().
// This reproduces the issue where manual schema creation for maps with nested struct values
// causes a panic: "reflect: call of reflect.Value.MapIndex on struct Value"
//
// https://github.com/parquet-go/parquet-go/issues/40
func TestIssue40(t *testing.T) {
	// Define the types as described in the issue
	type MapValue struct {
		Value string `parquet:"value,optional"`
	}

	type Message struct {
		Payload map[string]MapValue `parquet:"payload"`
	}

	// Use SchemaOf for writing (which works)
	writeSchema := parquet.SchemaOf(Message{})

	// Create test data
	original := Message{
		Payload: map[string]MapValue{
			"key1": {Value: "value1"},
			"key2": {Value: "value2"},
		},
	}

	// Write the data to a buffer using SchemaOf (which works)
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Message](buf, writeSchema)

	if _, err := writer.Write([]Message{original}); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("wrote %d bytes", buf.Len())

	// Create the manual schema for reading (the issue is about reading with NewSchema)
	// This schema should be compatible with the written data
	readSchema := parquet.NewSchema("test", parquet.Group{
		"payload": parquet.Map(
			parquet.String(),
			parquet.Group{
				"value": parquet.Optional(parquet.String()),
			},
		),
	})

	// Read the data back using NewSchema - this is where the panic occurred
	reader := parquet.NewGenericReader[Message](bytes.NewReader(buf.Bytes()), readSchema)
	defer reader.Close()

	t.Logf("reader created, num rows: %d", reader.NumRows())

	rows := make([]Message, 1)
	n, err := reader.Read(rows)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("failed to read: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}

	// Verify the data
	if len(rows[0].Payload) != 2 {
		t.Errorf("expected 2 entries in payload, got %d", len(rows[0].Payload))
	}
	if v, ok := rows[0].Payload["key1"]; !ok || v.Value != "value1" {
		t.Errorf("expected key1=value1, got %v", rows[0].Payload["key1"])
	}
	if v, ok := rows[0].Payload["key2"]; !ok || v.Value != "value2" {
		t.Errorf("expected key2=value2, got %v", rows[0].Payload["key2"])
	}
}

// TestIssue40WithSchemaOf verifies that using SchemaOf works correctly
// (this is the baseline that should work)
func TestIssue40WithSchemaOf(t *testing.T) {
	type MapValue struct {
		Value string `parquet:"value,optional"`
	}

	type Message struct {
		Payload map[string]MapValue `parquet:"payload"`
	}

	// Use SchemaOf for both reading and writing
	schema := parquet.SchemaOf(Message{})

	original := Message{
		Payload: map[string]MapValue{
			"key1": {Value: "value1"},
			"key2": {Value: "value2"},
		},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Message](buf, schema)

	if _, err := writer.Write([]Message{original}); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("wrote %d bytes", buf.Len())

	// Read without passing schema (use file's schema)
	reader := parquet.NewGenericReader[Message](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	t.Logf("reader created, num rows: %d", reader.NumRows())

	rows := make([]Message, 1)
	n, err := reader.Read(rows)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("failed to read: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}

	if len(rows[0].Payload) != 2 {
		t.Errorf("expected 2 entries in payload, got %d", len(rows[0].Payload))
	}
	if v, ok := rows[0].Payload["key1"]; !ok || v.Value != "value1" {
		t.Errorf("expected key1=value1, got %v", rows[0].Payload["key1"])
	}
	if v, ok := rows[0].Payload["key2"]; !ok || v.Value != "value2" {
		t.Errorf("expected key2=value2, got %v", rows[0].Payload["key2"])
	}
}

// TestIssue40SimpleMap tests reading a simple map (without nested struct values)
// with a manually-created schema for reading
func TestIssue40SimpleMap(t *testing.T) {
	type Message struct {
		Payload map[string]string `parquet:"payload"`
	}

	// Use SchemaOf for writing
	writeSchema := parquet.SchemaOf(Message{})

	original := Message{
		Payload: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Message](buf, writeSchema)

	if _, err := writer.Write([]Message{original}); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	t.Logf("wrote %d bytes", buf.Len())

	// Create schema manually for reading
	readSchema := parquet.NewSchema("test", parquet.Group{
		"payload": parquet.Map(
			parquet.String(),
			parquet.Optional(parquet.String()),
		),
	})

	reader := parquet.NewGenericReader[Message](bytes.NewReader(buf.Bytes()), readSchema)
	defer reader.Close()

	t.Logf("reader created, num rows: %d", reader.NumRows())

	rows := make([]Message, 1)
	n, err := reader.Read(rows)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("failed to read: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}

	if len(rows[0].Payload) != 2 {
		t.Errorf("expected 2 entries in payload, got %d", len(rows[0].Payload))
	}
	if v, ok := rows[0].Payload["key1"]; !ok || v != "value1" {
		t.Errorf("expected key1=value1, got %v", rows[0].Payload["key1"])
	}
	if v, ok := rows[0].Payload["key2"]; !ok || v != "value2" {
		t.Errorf("expected key2=value2, got %v", rows[0].Payload["key2"])
	}
}
