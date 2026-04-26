package parquet_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

func TestReconstructUUIDToInterface(t *testing.T) {
	id := uuid.MustParse("f8d46163-fb4f-40ba-a14c-949698265120")

	type Record struct {
		ID   uuid.UUID `parquet:"id,uuid"`
		Name string    `parquet:"name"`
	}

	schema := parquet.SchemaOf(new(Record))
	buf := parquet.NewBuffer(schema)
	buf.Write(&Record{ID: id, Name: "test"})

	rows := buf.Rows()
	defer rows.Close()

	rowBuf := make([]parquet.Row, 1)
	n, _ := rows.ReadRows(rowBuf)
	if n != 1 {
		t.Fatal("expected 1 row")
	}

	result := make(map[string]interface{})
	if err := schema.Reconstruct(&result, rowBuf[0]); err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}

	got, ok := result["id"].(string)
	if !ok {
		t.Fatalf("expected id to be string, got %T: %v", result["id"], result["id"])
	}
	if got != id.String() {
		t.Errorf("id = %q, want %q", got, id.String())
	}
}
