//go:build go1.27

package parquet_test

import (
	"bytes"
	"math/rand"
	"testing"
	"uuid"

	googleuuid "github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

type uuidSchemaRow[T ~[16]byte] struct {
	ID       T               `parquet:"id"`
	Nested   uuidSchemaID[T] `parquet:"nested"`
	Optional *T              `parquet:"optional"`
	Raw      [16]byte        `parquet:"raw"`
}

type uuidSchemaID[T ~[16]byte] struct {
	ID T `parquet:"id"`
}

func TestSchemaOfStdlibUUID(t *testing.T) {
	want := parquet.NewSchema("uuid", parquet.Group{
		"id":       parquet.UUID(),
		"nested":   parquet.Group{"id": parquet.UUID()},
		"optional": parquet.Optional(parquet.UUID()),
		"raw":      parquet.Leaf(parquet.FixedLenByteArrayType(16)),
	})
	for _, test := range []struct {
		name  string
		value any
	}{
		{"standard library", uuidSchemaRow[uuid.UUID]{}},
		{"google", uuidSchemaRow[googleuuid.UUID]{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := parquet.SchemaOf(test.value)
			if !parquet.EqualNodes(got, want) {
				t.Errorf("schema mismatch:\nwant:\n%s\ngot:\n%s", want, got)
			}
		})
	}
}

func TestStdlibUUIDRoundTrip(t *testing.T) {
	id := uuid.MustParse("01899e76-d85e-7d8c-aff7-b0008e05c8fb")
	zero := uuid.Nil()
	rows := []uuidSchemaRow[uuid.UUID]{
		{ID: id, Nested: uuidSchemaID[uuid.UUID]{id}, Optional: &id, Raw: [16]byte(id)},
		{Optional: &zero},
		{},
	}

	var buf bytes.Buffer
	if err := parquet.Write(&buf, rows); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	wantSchema := parquet.SchemaOf(uuidSchemaRow[googleuuid.UUID]{})
	if !parquet.EqualNodes(file.Schema(), wantSchema) {
		t.Fatalf("file schema mismatch:\nwant:\n%s\ngot:\n%s", wantSchema, file.Schema())
	}
	got, err := parquet.Read[uuidSchemaRow[uuid.UUID]](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	assertRowsEqual(t, rows, got)

	googleID := googleuuid.UUID(id)
	googleZero := googleuuid.Nil
	googleRows := []uuidSchemaRow[googleuuid.UUID]{
		{ID: googleID, Nested: uuidSchemaID[googleuuid.UUID]{googleID}, Optional: &googleID, Raw: [16]byte(id)},
		{Optional: &googleZero},
		{},
	}
	googleGot, err := parquet.Read[uuidSchemaRow[googleuuid.UUID]](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	assertRowsEqual(t, googleRows, googleGot)

	buf.Reset()
	if err := parquet.Write(&buf, googleRows); err != nil {
		t.Fatal(err)
	}
	got, err = parquet.Read[uuidSchemaRow[uuid.UUID]](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	assertRowsEqual(t, rows, got)
}

func BenchmarkStdlibUUIDWriter(b *testing.B) {
	benchmarkGenericWriter[stdlibUUIDColumn](b)
}

type stdlibUUIDColumn struct {
	Value uuid.UUID `parquet:",delta"`
}

func (row stdlibUUIDColumn) generate(prng *rand.Rand) stdlibUUIDColumn {
	prng.Read(row.Value[:])
	return row
}
