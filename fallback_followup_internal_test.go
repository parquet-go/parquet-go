package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestFixedLenByteArrayMissingTypeLengthOnFallback(t *testing.T) {
	type row struct {
		ID    int32
		Stamp [12]byte
	}
	var data bytes.Buffer
	in := []row{{ID: 10, Stamp: [12]byte{255}}, {ID: 20, Stamp: [12]byte{0, 1}}}
	if err := Write(&data, in); err != nil {
		t.Fatal(err)
	}
	patched := rewriteFileMetadata(t, data.Bytes(), func(meta *format.FileMetaData) {
		meta.Schema[2].LogicalType = *Timestamp(Millisecond).Type().LogicalType()
		meta.Schema[2].TypeLength.Reset()
	})
	_, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("OpenFile = %v, want an error wrapping ErrCorrupted", err)
	}
}

func TestUUIDMissingTypeLengthKeepsFixedWidth(t *testing.T) {
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	type row struct {
		ID uuid.UUID
	}
	var data bytes.Buffer
	if err := Write(&data, []row{{ID: id}}); err != nil {
		t.Fatal(err)
	}
	patched := rewriteFileMetadata(t, data.Bytes(), func(meta *format.FileMetaData) {
		meta.Schema[1].TypeLength.Reset()
	})
	f, err := OpenFile(bytes.NewReader(patched), int64(len(patched)))
	if err != nil {
		t.Fatal(err)
	}
	typ := f.RowGroups()[0].ColumnChunks()[0].Type()
	if typ.Kind() != FixedLenByteArray || typ.Length() != 16 {
		t.Fatalf("type = %v len %d, want FIXED_LEN_BYTE_ARRAY(16)", typ, typ.Length())
	}
	if _, ok := typ.LogicalType().Value.(*format.UUIDType); !ok {
		t.Fatalf("logical type = %v, want UUID", typ.LogicalType())
	}
	rows := readFileRows(t, f)
	if len(rows) != 1 || !bytes.Equal(rows[0][0].ByteArray(), id[:]) {
		t.Fatalf("rows = %v, want uuid bytes %x", rows, id[:])
	}
	ci, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if got := ci.MinValue(0).ByteArray(); !bytes.Equal(got, id[:]) {
		t.Fatalf("column index min = %x, want %x", got, id[:])
	}
}

func TestDecimalFixedLenByteArrayMissingTypeLength(t *testing.T) {
	elem := format.SchemaElement{
		Name:        "d",
		Type:        thrift.New(format.FixedLenByteArray),
		LogicalType: format.LogicalType{Value: &format.DecimalType{Scale: 2, Precision: 5}},
	}
	_, _, err := schemaElementTypeOf(&elem)
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("schemaElementTypeOf = %v, want an error wrapping ErrCorrupted", err)
	}
}

func rewriteFileMetadata(t *testing.T, original []byte, mutate func(*format.FileMetaData)) []byte {
	t.Helper()
	start := len(original) - 8 - int(binary.LittleEndian.Uint32(original[len(original)-8:]))
	var meta format.FileMetaData
	var proto thrift.CompactProtocol
	if err := thrift.Unmarshal(&proto, original[start:len(original)-8], &meta); err != nil {
		t.Fatal(err)
	}
	mutate(&meta)
	footer, err := thrift.Marshal(&proto, &meta)
	if err != nil {
		t.Fatal(err)
	}
	b := append(bytes.Clone(original[:start]), footer...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(footer)))
	return append(b, "PAR1"...)
}

func readFileRows(t *testing.T, f *File) []Row {
	t.Helper()
	rows := f.RowGroups()[0].Rows()
	defer rows.Close()
	buf := make([]Row, 8)
	var all []Row
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			all = append(all, row.Clone())
		}
		if errors.Is(err, io.EOF) {
			return all
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}
