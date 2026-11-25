package parquet_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// column_chunk_key_value_metadata.parquet is an interoperability fixture from
// apache/parquet-testing at 9b48ff4f94dc5e89592d46a119884dbb88100884. One
// column chunk has metadata {"foo": "bar", "thisiskeywithoutvalue": null}.
func TestColumnChunkKeyValueMetadata(t *testing.T) {
	data, err := os.ReadFile("testdata/column_chunk_key_value_metadata.parquet")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// The fixture omits field 2. An empty string written by this package is a
	// different byte sequence, and that sequence is not in the file.
	empty, err := thrift.Marshal(&thrift.CompactProtocol{}, format.KeyValue{Key: "thisiskeywithoutvalue", Value: ""})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, empty) {
		t.Fatal("fixture encodes the empty value; field 2 is present")
	}

	if len(file.Metadata().KeyValueMetadata) != 0 {
		t.Fatalf("file key/value metadata = %+v", file.Metadata().KeyValueMetadata)
	}

	var got []format.KeyValue
	var columns int
	for _, rowGroup := range file.Metadata().RowGroups {
		for _, column := range rowGroup.Columns {
			if len(column.MetaData.KeyValueMetadata) == 0 {
				continue
			}
			columns++
			got = append(got, column.MetaData.KeyValueMetadata...)
		}
	}
	if columns != 1 {
		t.Fatalf("columns with key/value metadata: %d", columns)
	}
	want := map[string]string{
		"foo":                   "bar",
		"thisiskeywithoutvalue": "",
	}
	if len(got) != len(want) {
		t.Fatalf("key/value metadata = %+v", got)
	}
	for _, kv := range got {
		value, ok := want[kv.Key]
		if !ok || value != kv.Value {
			t.Errorf("entry %q = %q", kv.Key, kv.Value)
		}
	}
}

func TestFileKeyValueMetadataAbsentValue(t *testing.T) {
	const (
		presentKey = "foo"
		absentKey  = "thisiskeywithoutvalue"
	)
	written := writeKeyValueFile(t, presentKey, "bar", absentKey, "")
	file := openKeyValueFile(t, written)
	if value, ok := file.Lookup(presentKey); !ok || value != "bar" {
		t.Fatalf("Lookup(%q) = %q, %t", presentKey, value, ok)
	}
	if value, ok := file.Lookup(absentKey); !ok || value != "" {
		t.Fatalf("Lookup(%q) = %q, %t", absentKey, value, ok)
	}

	// The writer cannot emit a missing field 2, because that is what older
	// readers reject. Drop the field from the empty entry and open the file
	// the way another implementation wrote it.
	absent := omitKeyValueField(t, written, absentKey)
	file = openKeyValueFile(t, absent)
	if value, ok := file.Lookup(presentKey); !ok || value != "bar" {
		t.Fatalf("Lookup(%q) = %q, %t", presentKey, value, ok)
	}
	if value, ok := file.Lookup(absentKey); !ok || value != "" {
		t.Fatalf("Lookup(%q) = %q, %t", absentKey, value, ok)
	}
	entries := map[string]string{}
	for _, kv := range file.Metadata().KeyValueMetadata {
		entries[kv.Key] = kv.Value
	}
	if len(entries) != 2 || entries[presentKey] != "bar" || entries[absentKey] != "" {
		t.Fatalf("file metadata = %+v", file.Metadata().KeyValueMetadata)
	}

	reader := parquet.NewGenericReader[keyValueRow](bytes.NewReader(absent))
	defer reader.Close()
	var rows [1]keyValueRow
	n, err := reader.Read(rows[:])
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 1 || rows[0].A != 1 {
		t.Fatalf("read %+v (n=%d)", rows[:n], n)
	}
}

func TestSetKeyValueMetadataEmptyValue(t *testing.T) {
	const key = "thisiskeywithoutvalue"
	written := writeKeyValueFile(t, key, "")

	empty, err := thrift.Marshal(&thrift.CompactProtocol{}, format.KeyValue{Key: key, Value: ""})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(written, empty) {
		t.Fatal("empty value was written without field 2")
	}
	file := openKeyValueFile(t, written)
	if value, ok := file.Lookup(key); !ok || value != "" {
		t.Fatalf("Lookup(%q) = %q, %t", key, value, ok)
	}

	// Copy the entry from a file that omitted the field. The value we read
	// is "", and the file we write has to contain field 2 again.
	src := openKeyValueFile(t, omitKeyValueField(t, written, key))
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.SchemaOf(new(keyValueRow)))
	if err := w.Write(&keyValueRow{A: 1}); err != nil {
		t.Fatal(err)
	}
	for _, kv := range src.Metadata().KeyValueMetadata {
		w.SetKeyValueMetadata(kv.Key, kv.Value)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), empty) {
		t.Fatal("rewritten empty value omitted field 2")
	}
	file = openKeyValueFile(t, buf.Bytes())
	if value, ok := file.Lookup(key); !ok || value != "" {
		t.Fatalf("Lookup(%q) = %q, %t", key, value, ok)
	}
}

type keyValueRow struct {
	A int32 `parquet:"a"`
}

func writeKeyValueFile(t *testing.T, pairs ...string) []byte {
	t.Helper()
	if len(pairs)%2 != 0 {
		t.Fatal("key/value pairs must be even")
	}
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.SchemaOf(new(keyValueRow)))
	if err := w.Write(&keyValueRow{A: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(pairs); i += 2 {
		w.SetKeyValueMetadata(pairs[i], pairs[i+1])
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openKeyValueFile(t *testing.T, data []byte) *parquet.File {
	t.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// keyValueKeyOnly mirrors format.KeyValue with field 2 omitted.
type keyValueKeyOnly struct {
	Key string `thrift:"1,required"`
}

// omitKeyValueField returns a copy of a parquet file whose file metadata
// entry for key was written as an empty string, with field 2 removed.
func omitKeyValueField(t *testing.T, file []byte, key string) []byte {
	t.Helper()
	protocol := &thrift.CompactProtocol{}
	withValue, err := thrift.Marshal(protocol, format.KeyValue{Key: key, Value: ""})
	if err != nil {
		t.Fatal(err)
	}
	withoutValue, err := thrift.Marshal(protocol, keyValueKeyOnly{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(file, withValue); n != 1 {
		t.Fatalf("encoded key %q appears %d times", key, n)
	}
	if len(withoutValue) >= len(withValue) {
		t.Fatalf("omitting field 2 did not shrink the encoding (%d >= %d)", len(withoutValue), len(withValue))
	}
	out := bytes.Replace(file, withValue, withoutValue, 1)
	if bytes.Contains(out, withValue) {
		t.Fatal("field 2 is still present")
	}
	delta := len(withValue) - len(withoutValue)
	n := len(out)
	if n < 8 || string(out[n-4:]) != "PAR1" {
		t.Fatal("patched file is missing the parquet trailer")
	}
	size := binary.LittleEndian.Uint32(out[n-8 : n-4])
	if uint32(delta) > size {
		t.Fatalf("footer size %d is smaller than the removed field (%d)", size, delta)
	}
	binary.LittleEndian.PutUint32(out[n-8:n-4], size-uint32(delta))
	return out
}
