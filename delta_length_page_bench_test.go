package parquet

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"
)

// BenchmarkDeltaLengthJSONScan is the #794 uncompressed JSON page scan:
// records from testdata/issue206.json, default page size, ReadPage, Release,
// and Close. Each record is stored as one JSON value so the page payload is
// the document bytes.
func BenchmarkDeltaLengthJSONScan(b *testing.B) {
	f, err := os.Open("testdata/issue206.json")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	tok, err := dec.Token()
	if err != nil {
		b.Fatal(err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		b.Fatalf("issue206.json: want a JSON array, got %v", tok)
	}
	type doc struct {
		Doc json.RawMessage `parquet:"doc,json"`
	}
	docs := make([]doc, 0, 4096)
	var total int64
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			b.Fatal(err)
		}
		total += int64(len(raw))
		docs = append(docs, doc{Doc: raw})
	}

	var output bytes.Buffer
	w := NewGenericWriter[doc](&output)
	if _, err := w.Write(docs); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		b.Fatal(err)
	}
	chunk := file.RowGroups()[0].ColumnChunks()[0]
	b.SetBytes(total)
	b.ResetTimer()
	for b.Loop() {
		pages := chunk.Pages()
		var n int64
		for {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			n += page.NumValues()
			Release(page)
		}
		if err := pages.Close(); err != nil {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatal("scan read no values")
		}
	}
}
