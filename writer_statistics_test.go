package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestWriterStatistics(t *testing.T) {
	type Record struct {
		ID       int64    `parquet:"id"`
		Name     string   `parquet:"name"`
		Email    *string  `parquet:"email,optional"`
		Tags     []string `parquet:"tags,list"`
		Metadata *string  `parquet:"metadata,optional"`
	}

	email1 := "alice@example.com"
	email2 := "bob@example.com"
	metadata1 := "meta1"

	records := []Record{
		{ID: 1, Name: "Alice", Email: &email1, Tags: []string{"tag1", "tag2"}, Metadata: &metadata1},
		{ID: 2, Name: "Bob", Email: &email2, Tags: []string{"tag3"}, Metadata: nil},
		{ID: 3, Name: "Charlie", Email: nil, Tags: nil, Metadata: nil},
		{ID: 4, Name: "Dave", Email: nil, Tags: []string{"tag4", "tag5", "tag6"}, Metadata: nil},
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf,
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "name")),
	)

	for _, record := range records {
		if err := w.Write(&record); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	metadata := f.Metadata()
	if len(metadata.RowGroups) != 1 {
		t.Fatalf("expected 1 row group, got %d", len(metadata.RowGroups))
	}

	rowGroup := metadata.RowGroups[0]
	columns := rowGroup.Columns

	type expectedStats struct {
		unencodedBytes        int64
		bloomFilterLength     int32
		defLevelHistogram     []int64
		repLevelHistogram     []int64
		colIndexDefLevelCount int
		colIndexRepLevelCount int
	}

	expected := map[string]expectedStats{
		"id": {},
		"name": {
			unencodedBytes:    19, // "Alice"(5) + "Bob"(3) + "Charlie"(7) + "Dave"(4)
			bloomFilterLength: 47,
		},
		"email": {
			unencodedBytes:        32,            // "alice@example.com"(17) + "bob@example.com"(15)
			defLevelHistogram:     []int64{2, 2}, // 2 nulls, 2 values
			colIndexDefLevelCount: 2,             // 1 page * (maxDefinitionLevel + 1)
		},
		"tags list element": {
			unencodedBytes:        24,            // "tag1"(4) + "tag2"(4) + "tag3"(4) + "tag4"(4) + "tag5"(4) + "tag6"(4)
			defLevelHistogram:     []int64{1, 6}, // 1 null list, 6 elements
			repLevelHistogram:     []int64{4, 3}, // 4 first elements, 3 repeated elements
			colIndexDefLevelCount: 2,             // 1 page * (maxDefinitionLevel + 1)
			colIndexRepLevelCount: 2,             // 1 page * (maxRepetitionLevel + 1)
		},
		"metadata": {
			unencodedBytes:        5,             // "meta1"(5)
			defLevelHistogram:     []int64{3, 1}, // 3 nulls, 1 value
			colIndexDefLevelCount: 2,             // 1 page * (maxDefinitionLevel + 1)
		},
	}

	for _, col := range columns {
		pathStr := ""
		for j, part := range col.MetaData.PathInSchema {
			if j > 0 {
				pathStr += " "
			}
			pathStr += string(part)
		}

		exp, ok := expected[pathStr]
		if !ok {
			t.Errorf("unexpected column: %s", pathStr)
			continue
		}

		if col.MetaData.Type == format.ByteArray {
			if got := col.MetaData.SizeStatistics.UnencodedByteArrayDataBytes; got != exp.unencodedBytes {
				t.Errorf("column %s: UnencodedByteArrayDataBytes = %d, want %d", pathStr, got, exp.unencodedBytes)
			}
		}

		if exp.bloomFilterLength > 0 {
			if got := col.MetaData.BloomFilterLength; got != exp.bloomFilterLength {
				t.Errorf("column %s: BloomFilterLength = %d, want %d", pathStr, got, exp.bloomFilterLength)
			}
		}

		if exp.defLevelHistogram != nil {
			got := col.MetaData.SizeStatistics.DefinitionLevelHistogram
			if len(got) != len(exp.defLevelHistogram) {
				t.Errorf("column %s: DefinitionLevelHistogram length = %d, want %d", pathStr, len(got), len(exp.defLevelHistogram))
			} else {
				for j, want := range exp.defLevelHistogram {
					if got[j] != want {
						t.Errorf("column %s: DefinitionLevelHistogram[%d] = %d, want %d", pathStr, j, got[j], want)
					}
				}
			}
		}

		if exp.repLevelHistogram != nil {
			got := col.MetaData.SizeStatistics.RepetitionLevelHistogram
			if len(got) != len(exp.repLevelHistogram) {
				t.Errorf("column %s: RepetitionLevelHistogram length = %d, want %d", pathStr, len(got), len(exp.repLevelHistogram))
			} else {
				for j, want := range exp.repLevelHistogram {
					if got[j] != want {
						t.Errorf("column %s: RepetitionLevelHistogram[%d] = %d, want %d", pathStr, j, got[j], want)
					}
				}
			}
		}
	}

	columnIndexes := f.ColumnIndexes()
	for i, colIdx := range columnIndexes {
		if i >= len(columns) {
			break
		}
		col := columns[i]
		pathStr := ""
		for j, part := range col.MetaData.PathInSchema {
			if j > 0 {
				pathStr += " "
			}
			pathStr += string(part)
		}

		exp, ok := expected[pathStr]
		if !ok {
			continue
		}

		if exp.colIndexDefLevelCount > 0 {
			if got := len(colIdx.DefinitionLevelHistogram); got != exp.colIndexDefLevelCount {
				t.Errorf("column %s: ColumnIndex.DefinitionLevelHistogram length = %d, want %d", pathStr, got, exp.colIndexDefLevelCount)
			}
		}

		if exp.colIndexRepLevelCount > 0 {
			if got := len(colIdx.RepetitionLevelHistogram); got != exp.colIndexRepLevelCount {
				t.Errorf("column %s: ColumnIndex.RepetitionLevelHistogram length = %d, want %d", pathStr, got, exp.colIndexRepLevelCount)
			}
		}
	}
}
