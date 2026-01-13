package parquet

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

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
	w := NewWriter(buf,
		BloomFilters(SplitBlockFilter(10, "name")),
	)

	for _, record := range records {
		if err := w.Write(&record); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
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

// TestStatsV1 tests that the StatsV1 option causes the deprecated Min/Max
// statistics fields to be written in column chunk metadata.
func TestStatsV1(t *testing.T) {
	type Record struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	records := []Record{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// Test without DeprecatedDataPageStatistics - deprecated fields should be nil
	t.Run("without DeprecatedDataPageStatistics", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := NewWriter(buf)

		for _, record := range records {
			if err := w.Write(&record); err != nil {
				t.Fatal(err)
			}
		}

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		metadata := f.Metadata()
		for _, col := range metadata.RowGroups[0].Columns {
			stats := col.MetaData.Statistics
			// MinValue and MaxValue should be set
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Errorf("MinValue or MaxValue is nil for column %v", col.MetaData.PathInSchema)
			}
			// Deprecated Min and Max should be nil (not set by default)
			if stats.Min != nil || stats.Max != nil {
				t.Errorf("deprecated Min or Max should be nil for column %v when DeprecatedDataPageStatistics is disabled", col.MetaData.PathInSchema)
			}
		}
	})

	// Test with DeprecatedDataPageStatistics - deprecated fields should be populated
	t.Run("with DeprecatedDataPageStatistics", func(t *testing.T) {
		buf := new(bytes.Buffer)
		w := NewWriter(buf, DeprecatedDataPageStatistics(true))

		for _, record := range records {
			if err := w.Write(&record); err != nil {
				t.Fatal(err)
			}
		}

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatal(err)
		}

		metadata := f.Metadata()
		for _, col := range metadata.RowGroups[0].Columns {
			stats := col.MetaData.Statistics
			// MinValue and MaxValue should be set
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Errorf("MinValue or MaxValue is nil for column %v", col.MetaData.PathInSchema)
			}
			// Deprecated Min and Max should also be set
			if stats.Min == nil || stats.Max == nil {
				t.Errorf("deprecated Min or Max is nil for column %v when DeprecatedDataPageStatistics is enabled", col.MetaData.PathInSchema)
			}
			// The values should match
			if !bytes.Equal(stats.Min, stats.MinValue) {
				t.Errorf("Min != MinValue for column %v: %v != %v", col.MetaData.PathInSchema, stats.Min, stats.MinValue)
			}
			if !bytes.Equal(stats.Max, stats.MaxValue) {
				t.Errorf("Max != MaxValue for column %v: %v != %v", col.MetaData.PathInSchema, stats.Max, stats.MaxValue)
			}
		}

		// Verify specific values for the ID column (int64)
		idCol := metadata.RowGroups[0].Columns[0]
		idStats := idCol.MetaData.Statistics
		expectedMinID := int64(1)
		expectedMaxID := int64(3)

		gotMinID := int64(binary.LittleEndian.Uint64(idStats.MinValue))
		gotMaxID := int64(binary.LittleEndian.Uint64(idStats.MaxValue))

		if gotMinID != expectedMinID {
			t.Errorf("ID MinValue = %d, want %d", gotMinID, expectedMinID)
		}
		if gotMaxID != expectedMaxID {
			t.Errorf("ID MaxValue = %d, want %d", gotMaxID, expectedMaxID)
		}

		// Verify the deprecated fields have the same values
		gotMinIDDeprecated := int64(binary.LittleEndian.Uint64(idStats.Min))
		gotMaxIDDeprecated := int64(binary.LittleEndian.Uint64(idStats.Max))

		if gotMinIDDeprecated != expectedMinID {
			t.Errorf("ID Min (deprecated) = %d, want %d", gotMinIDDeprecated, expectedMinID)
		}
		if gotMaxIDDeprecated != expectedMaxID {
			t.Errorf("ID Max (deprecated) = %d, want %d", gotMaxIDDeprecated, expectedMaxID)
		}

		// Verify specific values for the Name column (string)
		nameCol := metadata.RowGroups[0].Columns[1]
		nameStats := nameCol.MetaData.Statistics
		expectedMinName := "Alice"
		expectedMaxName := "Charlie"

		if string(nameStats.MinValue) != expectedMinName {
			t.Errorf("Name MinValue = %q, want %q", string(nameStats.MinValue), expectedMinName)
		}
		if string(nameStats.MaxValue) != expectedMaxName {
			t.Errorf("Name MaxValue = %q, want %q", string(nameStats.MaxValue), expectedMaxName)
		}

		// Verify the deprecated fields have the same values
		if string(nameStats.Min) != expectedMinName {
			t.Errorf("Name Min (deprecated) = %q, want %q", string(nameStats.Min), expectedMinName)
		}
		if string(nameStats.Max) != expectedMaxName {
			t.Errorf("Name Max (deprecated) = %q, want %q", string(nameStats.Max), expectedMaxName)
		}
	})
}

func BenchmarkLevelHistogram(b *testing.B) {
	prng := rand.New(rand.NewSource(0))
	levels := make([]byte, 100_000)
	for i := range levels {
		levels[i] = byte(prng.Intn(4))
	}

	const maxLevel byte = 3

	b.Run("current", func(b *testing.B) {
		columnHistogram := make([]int64, maxLevel+1)
		var pageHistograms []int64

		b.ResetTimer()
		for range b.N {
			clear(columnHistogram)
			pageHistograms = pageHistograms[:0]

			accumulateLevelHistogram(columnHistogram, levels)
			pageHistograms = appendPageLevelHistogram(pageHistograms, levels, maxLevel)
		}
	})

	b.Run("optimized", func(b *testing.B) {
		columnHistogram := make([]int64, maxLevel+1)
		var pageHistograms []int64

		b.ResetTimer()
		for range b.N {
			clear(columnHistogram)
			pageHistograms = pageHistograms[:0]

			pageHistograms = accumulateAndAppendPageLevelHistogram(
				columnHistogram,
				pageHistograms,
				levels,
				maxLevel,
			)
		}
	})
}
