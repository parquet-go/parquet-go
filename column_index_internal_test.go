package parquet

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

func TestIncrementByteArrayInplace(t *testing.T) {
	testCases := [][]byte{
		{0x00, 0x01, 0x02, 0x03}, {0x00, 0x01, 0x02, 0x04},
		{0x00, 0x01, 0x02, 0xFF}, {0x00, 0x01, 0x03, 0x00},
		{0x00, 0x01, 0xFF, 0xFF}, {0x00, 0x02, 0x00, 0x00},
		{0xFF, 0xFF, 0xFF, 0xFF}, {0xFF, 0xFF, 0xFF, 0xFF},
	}

	for i := 0; i < len(testCases); i += 2 {
		input := testCases[i]
		expected := testCases[i+1]
		actual := copyBytes(input)
		incrementByteArrayInplace(actual)
		if !bytes.Equal(actual, expected) {
			t.Errorf("incrementByteArrayInplace(%v) = %v, want %v", input, actual, expected)
		}
	}
}

func TestSearchFileAndMultiColumnIndexAbsentNullCounts(t *testing.T) {
	makeFormatIndex := func(withNullCounts bool) *format.ColumnIndex {
		indexer := Int32Type.NewColumnIndexer(0)
		indexer.IndexPage(2, 0, ValueOf(int32(1)), ValueOf(int32(2)))
		fi := indexer.ColumnIndex()
		fi.BoundaryOrder = format.Ascending
		if !withNullCounts {
			fi.NullCounts = nil
		}
		return &fi
	}

	fileNoCounts := &FileColumnIndex{index: makeFormatIndex(false), kind: Int32}
	if got := Search(fileNoCounts, Value{}, Int32Type); got != 0 {
		t.Errorf("FileColumnIndex without NullCounts: Search(null)=%d want=0", got)
	}

	fileWithCounts := &FileColumnIndex{index: makeFormatIndex(true), kind: Int32}
	if got := Search(fileWithCounts, Value{}, Int32Type); got != 1 {
		t.Errorf("FileColumnIndex with NullCounts=[0]: Search(null)=%d want=1", got)
	}

	multi := &multiColumnIndex{
		indexes:  []ColumnIndex{fileWithCounts, fileNoCounts},
		offsets:  []int{0, 1, 2},
		numPages: 2,
		typ:      Int32Type,
	}
	if got := Search(multi, Value{}, Int32Type); got != 1 {
		t.Errorf("multiColumnIndex [withCounts=[0], noCounts]: Search(null)=%d want=1", got)
	}
}
