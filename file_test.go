package parquet_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

var testdataFiles []string

func init() {
	entries, _ := os.ReadDir("testdata")
	for _, e := range entries {
		testdataFiles = append(testdataFiles, filepath.Join("testdata", e.Name()))
	}
}

func TestOpenFile(t *testing.T) {
	for _, path := range testdataFiles {
		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			s, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}

			p, err := parquet.OpenFile(f, s.Size())
			if err != nil {
				t.Fatal(err)
			}

			if size := p.Size(); size != s.Size() {
				t.Errorf("file size mismatch: want=%d got=%d", s.Size(), size)
			}

			root := p.Root()
			b := new(strings.Builder)
			parquet.PrintSchema(b, root.Name(), root)
			t.Log(b)

			printColumns(t, p.Root(), "")
		})
	}
}

func TestOpenFileWithoutPageIndex(t *testing.T) {
	for _, path := range testdataFiles {
		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			s, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}

			fileWithIndex, err := parquet.OpenFile(f, s.Size())
			if err != nil {
				t.Fatal(err)
			}
			fileWithoutIndex, err := parquet.OpenFile(f, s.Size(), parquet.SkipPageIndex(true))
			if err != nil {
				t.Fatal(err)
			}

			if size := fileWithoutIndex.Size(); size != s.Size() {
				t.Errorf("file size mismatch: want=%d got=%d", s.Size(), size)
			}

			for iRowGroup, rowGroup := range fileWithoutIndex.RowGroups() {
				for iChunk, chunk := range rowGroup.ColumnChunks() {
					chunkMeta := fileWithoutIndex.Metadata().RowGroups[iRowGroup].Columns[iChunk].MetaData

					preloadedColumnIndex, pErr := fileWithIndex.RowGroups()[iRowGroup].ColumnChunks()[iChunk].ColumnIndex()
					if errors.Is(pErr, parquet.ErrMissingColumnIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get column index for %s: %s", chunkMeta.PathInSchema[0], pErr)
					}
					columnIndex, err := chunk.ColumnIndex()
					if errors.Is(err, parquet.ErrMissingColumnIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get column index for %s: %s", chunkMeta.PathInSchema[0], err)
					}
					if !errors.Is(err, pErr) {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
					if preloadedColumnIndex == nil && columnIndex != nil || preloadedColumnIndex != nil && columnIndex == nil {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}

					preloadedOffsetIndex, pErr := fileWithIndex.RowGroups()[iRowGroup].ColumnChunks()[iChunk].OffsetIndex()
					if errors.Is(pErr, parquet.ErrMissingOffsetIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get offset index for %s: %s", chunkMeta.PathInSchema[0], pErr)
					}
					offsetIndex, err := chunk.OffsetIndex()
					if errors.Is(err, parquet.ErrMissingOffsetIndex) && chunkMeta.IndexPageOffset != 0 {
						t.Errorf("get offset index for %s: %s", chunkMeta.PathInSchema[0], err)
					}
					if !errors.Is(err, pErr) {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
					if preloadedOffsetIndex == nil && offsetIndex != nil || preloadedOffsetIndex != nil && offsetIndex == nil {
						t.Errorf("mismatch when opening file with and without index, chunk=%d, row group=%d", iChunk, iRowGroup)
					}
				}
			}
		})
	}
}

func printColumns(t *testing.T, col *parquet.Column, indent string) {
	if t.Failed() {
		return
	}

	path := strings.Join(col.Path(), ".")
	if col.Leaf() {
		t.Logf("%s%s %v %v", indent, path, col.Encoding(), col.Compression())
	} else {
		t.Logf("%s%s", indent, path)
	}
	indent += ". "

	buffer := make([]parquet.Value, 42)
	pages := col.Pages()
	defer pages.Close()
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}

		values := p.Values()
		numValues := int64(0)
		nullCount := int64(0)

		for {
			n, err := values.ReadValues(buffer)
			for _, v := range buffer[:n] {
				if v.Column() != col.Index() {
					t.Errorf("value read from page of column %d says it belongs to column %d", col.Index(), v.Column())
					return
				}
				if v.IsNull() {
					nullCount++
				}
			}
			numValues += int64(n)
			if err != nil {
				if err != io.EOF {
					t.Error(err)
					return
				}
				break
			}
		}

		if numValues != p.NumValues() {
			t.Errorf("page of column %d declared %d values but %d were read", col.Index(), p.NumValues(), numValues)
			return
		}

		if nullCount != p.NumNulls() {
			t.Errorf("page of column %d declared %d nulls but %d were read", col.Index(), p.NumNulls(), nullCount)
			return
		}

		parquet.Release(p)
	}

	for _, child := range col.Columns() {
		printColumns(t, child, indent)
	}
}

func TestFileKeyValueMetadata(t *testing.T) {
	type Row struct {
		Name string
	}

	f, err := createParquetFile(
		makeRows([]Row{{Name: "A"}, {Name: "B"}, {Name: "C"}}),
		parquet.KeyValueMetadata("hello", "ignore this one"),
		parquet.KeyValueMetadata("hello", "world"),
		parquet.KeyValueMetadata("answer", "42"),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range [][2]string{
		{"hello", "world"},
		{"answer", "42"},
	} {
		key, value := want[0], want[1]
		if found, ok := f.Lookup(key); !ok || found != value {
			t.Errorf("key/value metadata mismatch: want %q=%q but got %q=%q (found=%t)", key, value, key, found, ok)
		}
	}
}
