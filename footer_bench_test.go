package parquet_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// benchFooterShape describes a synthetic file used to benchmark footer
// decoding at different footer sizes. Footer size grows with the number of
// row groups and columns, not with the number of rows.
type benchFooterShape struct {
	rowGroups int
	columns   int
}

func (s benchFooterShape) String() string {
	return fmt.Sprintf("rowGroups=%d/columns=%d", s.rowGroups, s.columns)
}

var benchFooterShapes = []benchFooterShape{
	{rowGroups: 1, columns: 10},
	{rowGroups: 8, columns: 50},
	{rowGroups: 32, columns: 200},
}

// generateBenchFile writes a parquet file with the given number of row
// groups and columns. Half the columns are int64 and half are strings so
// the footer carries realistic statistics.
func generateBenchFile(tb testing.TB, shape benchFooterShape) []byte {
	fields := make(parquet.Group, shape.columns)
	for i := range shape.columns {
		if i%2 == 0 {
			fields[fmt.Sprintf("col%03d", i)] = parquet.Leaf(parquet.Int64Type)
		} else {
			fields[fmt.Sprintf("col%03d", i)] = parquet.String()
		}
	}
	schema := parquet.NewSchema("bench", fields)

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)

	const rowsPerGroup = 25
	row := make(parquet.Row, shape.columns)
	for g := range shape.rowGroups {
		for r := range rowsPerGroup {
			for i := range shape.columns {
				if i%2 == 0 {
					row[i] = parquet.Int64Value(int64(g*rowsPerGroup+r)).Level(0, 0, i)
				} else {
					row[i] = parquet.ByteArrayValue(fmt.Appendf(nil, "value-%03d-%05d", i, g*rowsPerGroup+r)).Level(0, 0, i)
				}
			}
			if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
				tb.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

// BenchmarkOpenFooter compares the cost of opening parquet files through the
// different footer paths, across footer sizes and with the page index
// enabled or disabled. The reader is fully in-memory, so the numbers isolate
// CPU and allocation cost: this is the floor that a byte-caching setup (e.g.
// caching footer bytes in memcached) cannot go below without WithFooter.
func BenchmarkOpenFooter(b *testing.B) {
	for _, shape := range benchFooterShapes {
		data := generateBenchFile(b, shape)
		size := int64(len(data))
		footerSize := binary.LittleEndian.Uint32(data[size-8 : size-4])
		footerBytes := data[size-int64(footerSize)-8 : size]

		footer, err := parquet.ReadFooter(bytes.NewReader(data), size)
		if err != nil {
			b.Fatal(err)
		}

		benchmarks := []struct {
			name    string
			options []parquet.FileOption
		}{
			{name: "OpenFile"},
			{name: "OpenFile+SkipPageIndex", options: []parquet.FileOption{
				parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true),
			}},
			{name: "OpenFile+WithFooter", options: []parquet.FileOption{
				parquet.WithFooter(footer),
			}},
			{name: "OpenFile+WithFooter+SkipPageIndex", options: []parquet.FileOption{
				parquet.WithFooter(footer), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true),
			}},
		}

		b.Run(shape.String(), func(b *testing.B) {
			b.Logf("file size: %d bytes, footer size: %d bytes", size, footerSize)

			for _, bench := range benchmarks {
				b.Run(bench.name, func(b *testing.B) {
					r := bytes.NewReader(data)
					b.ReportAllocs()
					for b.Loop() {
						if _, err := parquet.OpenFile(r, size, bench.options...); err != nil {
							b.Fatal(err)
						}
					}
				})
			}

			b.Run("ReadFooter", func(b *testing.B) {
				r := bytes.NewReader(data)
				b.ReportAllocs()
				for b.Loop() {
					if _, err := parquet.ReadFooter(r, size); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("DecodeFooter", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := parquet.DecodeFooter(footerBytes); err != nil {
						b.Fatal(err)
					}
				}
			})

			// The zero-allocation transient decode path: what a pooled
			// decoder pays per decode when re-decoding cached footer bytes.
			b.Run("FooterDecoder", func(b *testing.B) {
				decoder := new(format.FooterDecoder)
				payload := footerBytes[:len(footerBytes)-8]
				b.ReportAllocs()
				for b.Loop() {
					if _, _, err := decoder.Decode(payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
