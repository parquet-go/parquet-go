package parquet_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type benchFixedListRow struct {
	Emb []float32 `parquet:"emb"`
}

// makeFixedListFile writes a parquet file where every row contains a list of
// exactly n float32 values, and returns the serialized file.
func makeFixedListFile(b testing.TB, n, numRows int) []byte {
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[benchFixedListRow](buf)
	batch := make([]benchFixedListRow, 1000)
	v := float32(0)
	for i := range batch {
		emb := make([]float32, n)
		for j := range emb {
			emb[j] = v
			v++
		}
		batch[i].Emb = emb
	}
	for written := 0; written < numRows; written += len(batch) {
		m := min(len(batch), numRows-written)
		if _, err := w.Write(batch[:m]); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func benchmarkFixedListSizes(n int) (numRows int) {
	// Target ~32 MiB of float32 payload for each list width.
	const targetValues = 8 << 20
	numRows = targetValues / n
	if numRows == 0 {
		numRows = 1
	}
	return numRows
}

// BenchmarkFixedListGenericReader measures the end-to-end GenericReader path:
// pages -> Value pipeline -> rows -> reflect reconstruct.
func BenchmarkFixedListGenericReader(b *testing.B) {
	for _, n := range []int{3, 16, 768} {
		numRows := benchmarkFixedListSizes(n)
		data := makeFixedListFile(b, n, numRows)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(numRows * n * 4))
			out := make([]benchFixedListRow, 1000)
			for b.Loop() {
				r := parquet.NewGenericReader[benchFixedListRow](bytes.NewReader(data))
				total := 0
				for {
					m, err := r.Read(out)
					total += m
					if err != nil {
						break
					}
				}
				r.Close()
				if total != numRows {
					b.Fatalf("read %d rows, want %d", total, numRows)
				}
			}
		})
	}
}

// BenchmarkFixedListPageValues measures the Value pipeline without row
// assembly or reflection: pages -> Values().ReadValues.
func BenchmarkFixedListPageValues(b *testing.B) {
	for _, n := range []int{3, 16, 768} {
		numRows := benchmarkFixedListSizes(n)
		data := makeFixedListFile(b, n, numRows)
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(numRows * n * 4))
			values := make([]parquet.Value, 4096)
			for b.Loop() {
				total := 0
				for _, rg := range f.RowGroups() {
					pages := rg.ColumnChunks()[0].Pages()
					for {
						p, err := pages.ReadPage()
						if err != nil {
							break
						}
						vr := p.Values()
						for {
							m, err := vr.ReadValues(values)
							total += m
							if err != nil {
								break
							}
						}
						parquet.Release(p)
					}
					pages.Close()
				}
				if total != numRows*n {
					b.Fatalf("read %d values, want %d", total, numRows*n)
				}
			}
		})
	}
}

// BenchmarkFixedListPageData measures the columnar floor: pages decoded and
// accessed through Data() without materializing values.
func BenchmarkFixedListPageData(b *testing.B) {
	for _, n := range []int{3, 16, 768} {
		numRows := benchmarkFixedListSizes(n)
		data := makeFixedListFile(b, n, numRows)
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(numRows * n * 4))
			var sink float32
			for b.Loop() {
				total := 0
				for _, rg := range f.RowGroups() {
					pages := rg.ColumnChunks()[0].Pages()
					for {
						p, err := pages.ReadPage()
						if err != nil {
							break
						}
						pdata := p.Data()
						vals := pdata.Float()
						total += len(vals)
						if len(vals) > 0 {
							sink += vals[len(vals)-1]
						}
						parquet.Release(p)
					}
					pages.Close()
				}
				if total != numRows*n {
					b.Fatalf("read %d values, want %d", total, numRows*n)
				}
			}
			_ = sink
		})
	}
}
