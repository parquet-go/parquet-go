package parquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type fixedListTestRow struct {
	ID  int64     `parquet:"id"`
	Emb []float32 `parquet:"emb,list"`
}

func makeFixedListTestRows(numRows, n int) []fixedListTestRow {
	prng := rand.New(rand.NewSource(1))
	rows := make([]fixedListTestRow, numRows)
	for i := range rows {
		emb := make([]float32, n)
		for j := range emb {
			emb[j] = prng.Float32()
		}
		rows[i] = fixedListTestRow{ID: int64(i), Emb: emb}
	}
	return rows
}

func writeFixedListTestFile[T any](t testing.TB, rows []T, options ...WriterOption) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	w := NewGenericWriter[T](buf, options...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// withFixedListFastPath runs f twice, once with the fast path enabled and
// once disabled, and returns both results for comparison.
func withFixedListFastPath[T any](t testing.TB, f func(t testing.TB) T) (enabled, disabled T) {
	t.Helper()
	defer func(saved bool) { fixedListFastPathEnabled = saved }(fixedListFastPathEnabled)
	fixedListFastPathEnabled = true
	enabled = f(t)
	fixedListFastPathEnabled = false
	disabled = f(t)
	return enabled, disabled
}

// readAllValues reads every value of every column of the file, capturing the
// exact Value stream (data, repetition levels, definition levels).
func readAllValues(t testing.TB, data []byte) [][]Value {
	t.Helper()
	f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	numColumns := len(f.Schema().Columns())
	columns := make([][]Value, numColumns)
	for _, rg := range f.RowGroups() {
		for c, chunk := range rg.ColumnChunks() {
			pages := chunk.Pages()
			buf := make([]Value, 333) // odd size to exercise partial reads
			for {
				p, err := pages.ReadPage()
				if err != nil {
					break
				}
				vr := p.Values()
				for {
					n, err := vr.ReadValues(buf)
					for _, v := range buf[:n] {
						columns[c] = append(columns[c], v.Clone())
					}
					if err != nil {
						break
					}
				}
				Release(p)
			}
			pages.Close()
		}
	}
	return columns
}

func readAllRows[T any](t testing.TB, data []byte) []T {
	t.Helper()
	r := NewGenericReader[T](bytes.NewReader(data))
	defer r.Close()
	var rows []T
	buf := make([]T, 100)
	for {
		n, err := r.Read(buf)
		rows = append(rows, buf[:n]...)
		if err != nil {
			break
		}
	}
	return rows
}

func assertValuesEqual(t *testing.T, enabled, disabled [][]Value) {
	t.Helper()
	if len(enabled) != len(disabled) {
		t.Fatalf("column count mismatch: %d != %d", len(enabled), len(disabled))
	}
	for c := range enabled {
		if len(enabled[c]) != len(disabled[c]) {
			t.Fatalf("column %d: value count mismatch: %d != %d", c, len(enabled[c]), len(disabled[c]))
		}
		for i := range enabled[c] {
			a, b := enabled[c][i], disabled[c][i]
			if !Equal(a, b) || a.RepetitionLevel() != b.RepetitionLevel() || a.DefinitionLevel() != b.DefinitionLevel() {
				t.Fatalf("column %d value %d mismatch: fast=%v(r%d,d%d) slow=%v(r%d,d%d)",
					c, i, a, a.RepetitionLevel(), a.DefinitionLevel(), b, b.RepetitionLevel(), b.DefinitionLevel())
			}
		}
	}
}

func TestFixedListFastPathEquivalence(t *testing.T) {
	codecs := map[string]compress.Codec{
		"uncompressed": &uncompressed.Codec{},
		"zstd":         &zstd.Codec{},
	}
	for _, n := range []int{1, 3, 8, 15, 16, 768} {
		for _, pageVersion := range []int{1, 2} {
			for codecName, codec := range codecs {
				name := fmt.Sprintf("n=%d/v%d/%s", n, pageVersion, codecName)
				t.Run(name, func(t *testing.T) {
					rows := makeFixedListTestRows(1000, n)
					data := writeFixedListTestFile(t, rows,
						DataPageVersion(pageVersion),
						Compression(codec),
					)

					valuesEnabled, valuesDisabled := withFixedListFastPath(t, func(t testing.TB) [][]Value {
						return readAllValues(t, data)
					})
					assertValuesEqual(t, valuesEnabled, valuesDisabled)

					rowsEnabled, rowsDisabled := withFixedListFastPath(t, func(t testing.TB) []fixedListTestRow {
						return readAllRows[fixedListTestRow](t, data)
					})
					if !reflect.DeepEqual(rowsEnabled, rowsDisabled) {
						t.Fatal("rows read with fast path differ from rows read without")
					}
					if !reflect.DeepEqual(rowsEnabled, rows) {
						t.Fatal("rows read with fast path differ from rows written")
					}
				})
			}
		}
	}
}

// TestFixedListFastPathDetected verifies that pages of fixed-length list
// files actually take the fast path (guarding against silent regressions
// where detection stops triggering).
func TestFixedListFastPathDetected(t *testing.T) {
	for _, pageVersion := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", pageVersion), func(t *testing.T) {
			rows := makeFixedListTestRows(1000, 16)
			data := writeFixedListTestFile(t, rows, DataPageVersion(pageVersion))

			f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, rg := range f.RowGroups() {
				for _, chunk := range rg.ColumnChunks() {
					if chunk.Type().Kind() != Float {
						continue
					}
					pages := chunk.Pages()
					for {
						p, err := pages.ReadPage()
						if err != nil {
							break
						}
						bp, ok := p.(*bufferedPage)
						if !ok {
							t.Fatalf("expected *bufferedPage, got %T", p)
						}
						fp, ok := bp.Page.(*fixedRepeatedPage)
						if !ok {
							t.Fatalf("expected fast-path page, got %T", bp.Page)
						}
						if fp.FixedListLength() != 16 {
							t.Fatalf("fixed list length = %d, want 16", fp.FixedListLength())
						}
						if fp.NumRows()*16 != fp.NumValues() {
							t.Fatalf("NumRows %d * 16 != NumValues %d", fp.NumRows(), fp.NumValues())
						}
						found = true
						Release(p)
					}
					pages.Close()
				}
			}
			if !found {
				t.Fatal("no data pages read for the list column")
			}
		})
	}
}

// TestFixedListFastPathVariableLists verifies that files containing
// variable-length lists (which must not take the fast path) read
// identically with the flag on and off, and that mixed files work.
func TestFixedListFastPathVariableLists(t *testing.T) {
	prng := rand.New(rand.NewSource(2))
	rows := make([]fixedListTestRow, 1000)
	for i := range rows {
		emb := make([]float32, prng.Intn(8))
		for j := range emb {
			emb[j] = prng.Float32()
		}
		rows[i] = fixedListTestRow{ID: int64(i), Emb: emb}
	}
	data := writeFixedListTestFile(t, rows)

	valuesEnabled, valuesDisabled := withFixedListFastPath(t, func(t testing.TB) [][]Value {
		return readAllValues(t, data)
	})
	assertValuesEqual(t, valuesEnabled, valuesDisabled)

	rowsEnabled, rowsDisabled := withFixedListFastPath(t, func(t testing.TB) []fixedListTestRow {
		return readAllRows[fixedListTestRow](t, data)
	})
	if !reflect.DeepEqual(rowsEnabled, rowsDisabled) {
		t.Fatal("rows read with fast path differ from rows read without")
	}
}

// TestFixedListFastPathSeek verifies SeekToRow and page slicing behave the
// same on fast-path pages.
func TestFixedListFastPathSeek(t *testing.T) {
	rows := makeFixedListTestRows(5000, 7)
	data := writeFixedListTestFile(t, rows)

	for _, seekTo := range []int64{0, 1, 999, 2500, 4999} {
		readFrom := func(t testing.TB) []fixedListTestRow {
			r := NewGenericReader[fixedListTestRow](bytes.NewReader(data))
			defer r.Close()
			if err := r.SeekToRow(seekTo); err != nil {
				t.Fatal(err)
			}
			buf := make([]fixedListTestRow, 100)
			n, err := r.Read(buf)
			if n == 0 && err != nil {
				t.Fatal(err)
			}
			return buf[:n]
		}
		enabled, disabled := withFixedListFastPath(t, readFrom)
		if !reflect.DeepEqual(enabled, disabled) {
			t.Fatalf("seek to %d: fast path rows differ", seekTo)
		}
		if len(enabled) == 0 || enabled[0].ID != seekTo {
			t.Fatalf("seek to %d: first row has ID %d", seekTo, enabled[0].ID)
		}
	}
}

// TestFixedListFastPathLevels verifies lazily materialized levels match the
// levels produced by the regular decode path.
func TestFixedListFastPathLevels(t *testing.T) {
	rows := makeFixedListTestRows(1000, 5)
	data := writeFixedListTestFile(t, rows)

	type levels struct{ rep, def [][]byte }
	readLevels := func(t testing.TB) levels {
		f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		var l levels
		for _, rg := range f.RowGroups() {
			for _, chunk := range rg.ColumnChunks() {
				if chunk.Type().Kind() != Float {
					continue
				}
				pages := chunk.Pages()
				for {
					p, err := pages.ReadPage()
					if err != nil {
						break
					}
					l.rep = append(l.rep, bytes.Clone(p.RepetitionLevels()))
					l.def = append(l.def, bytes.Clone(p.DefinitionLevels()))
					Release(p)
				}
				pages.Close()
			}
		}
		return l
	}

	enabled, disabled := withFixedListFastPath(t, readLevels)
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("materialized levels differ between fast and regular paths")
	}
}
