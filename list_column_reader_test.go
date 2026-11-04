package parquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// TestListFastPathEngaged guards against silent regressions where the
// GenericReader bulk list path stops being selected for eligible schemas.
func TestListFastPathEngaged(t *testing.T) {
	rows := makeFixedListTestRows(10, 4)
	data := writeFixedListTestFile(t, rows)

	r := NewGenericReader[fixedListTestRow](bytes.NewReader(data))
	defer r.Close()
	if r.fast == nil {
		t.Fatal("bulk list read path not engaged for eligible schema")
	}
	if len(r.fast.columns) != 1 {
		t.Fatalf("expected 1 fast column, got %d", len(r.fast.columns))
	}
	if r.fast.allDetached {
		t.Fatal("allDetached should be false: the id column uses the regular path")
	}

	type allLists struct {
		A []float32 `parquet:"a"`
		B []int64   `parquet:"b,list"`
	}
	prng := rand.New(rand.NewSource(3))
	listRows := make([]allLists, 100)
	for i := range listRows {
		a := make([]float32, 3)
		b := make([]int64, 1+prng.Intn(4))
		for j := range a {
			a[j] = prng.Float32()
		}
		for j := range b {
			b[j] = prng.Int63()
		}
		listRows[i] = allLists{A: a, B: b}
	}
	listData := writeFixedListTestFile(t, listRows)
	r2 := NewGenericReader[allLists](bytes.NewReader(listData))
	defer r2.Close()
	if r2.fast == nil || !r2.fast.allDetached || len(r2.fast.columns) != 2 {
		t.Fatal("bulk list read path not fully engaged for all-list schema")
	}

	got := readAllRows[allLists](t, listData)
	if !reflect.DeepEqual(got, listRows) {
		t.Fatal("all-list rows read back differ from rows written")
	}
}

// TestListFastPathIneligible verifies schemas that must not use the bulk
// path: optional elements, nested lists, string lists, non-slice fields.
func TestListFastPathIneligible(t *testing.T) {
	type ineligible struct {
		A []string   `parquet:"a,list"` // byte-array elements
		B []int16    `parquet:"b,list"` // Go element narrower than the int32 column
		C []*float32 `parquet:"c,list"` // optional (pointer) elements
		D int64      `parquet:"d"`      // not a list
		E []bool     `parquet:"e,list"` // boolean elements (bit-packed)
	}
	rows := []ineligible{{
		A: []string{"x", "y"},
		B: []int16{1, 2, 3},
		C: []*float32{new(float32)},
		D: 42,
		E: []bool{true, false},
	}}
	data := writeFixedListTestFile(t, rows)

	r := NewGenericReader[ineligible](bytes.NewReader(data))
	defer r.Close()
	if r.fast != nil {
		t.Fatal("bulk list read path engaged for ineligible schema")
	}
	got := readAllRows[ineligible](t, data)
	if !reflect.DeepEqual(got, rows) {
		t.Fatal("ineligible rows read back differ from rows written")
	}
}

type listVariantsRow struct {
	ID       int64     `parquet:"id"`
	Plain    []float32 `parquet:"plain"`
	List     []float64 `parquet:"lst,list"`
	Optional []int32   `parquet:"opt,optional,list"`
	Uints    []uint64  `parquet:"uints,list"`
	Name     string    `parquet:"name"`
}

func makeListVariantsRows(prng *rand.Rand, numRows int, fixedN int) []listVariantsRow {
	rows := make([]listVariantsRow, numRows)
	for i := range rows {
		lengthOf := func() int {
			if fixedN > 0 {
				return fixedN
			}
			return prng.Intn(6)
		}
		plain := make([]float32, lengthOf())
		for j := range plain {
			plain[j] = prng.Float32()
		}
		lst := make([]float64, lengthOf())
		for j := range lst {
			lst[j] = prng.Float64()
		}
		var opt []int32
		if fixedN > 0 || prng.Intn(4) != 0 { // sometimes nil (null list)
			opt = make([]int32, lengthOf())
			for j := range opt {
				opt[j] = prng.Int31()
			}
		}
		uints := make([]uint64, lengthOf())
		for j := range uints {
			uints[j] = prng.Uint64()
		}
		rows[i] = listVariantsRow{
			ID:       int64(i),
			Plain:    plain,
			List:     lst,
			Optional: opt,
			Uints:    uints,
			Name:     fmt.Sprintf("row-%d", i),
		}
	}
	return rows
}

// TestListFastPathEquivalence compares rows read with the bulk list path
// against rows read with it disabled, across fixed and variable lists, page
// versions, and seeks.
func TestListFastPathEquivalence(t *testing.T) {
	prng := rand.New(rand.NewSource(4))
	for _, fixedN := range []int{0, 1, 3, 16} { // 0 = variable lengths
		for _, pageVersion := range []int{1, 2} {
			t.Run(fmt.Sprintf("fixedN=%d/v%d", fixedN, pageVersion), func(t *testing.T) {
				rows := makeListVariantsRows(prng, 3000, fixedN)
				data := writeFixedListTestFile(t, rows, DataPageVersion(pageVersion))

				enabled, disabled := withFixedListFastPath(t, func(t testing.TB) []listVariantsRow {
					return readAllRows[listVariantsRow](t, data)
				})
				if len(enabled) != len(rows) {
					t.Fatalf("read %d rows, want %d", len(enabled), len(rows))
				}
				for i := range disabled {
					if !reflect.DeepEqual(enabled[i], disabled[i]) {
						t.Fatalf("row %d differs:\nfast: %+v\nslow: %+v", i, enabled[i], disabled[i])
					}
				}
			})
		}
	}
}

// TestListFastPathSeek verifies seeking behaves identically on the bulk
// list path.
func TestListFastPathSeek(t *testing.T) {
	prng := rand.New(rand.NewSource(5))
	rows := makeListVariantsRows(prng, 5000, 0)
	data := writeFixedListTestFile(t, rows)

	for _, seekTo := range []int64{0, 1, 1234, 2500, 4999} {
		readFrom := func(t testing.TB) []listVariantsRow {
			r := NewGenericReader[listVariantsRow](bytes.NewReader(data))
			defer r.Close()
			if err := r.SeekToRow(seekTo); err != nil {
				t.Fatal(err)
			}
			buf := make([]listVariantsRow, 50)
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

	// Interleave reads and seeks on a single reader.
	interleaved := func(t testing.TB) []listVariantsRow {
		r := NewGenericReader[listVariantsRow](bytes.NewReader(data))
		defer r.Close()
		var out []listVariantsRow
		buf := make([]listVariantsRow, 10)
		for _, seekTo := range []int64{100, 4000, 100, 0, 4995} {
			if err := r.SeekToRow(seekTo); err != nil {
				t.Fatal(err)
			}
			n, err := r.Read(buf)
			if n == 0 && err != nil {
				t.Fatal(err)
			}
			out = append(out, buf[:n]...)
		}
		return out
	}
	enabled, disabled := withFixedListFastPath(t, interleaved)
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("interleaved seek/read: fast path rows differ")
	}
}

// TestListFastPathSmallBatches reads with a tiny buffer to exercise batch
// boundaries falling inside pages.
func TestListFastPathSmallBatches(t *testing.T) {
	prng := rand.New(rand.NewSource(6))
	rows := makeListVariantsRows(prng, 1111, 0)
	data := writeFixedListTestFile(t, rows)

	readSmall := func(t testing.TB) []listVariantsRow {
		r := NewGenericReader[listVariantsRow](bytes.NewReader(data))
		defer r.Close()
		var out []listVariantsRow
		buf := make([]listVariantsRow, 7)
		for {
			n, err := r.Read(buf)
			for i := range buf[:n] {
				// Rows must remain valid after subsequent reads: deep-copy
				// nothing, later batches must not overwrite earlier slices.
				out = append(out, buf[i])
			}
			if err != nil {
				break
			}
		}
		return out
	}
	enabled, disabled := withFixedListFastPath(t, readSmall)
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("small batches: fast path rows differ")
	}
	if len(enabled) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(enabled), len(rows))
	}
}

// TestListFastPathDictionary exercises the dictionary gather path with a
// dict-encoded list column holding few distinct values.
func TestListFastPathDictionary(t *testing.T) {
	type dictRow struct {
		ID  int64     `parquet:"id"`
		Emb []float32 `parquet:"emb,list,dict"`
	}
	prng := rand.New(rand.NewSource(8))
	distinct := []float32{1.5, -2.5, 3.25, 0}
	rows := make([]dictRow, 2000)
	for i := range rows {
		emb := make([]float32, 4)
		for j := range emb {
			emb[j] = distinct[prng.Intn(len(distinct))]
		}
		rows[i] = dictRow{ID: int64(i), Emb: emb}
	}
	data := writeFixedListTestFile(t, rows)

	enabled, disabled := withFixedListFastPath(t, func(t testing.TB) []dictRow {
		return readAllRows[dictRow](t, data)
	})
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("dictionary-encoded rows differ between fast and regular paths")
	}
	if !reflect.DeepEqual(enabled, rows) {
		t.Fatal("dictionary-encoded rows read back differ from rows written")
	}
}

// TestListFastPathMultiRowGroup verifies reads spanning multiple row groups.
func TestListFastPathMultiRowGroup(t *testing.T) {
	prng := rand.New(rand.NewSource(9))
	rows := makeListVariantsRows(prng, 3000, 0)

	buf := new(bytes.Buffer)
	w := NewGenericWriter[listVariantsRow](buf)
	for chunk := 0; chunk < 3; chunk++ {
		if _, err := w.Write(rows[chunk*1000 : (chunk+1)*1000]); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	f, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.RowGroups()) != 3 {
		t.Fatalf("expected 3 row groups, got %d", len(f.RowGroups()))
	}

	enabled, disabled := withFixedListFastPath(t, func(t testing.TB) []listVariantsRow {
		return readAllRows[listVariantsRow](t, data)
	})
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("multi row group rows differ between fast and regular paths")
	}
	if !reflect.DeepEqual(enabled, rows) {
		t.Fatal("multi row group rows read back differ from rows written")
	}

	// Seek across row group boundaries.
	for _, seekTo := range []int64{999, 1000, 1500, 2999} {
		seekRead := func(t testing.TB) []listVariantsRow {
			r := NewGenericReader[listVariantsRow](bytes.NewReader(data))
			defer r.Close()
			if err := r.SeekToRow(seekTo); err != nil {
				t.Fatal(err)
			}
			buf := make([]listVariantsRow, 10)
			n, err := r.Read(buf)
			if n == 0 && err != nil {
				t.Fatal(err)
			}
			return buf[:n]
		}
		enabled, disabled := withFixedListFastPath(t, seekRead)
		if !reflect.DeepEqual(enabled, disabled) {
			t.Fatalf("seek to %d: fast path rows differ", seekTo)
		}
	}
}

// TestListFastPathSmallPages uses a tiny page buffer size so that column
// chunks are split into many small pages.
func TestListFastPathSmallPages(t *testing.T) {
	prng := rand.New(rand.NewSource(10))
	for _, fixedN := range []int{0, 8} {
		rows := makeListVariantsRows(prng, 2000, fixedN)
		for _, pageVersion := range []int{1, 2} {
			data := writeFixedListTestFile(t, rows,
				DataPageVersion(pageVersion),
				PageBufferSize(1024),
			)
			enabled, disabled := withFixedListFastPath(t, func(t testing.TB) []listVariantsRow {
				return readAllRows[listVariantsRow](t, data)
			})
			if !reflect.DeepEqual(enabled, disabled) {
				t.Fatalf("fixedN=%d v%d: small page rows differ between fast and regular paths", fixedN, pageVersion)
			}
			if !reflect.DeepEqual(enabled, rows) {
				t.Fatalf("fixedN=%d v%d: small page rows read back differ from rows written", fixedN, pageVersion)
			}
		}
	}
}

// TestListFastPathRowGroupReader exercises NewGenericRowGroupReader over an
// in-memory buffer row group.
func TestListFastPathRowGroupReader(t *testing.T) {
	prng := rand.New(rand.NewSource(7))
	rows := makeListVariantsRows(prng, 500, 0)

	buffer := NewGenericBuffer[listVariantsRow]()
	if _, err := buffer.Write(rows); err != nil {
		t.Fatal(err)
	}

	readBuffered := func(t testing.TB) []listVariantsRow {
		r := NewGenericRowGroupReader[listVariantsRow](buffer)
		defer r.Close()
		var out []listVariantsRow
		buf := make([]listVariantsRow, 64)
		for {
			n, err := r.Read(buf)
			out = append(out, buf[:n]...)
			if err != nil {
				break
			}
		}
		return out
	}
	enabled, disabled := withFixedListFastPath(t, readBuffered)
	if !reflect.DeepEqual(enabled, disabled) {
		t.Fatal("row group reader: fast path rows differ")
	}
	if len(enabled) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(enabled), len(rows))
	}
}
