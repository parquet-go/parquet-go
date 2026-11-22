package parquet

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"
)

// TestOptionalPlainZeroIsNull reproduces https://github.com/parquet-go/parquet-go/issues/717.
// A plain optional field writes the Go zero value as NULL. A zero that follows
// one present value was stored as 0 when that present value ended in the
// middle of a bitmap word.
func TestOptionalPlainZeroIsNull(t *testing.T) {
	t.Run("reported", func(t *testing.T) {
		cases := [][]int64{
			{0, 5, 0},
			{0, 5, 0, 0},
			{0, 0, 5, 0},
			{0, 5, 0, 5, 0},
			{0, -5, 0},
			{5, 0, 5, 5, 0},
			{0, 0, 5, 5, 0},
		}
		for _, in := range cases {
			t.Run(fmt.Sprint(in), func(t *testing.T) {
				checkOptionalInt64(t, in)
			})
		}
	})

	t.Run("mask", func(t *testing.T) {
		// p leading zeros, then p present values, then a zero. For p != 32
		// the old mask (1<<p)-1 matches this shape and swallows the trailing zero.
		for p := 1; p <= 63; p++ {
			in := maskShape(p)
			t.Run(fmt.Sprintf("p%d", p), func(t *testing.T) {
				checkOptionalInt64(t, in)
			})
		}
		for _, prefix := range []int{63, 64, 65, 127, 128} {
			in := append(make([]int64, prefix), 0, 5, 0)
			t.Run(fmt.Sprintf("prefix%d", prefix), func(t *testing.T) {
				checkOptionalInt64(t, in)
			})
		}
	})

	t.Run("random", func(t *testing.T) {
		lengths := make([]int, 0, 160)
		for n := 1; n <= 128; n++ {
			lengths = append(lengths, n)
		}
		lengths = append(lengths, 129, 192, 255, 256, 257, 300, 512, 1000)
		for _, pZero := range []float64{0.5, 0.9} {
			t.Run(fmt.Sprintf("pZero%g", pZero), func(t *testing.T) {
				rng := rand.New(rand.NewSource(1))
				for _, n := range lengths {
					in := randomOptionalInt64(rng, n, pZero)
					checkOptionalInt64(t, in)
				}
			})
		}
	})

	t.Run("types", func(t *testing.T) {
		t.Run("int8", func(t *testing.T) { checkOptionalNumbers[int8](t) })
		t.Run("int16", func(t *testing.T) { checkOptionalNumbers[int16](t) })
		t.Run("int32", func(t *testing.T) { checkOptionalNumbers[int32](t) })
		t.Run("int64", func(t *testing.T) { checkOptionalNumbers[int64](t) })
		t.Run("uint32", func(t *testing.T) { checkOptionalNumbers[uint32](t) })
		t.Run("float32", func(t *testing.T) { checkOptionalNumbers[float32](t) })
		t.Run("float64", func(t *testing.T) { checkOptionalNumbers[float64](t) })
		t.Run("bool", func(t *testing.T) { checkOptionalBool(t) })
		t.Run("string", func(t *testing.T) { checkOptionalString(t) })
		t.Run("bytes", func(t *testing.T) { checkOptionalBytes(t) })
		t.Run("fixed", func(t *testing.T) { checkOptionalFixed(t) })
	})

	t.Run("twoColumns", func(t *testing.T) {
		type row struct {
			A int64 `parquet:"a,optional"`
			B int64 `parquet:"b,optional"`
		}
		in := []row{
			{A: 0, B: 0},
			{A: 5, B: 0},
			{A: 0, B: 9},
			{A: 0, B: 9},
			{A: 0, B: 0},
		}
		wantA := []int64{0, 5, 0, 0, 0}
		wantB := []int64{0, 0, 9, 9, 0}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			checkInt64Column(t, got, wantA, 0, 2)
			checkInt64Column(t, got, wantB, 1, 2)
		}
		t.Run("writer", func(t *testing.T) { check(t, writeFileRows(t, in)) })
		t.Run("buffer", func(t *testing.T) { check(t, writeBufferRows(t, in)) })
	})

	// List elements share writeRowsFuncOfOptional. The slice writer emits the
	// first element on its own and the rest as one batch, so the bug pattern
	// has to start at element 1. A list longer than 64 elements crosses a
	// bitmap word inside that batch.
	t.Run("list", func(t *testing.T) {
		type row struct {
			V []int64 `parquet:"v,list" parquet-element:",optional"`
		}
		cases := [][]int64{
			{7, 0, 5, 0},
			{7, 0, 5, 0, 0},
			{0, 5, 0},
		}
		// Zeros fill the rest of each bitmap word, which is the shape the
		// broken mask treats as present. The 5s sit at batch index 1 and at
		// the same place in the next word (element index is batch index + 1).
		long := make([]int64, 130)
		long[0] = 7
		long[2] = 5
		long[66] = 9
		cases = append(cases, long)
		for _, elems := range cases {
			t.Run(fmt.Sprintf("n%d", len(elems)), func(t *testing.T) {
				in := []row{{V: elems}}
				t.Run("writer", func(t *testing.T) {
					checkOptionalList(t, writeFileRows(t, in), elems)
				})
				t.Run("buffer", func(t *testing.T) {
					checkOptionalList(t, writeBufferRows(t, in), elems)
				})
			})
		}
	})

	// map[string]string written to a group batches every row of a column into
	// one optional scan. GenericBuffer takes that path; GenericWriter deconstructs
	// a mismatched schema row by row.
	t.Run("mapStringGroup", func(t *testing.T) {
		type mapRow struct {
			Nested map[string]string
		}
		type groupRow struct {
			Nested struct {
				A string `parquet:",optional"`
				B string `parquet:",optional"`
			}
		}
		in := []mapRow{
			{Nested: map[string]string{"A": "", "B": "b0"}},
			{Nested: map[string]string{"A": "x", "B": ""}},
			{Nested: map[string]string{"A": "", "B": "b2"}},
			{Nested: map[string]string{"A": "", "B": "b3"}},
		}
		schema := SchemaOf(groupRow{})
		check := func(t *testing.T, got []Row) {
			t.Helper()
			checkStringColumn(t, got, []string{"", "x", "", ""}, 0, 2)
			checkStringColumn(t, got, []string{"b0", "", "b2", "b3"}, 1, 2)
		}
		t.Run("writer", func(t *testing.T) {
			var buf bytes.Buffer
			w := NewGenericWriter[mapRow](&buf, schema)
			if _, err := w.Write(in); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			var got []Row
			for _, rg := range f.RowGroups() {
				got = append(got, collectRows(t, rg.Rows())...)
			}
			check(t, got)
		})
		t.Run("buffer", func(t *testing.T) {
			check(t, writeBufferRows(t, in, schema))
		})
	})

	// Pointers are null only when the pointer is nil. A pointer to zero stays present.
	t.Run("pointer", func(t *testing.T) {
		type row struct {
			V *int64 `parquet:"v,optional"`
		}
		five, zero := int64(5), int64(0)
		in := []row{{nil}, {&five}, {nil}, {&zero}, {&five}, {&zero}}
		wantNull := []bool{true, false, true, false, false, false}
		wantVal := []int64{0, 5, 0, 0, 5, 0}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			if len(got) != len(in) {
				t.Fatalf("got %d rows, want %d", len(got), len(in))
			}
			for i := range in {
				v := columnValue(t, got[i], 0, 1)
				if wantNull[i] {
					if !v.IsNull() || v.DefinitionLevel() != 0 {
						t.Fatalf("row %d: got %+v, want null", i, v)
					}
					continue
				}
				if v.IsNull() || v.DefinitionLevel() != 1 || v.Int64() != wantVal[i] {
					t.Fatalf("row %d: got %+v, want %d", i, v, wantVal[i])
				}
			}
		}
		t.Run("writer", func(t *testing.T) { check(t, writeFileRows(t, in)) })
		t.Run("buffer", func(t *testing.T) { check(t, writeBufferRows(t, in)) })
	})

	// time.Time uses its own run scan, not the bitmap in writeRowsFuncOfOptional.
	t.Run("time", func(t *testing.T) {
		type row struct {
			V time.Time `parquet:"v,optional"`
		}
		present := time.Unix(5, 0).UTC()
		in := []row{{}, {V: present}, {}, {}, {V: present}}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			if len(got) != len(in) {
				t.Fatalf("got %d rows, want %d", len(got), len(in))
			}
			for i, row := range in {
				v := columnValue(t, got[i], 0, 1)
				if row.V.IsZero() {
					if !v.IsNull() || v.DefinitionLevel() != 0 {
						t.Fatalf("row %d: got %+v, want null", i, v)
					}
					continue
				}
				if v.IsNull() || v.DefinitionLevel() != 1 || v.Int64() != row.V.UnixNano() {
					t.Fatalf("row %d: got %+v, want %d", i, v, row.V.UnixNano())
				}
			}
		}
		t.Run("writer", func(t *testing.T) { check(t, writeFileRows(t, in)) })
		t.Run("buffer", func(t *testing.T) { check(t, writeBufferRows(t, in)) })
	})
}

func checkOptionalInt64(t *testing.T, want []int64) {
	t.Helper()
	type row struct {
		V int64 `parquet:"v,optional"`
	}
	in := make([]row, len(want))
	for i, v := range want {
		in[i].V = v
	}
	t.Run("writer", func(t *testing.T) {
		checkInt64Column(t, writeFileRows(t, in), want, 0, 1)
	})
	t.Run("buffer", func(t *testing.T) {
		checkInt64Column(t, writeBufferRows(t, in), want, 0, 1)
	})
}

func checkInt64Column(t *testing.T, got []Row, want []int64, col, width int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, w := range want {
		v := columnValue(t, got[i], col, width)
		if w == 0 {
			if !v.IsNull() || v.DefinitionLevel() != 0 {
				t.Fatalf("len %d row %d: got %+v, want null\n%v", len(want), i, v, want)
			}
			continue
		}
		if v.IsNull() || v.DefinitionLevel() != 1 || v.Int64() != w {
			t.Fatalf("len %d row %d: got %+v, want %d\n%v", len(want), i, v, w, want)
		}
	}
}

type optionalNumber interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint32 | ~float32 | ~float64
}

func checkOptionalNumbers[T optionalNumber](t *testing.T) {
	t.Helper()
	short := []T{0, 5, 0}
	long := make([]T, 70)
	long[1] = 5
	long[66] = 5
	for _, vals := range [][]T{short, long, {0, 5, 0, 0}, {0, 0, 5, 0}} {
		checkOptionalNumberSlice(t, vals)
	}
}

func checkOptionalNumberSlice[T optionalNumber](t *testing.T, want []T) {
	t.Helper()
	type row struct {
		V T `parquet:"v,optional"`
	}
	in := make([]row, len(want))
	for i, v := range want {
		in[i].V = v
	}
	check := func(t *testing.T, got []Row) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %d rows, want %d", len(got), len(want))
		}
		var zero T
		for i, w := range want {
			v := columnValue(t, got[i], 0, 1)
			if w == zero {
				if !v.IsNull() || v.DefinitionLevel() != 0 {
					t.Fatalf("row %d: got %+v, want null", i, v)
				}
				continue
			}
			if v.IsNull() || v.DefinitionLevel() != 1 || parquetNumber(v) != float64(w) {
				t.Fatalf("row %d: got %+v, want %v", i, v, w)
			}
		}
	}
	t.Run(fmt.Sprintf("n%d/writer", len(want)), func(t *testing.T) {
		check(t, writeFileRows(t, in))
	})
	t.Run(fmt.Sprintf("n%d/buffer", len(want)), func(t *testing.T) {
		check(t, writeBufferRows(t, in))
	})
}

func checkOptionalBool(t *testing.T) {
	t.Helper()
	type row struct {
		V bool `parquet:"v,optional"`
	}
	patterns := [][]bool{
		{false, true, false},
		{false, true, false, false},
		make([]bool, 70),
	}
	patterns[2][1] = true
	patterns[2][66] = true
	for _, vals := range patterns {
		in := make([]row, len(vals))
		for i, v := range vals {
			in[i].V = v
		}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			if len(got) != len(vals) {
				t.Fatalf("got %d rows, want %d", len(got), len(vals))
			}
			for i, w := range vals {
				v := columnValue(t, got[i], 0, 1)
				if !w {
					if !v.IsNull() || v.DefinitionLevel() != 0 {
						t.Fatalf("row %d: got %+v, want null", i, v)
					}
					continue
				}
				if v.IsNull() || v.DefinitionLevel() != 1 || !v.Boolean() {
					t.Fatalf("row %d: got %+v, want true", i, v)
				}
			}
		}
		t.Run(fmt.Sprintf("n%d/writer", len(vals)), func(t *testing.T) {
			check(t, writeFileRows(t, in))
		})
		t.Run(fmt.Sprintf("n%d/buffer", len(vals)), func(t *testing.T) {
			check(t, writeBufferRows(t, in))
		})
	}
}

func checkOptionalString(t *testing.T) {
	t.Helper()
	type row struct {
		V string `parquet:"v,optional"`
	}
	long := make([]string, 70)
	long[1] = "x"
	long[66] = "y"
	patterns := [][]string{
		{"", "x", ""},
		{"", "x", "", ""},
		long,
	}
	for _, vals := range patterns {
		in := make([]row, len(vals))
		for i, v := range vals {
			in[i].V = v
		}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			checkStringColumn(t, got, vals, 0, 1)
		}
		t.Run(fmt.Sprintf("n%d/writer", len(vals)), func(t *testing.T) {
			check(t, writeFileRows(t, in))
		})
		t.Run(fmt.Sprintf("n%d/buffer", len(vals)), func(t *testing.T) {
			check(t, writeBufferRows(t, in))
		})
	}
}

func checkOptionalBytes(t *testing.T) {
	t.Helper()
	type row struct {
		V []byte `parquet:"v,optional"`
	}
	// nil is null. A non-nil empty slice is an empty value, not null.
	patterns := [][][]byte{
		{nil, {1, 2}, nil},
		{nil, {}, nil},
		{{}, {7}, {}},
		{nil, {1}, nil, nil},
	}
	for i, vals := range patterns {
		in := make([]row, len(vals))
		for j, v := range vals {
			in[j].V = v
		}
		check := func(t *testing.T, got []Row) {
			t.Helper()
			if len(got) != len(vals) {
				t.Fatalf("got %d rows, want %d", len(got), len(vals))
			}
			for j, w := range vals {
				v := columnValue(t, got[j], 0, 1)
				if w == nil {
					if !v.IsNull() || v.DefinitionLevel() != 0 {
						t.Fatalf("row %d: got %+v, want null", j, v)
					}
					continue
				}
				if v.IsNull() || v.DefinitionLevel() != 1 || !bytes.Equal(v.ByteArray(), w) {
					t.Fatalf("row %d: got %+v, want %q", j, v, w)
				}
			}
		}
		t.Run(fmt.Sprintf("p%d/writer", i), func(t *testing.T) {
			check(t, writeFileRows(t, in))
		})
		t.Run(fmt.Sprintf("p%d/buffer", i), func(t *testing.T) {
			check(t, writeBufferRows(t, in))
		})
	}
}

func checkOptionalFixed(t *testing.T) {
	t.Helper()
	type row struct {
		V [8]byte `parquet:"v,optional"`
	}
	var zero, present [8]byte
	present[0] = 1
	in := []row{{zero}, {present}, {zero}, {zero}}
	check := func(t *testing.T, got []Row) {
		t.Helper()
		if len(got) != len(in) {
			t.Fatalf("got %d rows, want %d", len(got), len(in))
		}
		for i, row := range in {
			v := columnValue(t, got[i], 0, 1)
			if row.V == zero {
				if !v.IsNull() || v.DefinitionLevel() != 0 {
					t.Fatalf("row %d: got %+v, want null", i, v)
				}
				continue
			}
			if v.IsNull() || v.DefinitionLevel() != 1 || !bytes.Equal(v.ByteArray(), row.V[:]) {
				t.Fatalf("row %d: got %+v, want %x", i, v, row.V)
			}
		}
	}
	t.Run("writer", func(t *testing.T) { check(t, writeFileRows(t, in)) })
	t.Run("buffer", func(t *testing.T) { check(t, writeBufferRows(t, in)) })
}

func checkOptionalList(t *testing.T, got []Row, want []int64) {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	row := got[0]
	if len(row) != len(want) {
		t.Fatalf("got %d values, want %d (%+v)", len(row), len(want), row)
	}
	for i, w := range want {
		v := row[i]
		if w == 0 {
			if !v.IsNull() || v.DefinitionLevel() != 1 {
				t.Fatalf("elem %d: got %+v, want null", i, v)
			}
			continue
		}
		if v.IsNull() || v.DefinitionLevel() != 2 || v.Int64() != w {
			t.Fatalf("elem %d: got %+v, want %d", i, v, w)
		}
	}
}

func checkStringColumn(t *testing.T, got []Row, want []string, col, width int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, w := range want {
		v := columnValue(t, got[i], col, width)
		if w == "" {
			if !v.IsNull() || v.DefinitionLevel() != 0 {
				t.Fatalf("row %d: got %+v, want null", i, v)
			}
			continue
		}
		if v.IsNull() || v.DefinitionLevel() != 1 || string(v.ByteArray()) != w {
			t.Fatalf("row %d: got %+v, want %q", i, v, w)
		}
	}
}

func columnValue(t *testing.T, row Row, col, width int) Value {
	t.Helper()
	if len(row) != width {
		t.Fatalf("got %d values, want %d (%+v)", len(row), width, row)
	}
	return row[col]
}

func parquetNumber(v Value) float64 {
	switch v.Kind() {
	case Int32:
		return float64(v.Int32())
	case Int64:
		return float64(v.Int64())
	case Float:
		return float64(v.Float())
	case Double:
		return v.Double()
	default:
		return 0
	}
}

func maskShape(p int) []int64 {
	out := make([]int64, 2*p+1)
	for i := range p {
		out[p+i] = 5
	}
	return out
}

func randomOptionalInt64(rng *rand.Rand, n int, pZero float64) []int64 {
	out := make([]int64, n)
	for i := range out {
		if rng.Float64() < pZero {
			continue
		}
		v := rng.Int63n(20) + 1
		if rng.Intn(2) == 0 {
			v = -v
		}
		out[i] = v
	}
	return out
}

func writeFileRows[T any](t *testing.T, rows []T) []Row {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[T](&buf)
	n, err := w.Write(rows)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var out []Row
	for _, rg := range f.RowGroups() {
		out = append(out, collectRows(t, rg.Rows())...)
	}
	return out
}

func writeBufferRows[T any](t *testing.T, rows []T, opts ...RowGroupOption) []Row {
	t.Helper()
	b := NewGenericBuffer[T](opts...)
	n, err := b.Write(rows)
	if err != nil {
		t.Fatalf("buffer write: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("buffered %d rows, want %d", n, len(rows))
	}
	return collectRows(t, b.Rows())
}

func collectRows(t *testing.T, rows Rows) []Row {
	t.Helper()
	defer rows.Close()
	buf := make([]Row, 32)
	var out []Row
	for {
		n, err := rows.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, row.Clone())
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("read rows: %v", err)
		}
		if n == 0 {
			t.Fatal("read rows: no progress")
		}
	}
}
