package parquet

import (
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"
)

// sliceRowReader yields pre-built rows, cut into arbitrary batch sizes.
type sliceRowReader struct {
	rows []Row
	off  int
}

func (r *sliceRowReader) ReadRows(rows []Row) (int, error) {
	if r.off >= len(r.rows) {
		return 0, io.EOF
	}
	n := min(len(rows), len(r.rows)-r.off)
	for i := range n {
		rows[i] = append(rows[i][:0], r.rows[r.off+i]...)
	}
	r.off += n
	return n, nil
}

// referenceMergeRows drains a mergedRowReader built without run detection by
// forcing the streak below the threshold before every read. It preserves the
// exact per-row replay behavior, serving as the oracle for run mode.
func drainMerged(t *testing.T, m RowReader) []Row {
	t.Helper()
	var out []Row
	buf := make([]Row, 7) // odd batch size to exercise batch boundaries
	for {
		n, err := m.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, slices.Clone(row))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

// TestMergedRowReaderRunDetection verifies that run detection produces exactly
// the same output (including the order of equal rows) as per-row replay, across
// random workloads ranging from fully interleaved to fully disjoint, with
// duplicated keys within and across readers.
func TestMergedRowReaderRunDetection(t *testing.T) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}

	makeRow := func(key int64, reader int, pos int) Row {
		return Row{
			ValueOf(key).Level(0, 0, 0),
			ValueOf(fmt.Sprintf("r%d-%d", reader, pos)).Level(0, 0, 1), // provenance
		}
	}

	prng := rand.New(rand.NewSource(1))

	for _, numReaders := range []int{3, 4, 5, 8, 16} {
		for _, mode := range []string{"interleaved", "disjoint", "boundary_overlap", "duplicates"} {
			t.Run(fmt.Sprintf("k=%d/%s", numReaders, mode), func(t *testing.T) {
				inputs := make([][]Row, numReaders)
				for r := range numReaders {
					numRows := 50 + prng.Intn(200)
					keys := make([]int64, numRows)
					for i := range keys {
						switch mode {
						case "interleaved":
							keys[i] = prng.Int63n(1000)
						case "disjoint":
							keys[i] = int64(r*100_000) + prng.Int63n(10_000)
						case "boundary_overlap":
							keys[i] = int64(r*1000) + prng.Int63n(1100) // ~10% overlap
						case "duplicates":
							keys[i] = prng.Int63n(20) // heavy ties everywhere
						}
					}
					slices.Sort(keys)
					rows := make([]Row, numRows)
					for i, k := range keys {
						rows[i] = makeRow(k, r, i)
					}
					inputs[r] = rows
				}

				newReaders := func() []RowReader {
					readers := make([]RowReader, numReaders)
					for i := range readers {
						readers[i] = &sliceRowReader{rows: inputs[i]}
					}
					return readers
				}

				// Reference: same merger with run detection disabled by keeping
				// the streak permanently below the threshold.
				ref := mergeRowReaders(newReaders(), compare).(*mergedRowReader)
				refOut := drainReference(t, ref)

				got := drainMerged(t, mergeRowReaders(newReaders(), compare))

				if len(got) != len(refOut) {
					t.Fatalf("row count %d, want %d", len(got), len(refOut))
				}
				for i := range got {
					if compare(got[i], refOut[i]) != 0 || got[i][1].String() != refOut[i][1].String() {
						t.Fatalf("row %d differs: got key=%d src=%s, want key=%d src=%s",
							i, got[i][0].Int64(), got[i][1].String(), refOut[i][0].Int64(), refOut[i][1].String())
					}
				}
			})
		}
	}
}

// drainReference drains m with run detection suppressed (streak reset before
// every batch), reproducing pure per-row replay.
func drainReference(t *testing.T, m *mergedRowReader) []Row {
	t.Helper()
	var out []Row
	buf := make([]Row, 7)
	for {
		m.streak = -1 << 30 // never reaches runDetectionStreak within a batch...
		n, err := m.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, slices.Clone(row))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

// TestMergedRowReader2RunDetection verifies that run galloping in the two-way
// merge produces exactly the same output (including the order of equal rows
// and batch-boundary behavior) as per-row merging, using the same merger with
// galloping disabled as the oracle.
func TestMergedRowReader2RunDetection(t *testing.T) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}

	makeRow := func(key int64, reader int, pos int) Row {
		return Row{
			ValueOf(key).Level(0, 0, 0),
			ValueOf(fmt.Sprintf("r%d-%d", reader, pos)).Level(0, 0, 1), // provenance
		}
	}

	prng := rand.New(rand.NewSource(2))

	for _, mode := range []string{"interleaved", "disjoint", "boundary_overlap", "duplicates"} {
		t.Run(mode, func(t *testing.T) {
			inputs := make([][]Row, 2)
			for r := range 2 {
				numRows := 50 + prng.Intn(500)
				keys := make([]int64, numRows)
				for i := range keys {
					switch mode {
					case "interleaved":
						keys[i] = prng.Int63n(1000)
					case "disjoint":
						keys[i] = int64(r*100_000) + prng.Int63n(10_000)
					case "boundary_overlap":
						keys[i] = int64(r*1000) + prng.Int63n(1100) // ~10% overlap
					case "duplicates":
						keys[i] = prng.Int63n(20) // heavy ties everywhere
					}
				}
				slices.Sort(keys)
				rows := make([]Row, numRows)
				for i, k := range keys {
					rows[i] = makeRow(k, r, i)
				}
				inputs[r] = rows
			}

			newMerge := func(noGallop bool) RowReader {
				readers := make([]RowReader, 2)
				for i := range readers {
					readers[i] = &sliceRowReader{rows: inputs[i]}
				}
				m := mergeRowReaders(readers, compare).(*mergedRowReader2)
				m.noGallop = noGallop
				return m
			}

			refOut := drainMerged(t, newMerge(true))
			got := drainMerged(t, newMerge(false))

			if len(got) != len(refOut) {
				t.Fatalf("row count %d, want %d", len(got), len(refOut))
			}
			for i := range got {
				if compare(got[i], refOut[i]) != 0 || got[i][1].String() != refOut[i][1].String() {
					t.Fatalf("row %d differs: got key=%d src=%s, want key=%d src=%s",
						i, got[i][0].Int64(), got[i][1].String(), refOut[i][0].Int64(), refOut[i][1].String())
				}
			}
		})
	}
}

func benchmarkMergedRowReader(b *testing.B, numReaders int, mode string, runs bool) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}
	prng := rand.New(rand.NewSource(1))
	inputs := make([][]Row, numReaders)
	for r := range numReaders {
		const numRows = 10_000
		keys := make([]int64, numRows)
		for i := range keys {
			switch mode {
			case "interleaved":
				keys[i] = prng.Int63n(1_000_000)
			case "boundary_overlap":
				keys[i] = int64(r*numRows) + prng.Int63n(numRows+numRows/10)
			}
		}
		slices.Sort(keys)
		rows := make([]Row, numRows)
		for i, k := range keys {
			rows[i] = Row{ValueOf(k).Level(0, 0, 0)}
		}
		inputs[r] = rows
	}

	buf := make([]Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readers := make([]RowReader, numReaders)
		for i := range readers {
			readers[i] = &sliceRowReader{rows: inputs[i]}
		}
		m := mergeRowReaders(readers, compare).(*mergedRowReader)
		for {
			var err error
			if !runs {
				m.streak = -1 << 30
			}
			_, err = m.ReadRows(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMergedRowReaderRuns(b *testing.B) {
	for _, k := range []int{4, 16} {
		for _, mode := range []string{"boundary_overlap", "interleaved"} {
			b.Run(fmt.Sprintf("k=%d/%s/runs", k, mode), func(b *testing.B) { benchmarkMergedRowReader(b, k, mode, true) })
			b.Run(fmt.Sprintf("k=%d/%s/base", k, mode), func(b *testing.B) { benchmarkMergedRowReader(b, k, mode, false) })
		}
	}
}

func benchmarkMergedRowReader2(b *testing.B, mode string, gallop bool) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}
	prng := rand.New(rand.NewSource(1))
	inputs := make([][]Row, 2)
	for r := range 2 {
		const numRows = 10_000
		keys := make([]int64, numRows)
		for i := range keys {
			switch mode {
			case "interleaved":
				keys[i] = prng.Int63n(1_000_000)
			case "boundary_overlap":
				keys[i] = int64(r*numRows) + prng.Int63n(numRows+numRows/10)
			}
		}
		slices.Sort(keys)
		rows := make([]Row, numRows)
		for i, k := range keys {
			rows[i] = Row{ValueOf(k).Level(0, 0, 0)}
		}
		inputs[r] = rows
	}

	buf := make([]Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readers := make([]RowReader, 2)
		for i := range readers {
			readers[i] = &sliceRowReader{rows: inputs[i]}
		}
		m := mergeRowReaders(readers, compare).(*mergedRowReader2)
		m.noGallop = !gallop
		for {
			_, err := m.ReadRows(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMergedRowReader2Runs(b *testing.B) {
	for _, mode := range []string{"boundary_overlap", "interleaved"} {
		b.Run(fmt.Sprintf("%s/gallop", mode), func(b *testing.B) { benchmarkMergedRowReader2(b, mode, true) })
		b.Run(fmt.Sprintf("%s/base", mode), func(b *testing.B) { benchmarkMergedRowReader2(b, mode, false) })
	}
}
