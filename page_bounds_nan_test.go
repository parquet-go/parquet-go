package parquet

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/memory"
)

func TestFloatPageBoundsExcludeNaN(t *testing.T) {
	nan32 := float32(math.NaN())

	tests := []struct {
		name      string
		values    []float32
		wantMin   float32
		wantMax   float32
		wantIsNaN bool
	}{
		{
			name:    "no NaN",
			values:  []float32{3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN at start",
			values:  []float32{nan32, 3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN in middle",
			values:  []float32{3.0, nan32, 1.0, nan32, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN at end",
			values:  []float32{3.0, 1.0, 2.0, nan32},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "single non-NaN",
			values:  []float32{nan32, 5.0, nan32},
			wantMin: 5.0,
			wantMax: 5.0,
		},
		{
			name:    "negative values with NaN",
			values:  []float32{nan32, -3.0, -1.0, nan32, -2.0},
			wantMin: -3.0,
			wantMax: -1.0,
		},
		{
			name:      "all NaN",
			values:    []float32{nan32, nan32, nan32},
			wantIsNaN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := &floatPage{
				typ:         floatType{},
				values:      memory.SliceBufferFrom(tt.values),
				columnIndex: ^uint16(0),
			}
			min, max, ok := page.Bounds()
			if !ok {
				t.Fatal("expected ok=true")
			}
			if tt.wantIsNaN {
				if !math.IsNaN(float64(min.Float())) {
					t.Errorf("min = %v, want NaN", min.Float())
				}
				if !math.IsNaN(float64(max.Float())) {
					t.Errorf("max = %v, want NaN", max.Float())
				}
				return
			}
			if min.Float() != tt.wantMin {
				t.Errorf("min = %v, want %v", min.Float(), tt.wantMin)
			}
			if max.Float() != tt.wantMax {
				t.Errorf("max = %v, want %v", max.Float(), tt.wantMax)
			}
		})
	}
}

func TestDoublePageBoundsExcludeNaN(t *testing.T) {
	nan64 := math.NaN()

	tests := []struct {
		name      string
		values    []float64
		wantMin   float64
		wantMax   float64
		wantIsNaN bool
	}{
		{
			name:    "no NaN",
			values:  []float64{3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN at start",
			values:  []float64{nan64, 3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN in middle",
			values:  []float64{3.0, nan64, 1.0, nan64, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "NaN at end",
			values:  []float64{3.0, 1.0, 2.0, nan64},
			wantMin: 1.0,
			wantMax: 3.0,
		},
		{
			name:    "single non-NaN",
			values:  []float64{nan64, 5.0, nan64},
			wantMin: 5.0,
			wantMax: 5.0,
		},
		{
			name:    "negative values with NaN",
			values:  []float64{nan64, -3.0, -1.0, nan64, -2.0},
			wantMin: -3.0,
			wantMax: -1.0,
		},
		{
			name:      "all NaN",
			values:    []float64{nan64, nan64, nan64},
			wantIsNaN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := &doublePage{
				typ:         doubleType{},
				values:      memory.SliceBufferFrom(tt.values),
				columnIndex: ^uint16(0),
			}
			min, max, ok := page.Bounds()
			if !ok {
				t.Fatal("expected ok=true")
			}
			if tt.wantIsNaN {
				if !math.IsNaN(min.Double()) {
					t.Errorf("min = %v, want NaN", min.Double())
				}
				if !math.IsNaN(max.Double()) {
					t.Errorf("max = %v, want NaN", max.Double())
				}
				return
			}
			if min.Double() != tt.wantMin {
				t.Errorf("min = %v, want %v", min.Double(), tt.wantMin)
			}
			if max.Double() != tt.wantMax {
				t.Errorf("max = %v, want %v", max.Double(), tt.wantMax)
			}
		})
	}
}

// TestWriterStatisticsExcludeNaN writes a parquet file with float and double
// columns containing NaN values mixed with real values, then reads back the
// column chunk statistics and verifies NaN is excluded from min/max.
func TestWriterStatisticsExcludeNaN(t *testing.T) {
	type Record struct {
		FloatVal  float32 `parquet:"float_val"`
		DoubleVal float64 `parquet:"double_val"`
	}

	nan32 := float32(math.NaN())
	nan64 := math.NaN()

	records := []Record{
		{FloatVal: nan32, DoubleVal: nan64},
		{FloatVal: 3.0, DoubleVal: 30.0},
		{FloatVal: 1.0, DoubleVal: 10.0},
		{FloatVal: nan32, DoubleVal: nan64},
		{FloatVal: 2.0, DoubleVal: 20.0},
	}

	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	for _, r := range records {
		if err := w.Write(&r); err != nil {
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

	for _, col := range f.Metadata().RowGroups[0].Columns {
		name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
		stats := col.MetaData.Statistics

		switch name {
		case "float_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatal("float_val: expected non-nil min/max statistics")
			}
			minVal := Float.Value(stats.MinValue).Float()
			maxVal := Float.Value(stats.MaxValue).Float()
			if minVal != 1.0 {
				t.Errorf("float_val min = %v, want 1.0", minVal)
			}
			if maxVal != 3.0 {
				t.Errorf("float_val max = %v, want 3.0", maxVal)
			}

		case "double_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatal("double_val: expected non-nil min/max statistics")
			}
			minVal := Double.Value(stats.MinValue).Double()
			maxVal := Double.Value(stats.MaxValue).Double()
			if minVal != 10.0 {
				t.Errorf("double_val min = %v, want 10.0", minVal)
			}
			if maxVal != 30.0 {
				t.Errorf("double_val max = %v, want 30.0", maxVal)
			}
		}
	}
}

// TestWriterStatisticsAllNaN verifies that when all float/double values are NaN,
// the statistics report NaN as min/max (not null), so readers know data exists.
func TestWriterStatisticsAllNaN(t *testing.T) {
	type Record struct {
		FloatVal  float32 `parquet:"float_val"`
		DoubleVal float64 `parquet:"double_val"`
	}

	nan32 := float32(math.NaN())
	nan64 := math.NaN()

	records := []Record{
		{FloatVal: nan32, DoubleVal: nan64},
		{FloatVal: nan32, DoubleVal: nan64},
		{FloatVal: nan32, DoubleVal: nan64},
	}

	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	for _, r := range records {
		if err := w.Write(&r); err != nil {
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

	for _, col := range f.Metadata().RowGroups[0].Columns {
		name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
		stats := col.MetaData.Statistics

		switch name {
		case "float_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatal("float_val: expected non-nil min/max for all-NaN page")
			}
			minVal := Float.Value(stats.MinValue).Float()
			maxVal := Float.Value(stats.MaxValue).Float()
			if !math.IsNaN(float64(minVal)) {
				t.Errorf("float_val min = %v, want NaN", minVal)
			}
			if !math.IsNaN(float64(maxVal)) {
				t.Errorf("float_val max = %v, want NaN", maxVal)
			}

		case "double_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatal("double_val: expected non-nil min/max for all-NaN page")
			}
			minVal := Double.Value(stats.MinValue).Double()
			maxVal := Double.Value(stats.MaxValue).Double()
			if !math.IsNaN(minVal) {
				t.Errorf("double_val min = %v, want NaN", minVal)
			}
			if !math.IsNaN(maxVal) {
				t.Errorf("double_val max = %v, want NaN", maxVal)
			}
		}
	}
}

func TestWriterDictionaryStatisticsExcludeNaN(t *testing.T) {
	type Record struct {
		FloatVal  float32 `parquet:"float_val,dict"`
		DoubleVal float64 `parquet:"double_val,dict"`
	}

	nan, inf := math.NaN(), math.Inf(1)
	makeLong := func(n, nanPos int) []float64 {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = float64(i + 10)
		}
		// Place extrema in non-zero lanes of batch 0 so AVX-512 must preserve
		// them alongside NaN lanes rather than relying on lane 0 alone.
		vals[1] = -50
		vals[2] = 500
		vals[nanPos] = nan
		return vals
	}
	allNaN := func(n int) []float64 {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = nan
		}
		return vals
	}

	tests := []struct {
		name      string
		values    []float64
		wantMin   float64
		wantMax   float64
		wantIsNaN bool
	}{
		{name: "neg_nan_zero", values: []float64{-1, nan, 0}, wantMin: -1, wantMax: 0},
		{name: "zero_nan_neg", values: []float64{0, nan, -1}, wantMin: -1, wantMax: 0},
		{name: "neginf_nan_zero", values: []float64{-inf, nan, 0}, wantMin: -inf, wantMax: 0},
		{name: "nan_neginf_zero", values: []float64{nan, -inf, 0}, wantMin: -inf, wantMax: 0},
		{name: "nan_posinf_zero", values: []float64{nan, inf, 0}, wantMin: 0, wantMax: inf},
		{name: "nan_mixed", values: []float64{nan, 3, 1, nan, 2}, wantMin: 1, wantMax: 3},
		{name: "neg_nan_zero_pos", values: []float64{-1, nan, 0, 2}, wantMin: -1, wantMax: 2},
		{name: "signed_zeros_nan", values: []float64{math.Copysign(0, -1), nan, 0}, wantMin: 0, wantMax: 0},
		{name: "all_nan_short", values: allNaN(3), wantIsNaN: true},
		{name: "all_nan_long", values: allNaN(17), wantIsNaN: true},
		{name: "long_nan_at_0", values: makeLong(20, 0), wantMin: -50, wantMax: 500},
		{name: "long_nan_in_batch0", values: makeLong(20, 5), wantMin: -50, wantMax: 500},
		{name: "long_nan_in_batch1", values: makeLong(20, 11), wantMin: -50, wantMax: 500},
		{name: "long_nan_in_tail", values: makeLong(20, 18), wantMin: -50, wantMax: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			w := NewGenericWriter[Record](buf)
			for _, v := range tt.values {
				if _, err := w.Write([]Record{{FloatVal: float32(v), DoubleVal: v}}); err != nil {
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
			for _, col := range f.Metadata().RowGroups[0].Columns {
				name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
				stats := col.MetaData.Statistics
				if stats.MinValue == nil || stats.MaxValue == nil {
					t.Fatalf("%s: expected non-nil min/max statistics", name)
				}
				var gotMin, gotMax float64
				switch name {
				case "float_val":
					gotMin = float64(Float.Value(stats.MinValue).Float())
					gotMax = float64(Float.Value(stats.MaxValue).Float())
				case "double_val":
					gotMin = Double.Value(stats.MinValue).Double()
					gotMax = Double.Value(stats.MaxValue).Double()
				default:
					continue
				}
				if tt.wantIsNaN {
					if !math.IsNaN(gotMin) || !math.IsNaN(gotMax) {
						t.Errorf("%s: min=%v max=%v, want NaN and NaN", name, gotMin, gotMax)
					}
					continue
				}
				if gotMin != tt.wantMin || gotMax != tt.wantMax {
					t.Errorf("%s: min=%v max=%v, want %v and %v", name, gotMin, gotMax, tt.wantMin, tt.wantMax)
				}
			}
		})
	}
}

func TestDictionaryFloatBoundsNaN(t *testing.T) {
	nan32 := float32(math.NaN())
	qnan32Pos := math.Float32frombits(0x7fc00001)
	qnan32Neg := math.Float32frombits(0xffc00001)
	snan32 := math.Float32frombits(0x7f800001)
	inf32 := float32(math.Inf(1))
	negZero32 := float32(math.Copysign(0, -1))
	subPos32 := float32(math.SmallestNonzeroFloat32)
	subNeg32 := -subPos32

	nan64 := math.NaN()
	qnan64Pos := math.Float64frombits(0x7ff8000000000001)
	qnan64Neg := math.Float64frombits(0xfff8000000000001)
	snan64 := math.Float64frombits(0x7ff0000000000001)
	inf64 := math.Inf(1)
	negZero64 := math.Copysign(0, -1)
	subPos64 := math.SmallestNonzeroFloat64
	subNeg64 := -subPos64

	floatDictVals := []float32{
		0,         // 0
		-1,        // 1
		1,         // 2
		-50,       // 3
		500,       // 4
		-inf32,    // 5
		inf32,     // 6
		negZero32, // 7
		nan32,     // 8
		qnan32Pos, // 9
		qnan32Neg, // 10
		snan32,    // 11
		subNeg32,  // 12
		subPos32,  // 13
	}
	doubleDictVals := []float64{
		0,         // 0
		-1,        // 1
		1,         // 2
		-50,       // 3
		500,       // 4
		-inf64,    // 5
		inf64,     // 6
		negZero64, // 7
		nan64,     // 8
		qnan64Pos, // 9
		qnan64Neg, // 10
		snan64,    // 11
		subNeg64,  // 12
		subPos64,  // 13
	}

	floatDict := newFloatDictionary(FloatType, 0, int32(len(floatDictVals)), encoding.FloatValues(floatDictVals))
	doubleDict := newDoubleDictionary(DoubleType, 0, int32(len(doubleDictVals)), encoding.DoubleValues(doubleDictVals))

	checkBounds := func(t *testing.T, indexes []int32, wantMin, wantMax float64, wantIsNaN bool) {
		t.Helper()
		fMin, fMax := floatDict.Bounds(indexes)
		dMin, dMax := doubleDict.Bounds(indexes)
		if wantIsNaN {
			if !math.IsNaN(float64(fMin.Float())) || !math.IsNaN(float64(fMax.Float())) {
				t.Errorf("float32 Bounds(%v) = (%v, %v), want (NaN, NaN)", indexes, fMin.Float(), fMax.Float())
			}
			if !math.IsNaN(dMin.Double()) || !math.IsNaN(dMax.Double()) {
				t.Errorf("float64 Bounds(%v) = (%v, %v), want (NaN, NaN)", indexes, dMin.Double(), dMax.Double())
			}
			return
		}
		if gotMin, gotMax := float64(fMin.Float()), float64(fMax.Float()); gotMin != wantMin || gotMax != wantMax {
			t.Errorf("float32 Bounds(%v) = (%v, %v), want (%v, %v)", indexes, gotMin, gotMax, wantMin, wantMax)
		}
		if gotMin, gotMax := dMin.Double(), dMax.Double(); gotMin != wantMin || gotMax != wantMax {
			t.Errorf("float64 Bounds(%v) = (%v, %v), want (%v, %v)", indexes, gotMin, gotMax, wantMin, wantMax)
		}
	}

	t.Run("sweep_nan_position", func(t *testing.T) {
		for n := 1; n <= 25; n++ {
			for nanPos := 0; nanPos < n; nanPos++ {
				indexes := make([]int32, n)
				for i := range indexes {
					indexes[i] = 0 // 0.0
				}
				if n >= 2 {
					minPos := 1
					if nanPos == minPos {
						minPos = 0
					}
					indexes[minPos] = 3 // -50
				}
				if n >= 3 {
					maxPos := 2
					if nanPos == maxPos {
						if indexes[0] == 0 {
							maxPos = 0
						} else {
							maxPos = 1
						}
					}
					indexes[maxPos] = 4 // 500
				}
				indexes[nanPos] = 8 // NaN

				if n == 1 {
					checkBounds(t, indexes, 0, 0, true)
					continue
				}
				wantMin := -50.0
				wantMax := -50.0
				if n >= 3 {
					wantMax = 500.0
				}
				checkBounds(t, indexes, wantMin, wantMax, false)
			}
		}
	})

	t.Run("all_nan_lengths", func(t *testing.T) {
		for _, n := range []int{1, 7, 8, 9, 16, 17, 25} {
			indexes := make([]int32, n)
			for i := range indexes {
				indexes[i] = 8 + int32(i%3) // mix standard, positive-payload, and negative-payload NaNs
			}
			checkBounds(t, indexes, 0, 0, true)
		}
	})

	t.Run("leading_nan_prefix_sweep", func(t *testing.T) {
		for _, n := range []int{1, 7, 8, 9, 10, 15, 16, 17, 25, 33} {
			for k := 0; k <= n; k++ {
				indexes := make([]int32, n)
				for i := 0; i < k; i++ {
					indexes[i] = 8 + int32(i%4) // mix qNaN payloads and sNaN
				}
				for i := k; i < n; i++ {
					indexes[i] = 0 // 0.0
				}
				if k == n {
					checkBounds(t, indexes, 0, 0, true)
					continue
				}
				rem := n - k
				wantMin, wantMax := 0.0, 0.0
				switch rem {
				case 1:
					indexes[k] = 2 // 1.0
					wantMin, wantMax = 1.0, 1.0
				case 2:
					indexes[k] = 3   // -50
					indexes[k+1] = 4 // 500
					wantMin, wantMax = -50.0, 500.0
				default:
					indexes[k] = 0
					indexes[k+1] = 4 // 500
					indexes[n-1] = 3 // -50
					wantMin, wantMax = -50.0, 500.0
				}
				checkBounds(t, indexes, wantMin, wantMax, false)
			}
		}
	})

	t.Run("all_8_lane_extrema_sweep", func(t *testing.T) {
		for _, n := range []int{8, 16} {
			for base := 0; base < n; base += 8 {
				for minLane := 0; minLane < 8; minLane++ {
					for maxLane := 0; maxLane < 8; maxLane++ {
						indexes := make([]int32, n)
						for i := range indexes {
							if i%2 == 0 {
								indexes[i] = 9 // qNaN
							} else {
								indexes[i] = 11 // sNaN
							}
						}
						if minLane == maxLane {
							indexes[base+minLane] = 3 // -50
							checkBounds(t, indexes, -50, -50, false)
							continue
						}
						indexes[base+minLane] = 3 // -50
						indexes[base+maxLane] = 4 // 500
						checkBounds(t, indexes, -50, 500, false)
					}
				}
			}
		}
	})

	t.Run("payload_nans_and_specials", func(t *testing.T) {
		cases := []struct {
			name    string
			indexes []int32
			wantMin float64
			wantMax float64
		}{
			{"qnan_payloads", []int32{9, 1, 10, 2, 8}, -1, 1},
			{"snan_mixed_short", []int32{11, 1, 11, 2, 11}, -1, 1},
			{"snan_mixed_long", []int32{11, 0, 11, 3, 11, 4, 11, 0, 11, 2}, -50, 500},
			{"infinities_and_nan", []int32{8, 5, 0, 10, 6, 9}, math.Inf(-1), math.Inf(1)},
			{"signed_zeros_and_nan", []int32{7, 8, 0, 9, 7, 10, 0, 7, 8}, 0, 0},
			{"unreferenced_nan_in_dict", []int32{0, 1, 2, 0, 1, 2, 0, 1, 2}, -1, 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				checkBounds(t, tc.indexes, tc.wantMin, tc.wantMax, false)
			})
		}

		t.Run("subnormals_with_nan", func(t *testing.T) {
			indexes := []int32{11, 12, 8, 13, 9, 12, 10, 13, 11}
			fMin, fMax := floatDict.Bounds(indexes)
			if fMin.Float() != subNeg32 || fMax.Float() != subPos32 {
				t.Errorf("float32 subnormals = (%g, %g), want (%g, %g)", fMin.Float(), fMax.Float(), subNeg32, subPos32)
			}
			dMin, dMax := doubleDict.Bounds(indexes)
			if dMin.Double() != subNeg64 || dMax.Double() != subPos64 {
				t.Errorf("float64 subnormals = (%g, %g), want (%g, %g)", dMin.Double(), dMax.Double(), subNeg64, subPos64)
			}
		})

		t.Run("empty_indexes", func(t *testing.T) {
			for _, indexes := range [][]int32{nil, {}} {
				fMin, fMax := floatDict.Bounds(indexes)
				if !fMin.IsNull() || !fMax.IsNull() {
					t.Errorf("float32 Bounds(%v) = (%v, %v), want null values", indexes, fMin, fMax)
				}
				dMin, dMax := doubleDict.Bounds(indexes)
				if !dMin.IsNull() || !dMax.IsNull() {
					t.Errorf("float64 Bounds(%v) = (%v, %v), want null values", indexes, dMin, dMax)
				}
			}
		})
	})

	t.Run("out_of_bounds_with_nan", func(t *testing.T) {
		oobLen := int32(len(floatDictVals))
		makePrefixOOB := func(n, k, oobPos int, oobVal int32) []int32 {
			indexes := make([]int32, n)
			for i := 0; i < k; i++ {
				indexes[i] = 8
			}
			indexes[oobPos] = oobVal
			return indexes
		}

		cases := []struct {
			name    string
			indexes []int32
		}{
			{"neg_before_nan_short", []int32{-1, 8, 0}},
			{"high_before_nan_short", []int32{oobLen, 8, 0}},
			{"neg_after_nan_short", []int32{8, 0, -1}},
			{"high_after_nan_short", []int32{8, 0, oobLen}},
			{"oob_same_batch_before_nan", []int32{0, 1, oobLen, 8, 0, 0, 0, 0, 0}},
			{"oob_same_batch_after_nan", []int32{0, 1, 8, -1, 0, 0, 0, 0, 0}},
			{"oob_after_nan_avx512", []int32{0, 1, 2, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, oobLen}},
			{"neg_after_nan_avx512", []int32{0, 1, 2, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, -1}},
			{"oob_in_all_nan_long", []int32{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, oobLen}},
			{"prefix_k1_oob_inside_neg", makePrefixOOB(20, 1, 0, -1)},
			{"prefix_k1_oob_inside_high", makePrefixOOB(20, 1, 0, oobLen)},
			{"prefix_k1_oob_after_neg", makePrefixOOB(20, 1, 10, -1)},
			{"prefix_k1_oob_after_high", makePrefixOOB(20, 1, 19, oobLen)},
			{"prefix_k8_oob_inside_neg", makePrefixOOB(20, 8, 5, -1)},
			{"prefix_k8_oob_inside_high", makePrefixOOB(20, 8, 7, oobLen)},
			{"prefix_k8_oob_after_neg", makePrefixOOB(20, 8, 12, -1)},
			{"prefix_k8_oob_after_high", makePrefixOOB(20, 8, 19, oobLen)},
			{"prefix_k9_oob_inside_neg", makePrefixOOB(20, 9, 8, -1)},
			{"prefix_k9_oob_inside_high", makePrefixOOB(20, 9, 4, oobLen)},
			{"prefix_k9_oob_after_neg", makePrefixOOB(20, 9, 15, -1)},
			{"prefix_k9_oob_after_high", makePrefixOOB(20, 9, 19, oobLen)},
		}

		expectPanic := func(t *testing.T, label string, fn func()) {
			t.Helper()
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%s: expected panic on out-of-bounds index", label)
				}
			}()
			fn()
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				expectPanic(t, fmt.Sprintf("float32/%s", tc.name), func() {
					floatDict.Bounds(tc.indexes)
				})
				expectPanic(t, fmt.Sprintf("float64/%s", tc.name), func() {
					doubleDict.Bounds(tc.indexes)
				})
			})
		}

		t.Run("empty_dict_non_empty_indexes", func(t *testing.T) {
			emptyFloatDict := newFloatDictionary(FloatType, 0, 0, encoding.FloatValues(nil))
			emptyDoubleDict := newDoubleDictionary(DoubleType, 0, 0, encoding.DoubleValues(nil))
			expectPanic(t, "float32/empty_dict", func() {
				emptyFloatDict.Bounds([]int32{0})
			})
			expectPanic(t, "float64/empty_dict", func() {
				emptyDoubleDict.Bounds([]int32{0})
			})
		})
	})
}
