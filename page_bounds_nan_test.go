package parquet

import (
	"bytes"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

func newSliceBufferFloat32(data []float32) memory.SliceBuffer[float32] {
	return memory.SliceBufferFrom(data)
}

func newSliceBufferFloat64(data []float64) memory.SliceBuffer[float64] {
	return memory.SliceBufferFrom(data)
}

func TestBoundsFloat32ExcludeNaN(t *testing.T) {
	nan := float32(math.NaN())

	tests := []struct {
		name    string
		data    []float32
		wantMin float32
		wantMax float32
		wantOk  bool
	}{
		{
			name:   "empty",
			data:   []float32{},
			wantOk: false,
		},
		{
			name:   "all NaN",
			data:   []float32{nan, nan, nan},
			wantOk: false,
		},
		{
			name:    "no NaN",
			data:    []float32{3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN at start",
			data:    []float32{nan, 3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN at end",
			data:    []float32{3.0, 1.0, 2.0, nan},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN in middle",
			data:    []float32{3.0, nan, 1.0, nan, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "single non-NaN value",
			data:    []float32{nan, 5.0, nan},
			wantMin: 5.0,
			wantMax: 5.0,
			wantOk:  true,
		},
		{
			name:    "negative values with NaN",
			data:    []float32{nan, -3.0, -1.0, nan, -2.0},
			wantMin: -3.0,
			wantMax: -1.0,
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, ok := boundsFloat32ExcludeNaN(tt.data)
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
			if ok {
				if min != tt.wantMin {
					t.Errorf("min = %v, want %v", min, tt.wantMin)
				}
				if max != tt.wantMax {
					t.Errorf("max = %v, want %v", max, tt.wantMax)
				}
			}
		})
	}
}

func TestBoundsFloat64ExcludeNaN(t *testing.T) {
	nan := math.NaN()

	tests := []struct {
		name    string
		data    []float64
		wantMin float64
		wantMax float64
		wantOk  bool
	}{
		{
			name:   "empty",
			data:   []float64{},
			wantOk: false,
		},
		{
			name:   "all NaN",
			data:   []float64{nan, nan, nan},
			wantOk: false,
		},
		{
			name:    "no NaN",
			data:    []float64{3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN at start",
			data:    []float64{nan, 3.0, 1.0, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN at end",
			data:    []float64{3.0, 1.0, 2.0, nan},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "NaN in middle",
			data:    []float64{3.0, nan, 1.0, nan, 2.0},
			wantMin: 1.0,
			wantMax: 3.0,
			wantOk:  true,
		},
		{
			name:    "single non-NaN value",
			data:    []float64{nan, 5.0, nan},
			wantMin: 5.0,
			wantMax: 5.0,
			wantOk:  true,
		},
		{
			name:    "negative values with NaN",
			data:    []float64{nan, -3.0, -1.0, nan, -2.0},
			wantMin: -3.0,
			wantMax: -1.0,
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, ok := boundsFloat64ExcludeNaN(tt.data)
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
			if ok {
				if min != tt.wantMin {
					t.Errorf("min = %v, want %v", min, tt.wantMin)
				}
				if max != tt.wantMax {
					t.Errorf("max = %v, want %v", max, tt.wantMax)
				}
			}
		})
	}
}

func TestFloatPageBoundsExcludeNaN(t *testing.T) {
	nan32 := float32(math.NaN())

	t.Run("float32 page with NaN", func(t *testing.T) {
		page := &floatPage{
			typ:         floatType{},
			values:      newSliceBufferFloat32([]float32{nan32, 3.0, 1.0, nan32, 2.0}),
			columnIndex: ^int16(0),
		}

		min, max, ok := page.Bounds()
		if !ok {
			t.Fatal("expected bounds to be present")
		}
		if min.Float() != 1.0 {
			t.Errorf("min = %v, want 1.0", min.Float())
		}
		if max.Float() != 3.0 {
			t.Errorf("max = %v, want 3.0", max.Float())
		}
	})

	t.Run("float32 page all NaN", func(t *testing.T) {
		page := &floatPage{
			typ:         floatType{},
			values:      newSliceBufferFloat32([]float32{nan32, nan32}),
			columnIndex: ^int16(0),
		}

		_, _, ok := page.Bounds()
		if ok {
			t.Fatal("expected no bounds for all-NaN page")
		}
	})
}

func TestDoublePageBoundsExcludeNaN(t *testing.T) {
	nan64 := math.NaN()

	t.Run("float64 page with NaN", func(t *testing.T) {
		page := &doublePage{
			typ:         doubleType{},
			values:      newSliceBufferFloat64([]float64{nan64, 3.0, 1.0, nan64, 2.0}),
			columnIndex: ^int16(0),
		}

		min, max, ok := page.Bounds()
		if !ok {
			t.Fatal("expected bounds to be present")
		}
		if min.Double() != 1.0 {
			t.Errorf("min = %v, want 1.0", min.Double())
		}
		if max.Double() != 3.0 {
			t.Errorf("max = %v, want 3.0", max.Double())
		}
	})

	t.Run("float64 page all NaN", func(t *testing.T) {
		page := &doublePage{
			typ:         doubleType{},
			values:      newSliceBufferFloat64([]float64{nan64, nan64}),
			columnIndex: ^int16(0),
		}

		_, _, ok := page.Bounds()
		if ok {
			t.Fatal("expected no bounds for all-NaN page")
		}
	})
}

// TestWriterStatisticsExcludeNaN is an end-to-end test that writes a parquet file
// with float and double columns containing NaN values, then reads back the
// column chunk statistics to verify that NaN is excluded from min/max.
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

	for _, col := range rowGroup.Columns {
		name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
		stats := col.MetaData.Statistics

		switch name {
		case "float_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatalf("float_val: expected non-nil min/max statistics")
			}
			minVal := Float.Value(stats.MinValue)
			maxVal := Float.Value(stats.MaxValue)

			if math.IsNaN(float64(minVal.Float())) {
				t.Errorf("float_val min is NaN, expected 1.0")
			}
			if math.IsNaN(float64(maxVal.Float())) {
				t.Errorf("float_val max is NaN, expected 3.0")
			}
			if minVal.Float() != 1.0 {
				t.Errorf("float_val min = %v, want 1.0", minVal.Float())
			}
			if maxVal.Float() != 3.0 {
				t.Errorf("float_val max = %v, want 3.0", maxVal.Float())
			}

		case "double_val":
			if stats.MinValue == nil || stats.MaxValue == nil {
				t.Fatalf("double_val: expected non-nil min/max statistics")
			}
			minVal := Double.Value(stats.MinValue)
			maxVal := Double.Value(stats.MaxValue)

			if math.IsNaN(minVal.Double()) {
				t.Errorf("double_val min is NaN, expected 10.0")
			}
			if math.IsNaN(maxVal.Double()) {
				t.Errorf("double_val max is NaN, expected 30.0")
			}
			if minVal.Double() != 10.0 {
				t.Errorf("double_val min = %v, want 10.0", minVal.Double())
			}
			if maxVal.Double() != 30.0 {
				t.Errorf("double_val max = %v, want 30.0", maxVal.Double())
			}
		}
	}
}

// TestWriterStatisticsAllNaN verifies that when all float/double values are NaN,
// the statistics do not contain NaN min/max values.
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

	for _, col := range rowGroup.Columns {
		name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
		stats := col.MetaData.Statistics

		switch name {
		case "float_val", "double_val":
			// When all values are NaN, min/max should either be nil or not NaN.
			if stats.MinValue != nil {
				var minIsNaN bool
				if name == "float_val" {
					minIsNaN = math.IsNaN(float64(Float.Value(stats.MinValue).Float()))
				} else {
					minIsNaN = math.IsNaN(Double.Value(stats.MinValue).Double())
				}
				if minIsNaN {
					t.Errorf("%s: min should not be NaN when all values are NaN", name)
				}
			}
			if stats.MaxValue != nil {
				var maxIsNaN bool
				if name == "float_val" {
					maxIsNaN = math.IsNaN(float64(Float.Value(stats.MaxValue).Float()))
				} else {
					maxIsNaN = math.IsNaN(Double.Value(stats.MaxValue).Double())
				}
				if maxIsNaN {
					t.Errorf("%s: max should not be NaN when all values are NaN", name)
				}
			}
		}
	}
}
