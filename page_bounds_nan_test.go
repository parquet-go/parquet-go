package parquet

import (
	"bytes"
	"math"
	"testing"

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
				columnIndex: ^int16(0),
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
				columnIndex: ^int16(0),
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
