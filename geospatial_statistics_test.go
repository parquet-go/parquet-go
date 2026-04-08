package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	geom "github.com/twpayne/go-geom"
)

// geospatialStats reads back the GeospatialStatistics for the given column
// index from a written parquet buffer.
func geospatialStats(t *testing.T, buf *bytes.Buffer, rowGroupIdx, colIdx int) format.GeospatialStatistics {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	rg := f.Metadata().RowGroups
	if rowGroupIdx >= len(rg) {
		t.Fatalf("row group %d not found (total %d)", rowGroupIdx, len(rg))
	}
	cols := rg[rowGroupIdx].Columns
	if colIdx >= len(cols) {
		t.Fatalf("column %d not found (total %d)", colIdx, len(cols))
	}
	return cols[colIdx].MetaData.GeospatialStatistics
}

func TestGeospatialStatistics2DPoint(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XY, []float64{10, 20})},
		{Geom: geom.NewPointFlat(geom.XY, []float64{30, 40})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.XMin != 10 || bbox.XMax != 30 {
		t.Errorf("X range: got [%v, %v], want [10, 30]", bbox.XMin, bbox.XMax)
	}
	if bbox.YMin != 20 || bbox.YMax != 40 {
		t.Errorf("Y range: got [%v, %v], want [20, 40]", bbox.YMin, bbox.YMax)
	}
	if bbox.ZMin.Valid || bbox.ZMax.Valid {
		t.Error("unexpected Z values for 2D geometry")
	}
	if bbox.MMin.Valid || bbox.MMax.Valid {
		t.Error("unexpected M values for 2D geometry")
	}
	if len(stats.GeoSpatialTypes) != 1 || stats.GeoSpatialTypes[0] != 1 {
		t.Errorf("GeoSpatialTypes: got %v, want [1]", stats.GeoSpatialTypes)
	}
}

func TestGeospatialStatisticsMixedTypes(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XY, []float64{1, 2})},
		{Geom: geom.NewLineStringFlat(geom.XY, []float64{3, 4, 5, 6})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.XMin != 1 || bbox.XMax != 5 {
		t.Errorf("X range: got [%v, %v], want [1, 5]", bbox.XMin, bbox.XMax)
	}
	if bbox.YMin != 2 || bbox.YMax != 6 {
		t.Errorf("Y range: got [%v, %v], want [2, 6]", bbox.YMin, bbox.YMax)
	}
	// GeoSpatialTypes should contain both 1 (Point) and 2 (LineString), sorted
	if len(stats.GeoSpatialTypes) != 2 || stats.GeoSpatialTypes[0] != 1 || stats.GeoSpatialTypes[1] != 2 {
		t.Errorf("GeoSpatialTypes: got %v, want [1, 2]", stats.GeoSpatialTypes)
	}
}

func TestGeospatialStatisticsNullsSkipped(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,optional,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XY, []float64{5, 10})},
		{Geom: nil},
		{Geom: geom.NewPointFlat(geom.XY, []float64{15, 20})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.XMin != 5 || bbox.XMax != 15 {
		t.Errorf("X range: got [%v, %v], want [5, 15]", bbox.XMin, bbox.XMax)
	}
	if bbox.YMin != 10 || bbox.YMax != 20 {
		t.Errorf("Y range: got [%v, %v], want [10, 20]", bbox.YMin, bbox.YMax)
	}
}

func TestGeospatialStatisticsAllNull(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,optional,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{{Geom: nil}, {Geom: nil}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	// No BBox should be set for an all-null column — both coordinates default to zero.
	if stats.BBox.XMin != 0 && stats.BBox.XMax != 0 {
		t.Errorf("expected zero BBox for all-null column, got %+v", stats.BBox)
	}
	if len(stats.GeoSpatialTypes) != 0 {
		t.Errorf("expected no GeoSpatialTypes for all-null column, got %v", stats.GeoSpatialTypes)
	}
}

func TestGeospatialStatistics3D(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XYZ, []float64{1, 2, 10})},
		{Geom: geom.NewPointFlat(geom.XYZ, []float64{3, 4, 20})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.XMin != 1 || bbox.XMax != 3 {
		t.Errorf("X range: got [%v, %v], want [1, 3]", bbox.XMin, bbox.XMax)
	}
	if bbox.YMin != 2 || bbox.YMax != 4 {
		t.Errorf("Y range: got [%v, %v], want [2, 4]", bbox.YMin, bbox.YMax)
	}
	if !bbox.ZMin.Valid || !bbox.ZMax.Valid {
		t.Error("expected Z values for 3D geometry")
	} else {
		if bbox.ZMin.V != 10 || bbox.ZMax.V != 20 {
			t.Errorf("Z range: got [%v, %v], want [10, 20]", bbox.ZMin.V, bbox.ZMax.V)
		}
	}
	if bbox.MMin.Valid || bbox.MMax.Valid {
		t.Error("unexpected M values for XYZ geometry")
	}
	// WKB type 1001 = Point + 1000 (Z)
	if len(stats.GeoSpatialTypes) != 1 || stats.GeoSpatialTypes[0] != 1001 {
		t.Errorf("GeoSpatialTypes: got %v, want [1001]", stats.GeoSpatialTypes)
	}
}

func TestGeospatialStatisticsMeasured(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XYM, []float64{1, 2, 100})},
		{Geom: geom.NewPointFlat(geom.XYM, []float64{3, 4, 200})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.ZMin.Valid || bbox.ZMax.Valid {
		t.Error("unexpected Z values for XYM geometry")
	}
	if !bbox.MMin.Valid || !bbox.MMax.Valid {
		t.Error("expected M values for XYM geometry")
	} else {
		if bbox.MMin.V != 100 || bbox.MMax.V != 200 {
			t.Errorf("M range: got [%v, %v], want [100, 200]", bbox.MMin.V, bbox.MMax.V)
		}
	}
	// WKB type 2001 = Point + 2000 (M)
	if len(stats.GeoSpatialTypes) != 1 || stats.GeoSpatialTypes[0] != 2001 {
		t.Errorf("GeoSpatialTypes: got %v, want [2001]", stats.GeoSpatialTypes)
	}
}

func TestGeospatialStatisticsMultipleRowGroups(t *testing.T) {
	type Record struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf, parquet.MaxRowsPerRowGroup(1))
	_, err := w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XY, []float64{1, 2})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Write([]Record{
		{Geom: geom.NewPointFlat(geom.XY, []float64{10, 20})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Row group 0: only the first point
	stats0 := geospatialStats(t, buf, 0, 0)
	if stats0.BBox.XMin != 1 || stats0.BBox.XMax != 1 {
		t.Errorf("RG0 X range: got [%v, %v], want [1, 1]", stats0.BBox.XMin, stats0.BBox.XMax)
	}

	// Row group 1: only the second point
	stats1 := geospatialStats(t, buf, 1, 0)
	if stats1.BBox.XMin != 10 || stats1.BBox.XMax != 10 {
		t.Errorf("RG1 X range: got [%v, %v], want [10, 10]", stats1.BBox.XMin, stats1.BBox.XMax)
	}
}

func TestGeospatialStatisticsGeography(t *testing.T) {
	type Record struct {
		Geog geom.T `parquet:"geog,geography(OGC:CRS84:Spherical)"`
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geog: geom.NewPointFlat(geom.XY, []float64{-10, 5})},
		{Geog: geom.NewPointFlat(geom.XY, []float64{20, 50})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	bbox := stats.BBox
	if bbox.XMin != -10 || bbox.XMax != 20 {
		t.Errorf("X range: got [%v, %v], want [-10, 20]", bbox.XMin, bbox.XMax)
	}
	if bbox.YMin != 5 || bbox.YMax != 50 {
		t.Errorf("Y range: got [%v, %v], want [5, 50]", bbox.YMin, bbox.YMax)
	}
}
