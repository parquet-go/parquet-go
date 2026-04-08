package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
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

func TestGeospatialStatisticsEmptyGeometry(t *testing.T) {
	// Empty geometries (WKB with 0 coordinates) should appear in GeoSpatialTypes
	// but must not affect the bounding box.
	// Use the []byte column path to write raw WKB, since go-geom cannot marshal
	// empty Points to WKB directly.
	type Record struct {
		Geom []byte `parquet:"geom,geometry(OGC:CRS84)"`
	}

	// WKB empty LineString (little-endian): type 2, numPoints 0
	emptyLineStringWKB := []byte{
		0x01,                   // byte order: little-endian
		0x02, 0x00, 0x00, 0x00, // type: LineString (2)
		0x00, 0x00, 0x00, 0x00, // numPoints: 0
	}
	nonEmptyWKB, err := wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{5, 10}), wkb.NDR)
	if err != nil {
		t.Fatal(err)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err = w.Write([]Record{
		{Geom: emptyLineStringWKB},
		{Geom: nonEmptyWKB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	// Both type codes must appear in GeoSpatialTypes: 1 (Point) and 2 (LineString).
	if len(stats.GeoSpatialTypes) != 2 || stats.GeoSpatialTypes[0] != 1 || stats.GeoSpatialTypes[1] != 2 {
		t.Errorf("GeoSpatialTypes: got %v, want [1, 2]", stats.GeoSpatialTypes)
	}
	// BBox comes only from the non-empty Point.
	bbox := stats.BBox
	if bbox.XMin != 5 || bbox.XMax != 5 || bbox.YMin != 10 || bbox.YMax != 10 {
		t.Errorf("BBox: got %+v, want {5 5 10 10}", bbox)
	}
}

func TestGeospatialStatisticsAllEmptyGeometry(t *testing.T) {
	// A column of only empty geometries should emit GeoSpatialTypes but no BBox.
	type Record struct {
		Geom []byte `parquet:"geom,geometry(OGC:CRS84)"`
	}

	// WKB empty LineString (little-endian)
	emptyLineStringWKB := []byte{
		0x01,
		0x02, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err := w.Write([]Record{
		{Geom: emptyLineStringWKB},
		{Geom: emptyLineStringWKB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	// LineString (type 2) must appear in GeoSpatialTypes.
	if len(stats.GeoSpatialTypes) != 1 || stats.GeoSpatialTypes[0] != 2 {
		t.Errorf("GeoSpatialTypes: got %v, want [2]", stats.GeoSpatialTypes)
	}
	// BBox should be zero (not populated) since no geometry has coordinates.
	if stats.BBox.XMin != 0 || stats.BBox.XMax != 0 {
		t.Errorf("expected zero BBox for all-empty geometry column, got %+v", stats.BBox)
	}
}

func TestGeospatialStatisticsInvalidWKBSuppressesStats(t *testing.T) {
	// A non-null value that fails WKB parsing should suppress GeospatialStatistics
	// for the whole chunk rather than publishing partial metadata.
	type Record struct {
		Geom []byte `parquet:"geom,geometry(OGC:CRS84)"`
	}

	validWKB, err := wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{1, 2}), wkb.NDR)
	if err != nil {
		t.Fatal(err)
	}

	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[Record](buf)
	_, err = w.Write([]Record{
		{Geom: validWKB},
		{Geom: []byte("not valid wkb")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	stats := geospatialStats(t, buf, 0, 0)
	// Stats must be suppressed — zero value means no stats were emitted.
	if len(stats.GeoSpatialTypes) != 0 || stats.BBox.XMin != 0 {
		t.Errorf("expected suppressed stats after WKB parse error, got %+v", stats)
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
