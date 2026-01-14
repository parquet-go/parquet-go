package parquet_test

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

func TestLogicalTypesEqual(t *testing.T) {
	tests := []struct {
		a, b parquet.Node
	}{
		{parquet.Leaf(parquet.Int32Type), parquet.Int(32)},
		{parquet.Leaf(parquet.Int64Type), parquet.Int(64)},
	}

	for _, test := range tests {
		eq := parquet.EqualNodes(test.a, test.b)
		if !eq {
			t.Errorf("expected %v to be equal to %v", test.a, test.b)
		}
	}
}

func TestEqualTypes(t *testing.T) {
	tests := []struct {
		name     string
		type1    parquet.Type
		type2    parquet.Type
		expected bool
	}{
		// Basic physical types - should be equal
		{
			name:     "same boolean types",
			type1:    parquet.BooleanType,
			type2:    parquet.BooleanType,
			expected: true,
		},
		{
			name:     "same int32 types",
			type1:    parquet.Int32Type,
			type2:    parquet.Int32Type,
			expected: true,
		},
		{
			name:     "same int64 types",
			type1:    parquet.Int64Type,
			type2:    parquet.Int64Type,
			expected: true,
		},
		{
			name:     "same float types",
			type1:    parquet.FloatType,
			type2:    parquet.FloatType,
			expected: true,
		},
		{
			name:     "same double types",
			type1:    parquet.DoubleType,
			type2:    parquet.DoubleType,
			expected: true,
		},
		{
			name:     "same byte array types",
			type1:    parquet.ByteArrayType,
			type2:    parquet.ByteArrayType,
			expected: true,
		},
		{
			name:     "same int96 types",
			type1:    parquet.Int96Type,
			type2:    parquet.Int96Type,
			expected: true,
		},

		// Different physical types - should not be equal
		{
			name:     "different kinds - int32 vs int64",
			type1:    parquet.Int32Type,
			type2:    parquet.Int64Type,
			expected: false,
		},
		{
			name:     "different kinds - boolean vs int32",
			type1:    parquet.BooleanType,
			type2:    parquet.Int32Type,
			expected: false,
		},
		{
			name:     "different kinds - float vs double",
			type1:    parquet.FloatType,
			type2:    parquet.DoubleType,
			expected: false,
		},
		{
			name:     "different kinds - byte array vs fixed len byte array",
			type1:    parquet.ByteArrayType,
			type2:    parquet.FixedLenByteArrayType(10),
			expected: false,
		},

		// Fixed length byte arrays with different lengths
		{
			name:     "same fixed len byte array types",
			type1:    parquet.FixedLenByteArrayType(16),
			type2:    parquet.FixedLenByteArrayType(16),
			expected: true,
		},
		{
			name:     "different fixed len byte array lengths",
			type1:    parquet.FixedLenByteArrayType(16),
			type2:    parquet.FixedLenByteArrayType(32),
			expected: false,
		},

		// Logical types - same underlying physical type
		{
			name:     "same string logical types",
			type1:    parquet.String().Type(),
			type2:    parquet.String().Type(),
			expected: true,
		},
		{
			name:     "same int32 logical types",
			type1:    parquet.Int(32).Type(),
			type2:    parquet.Int(32).Type(),
			expected: true,
		},
		{
			name:     "same int64 logical types",
			type1:    parquet.Int(64).Type(),
			type2:    parquet.Int(64).Type(),
			expected: true,
		},
		{
			name:     "same uint32 logical types",
			type1:    parquet.Uint(32).Type(),
			type2:    parquet.Uint(32).Type(),
			expected: true,
		},
		{
			name:     "same uint64 logical types",
			type1:    parquet.Uint(64).Type(),
			type2:    parquet.Uint(64).Type(),
			expected: true,
		},
		{
			name:     "same date logical types",
			type1:    parquet.Date().Type(),
			type2:    parquet.Date().Type(),
			expected: true,
		},
		{
			name:     "same json logical types",
			type1:    parquet.JSON().Type(),
			type2:    parquet.JSON().Type(),
			expected: true,
		},
		{
			name:     "same bson logical types",
			type1:    parquet.BSON().Type(),
			type2:    parquet.BSON().Type(),
			expected: true,
		},

		// Different logical types with same physical type
		{
			name:     "string vs json (both byte array)",
			type1:    parquet.String().Type(),
			type2:    parquet.JSON().Type(),
			expected: false,
		},
		{
			name:     "string vs bson (both byte array)",
			type1:    parquet.String().Type(),
			type2:    parquet.BSON().Type(),
			expected: false,
		},
		{
			name:     "json vs bson (both byte array)",
			type1:    parquet.JSON().Type(),
			type2:    parquet.BSON().Type(),
			expected: false,
		},
		{
			name:     "int32 vs uint32 (same physical kind)",
			type1:    parquet.Int(32).Type(),
			type2:    parquet.Uint(32).Type(),
			expected: false,
		},
		{
			name:     "int64 vs uint64 (same physical kind)",
			type1:    parquet.Int(64).Type(),
			type2:    parquet.Uint(64).Type(),
			expected: false,
		},

		// Different bit widths for same logical type
		{
			name:     "int32 vs int64 logical types",
			type1:    parquet.Int(32).Type(),
			type2:    parquet.Int(64).Type(),
			expected: false,
		},
		{
			name:     "uint32 vs uint64 logical types",
			type1:    parquet.Uint(32).Type(),
			type2:    parquet.Uint(64).Type(),
			expected: false,
		},

		// Timestamp logical types with different units
		{
			name:     "same timestamp millis",
			type1:    parquet.Timestamp(parquet.Millisecond).Type(),
			type2:    parquet.Timestamp(parquet.Millisecond).Type(),
			expected: true,
		},
		{
			name:     "same timestamp micros",
			type1:    parquet.Timestamp(parquet.Microsecond).Type(),
			type2:    parquet.Timestamp(parquet.Microsecond).Type(),
			expected: true,
		},
		{
			name:     "same timestamp nanos",
			type1:    parquet.Timestamp(parquet.Nanosecond).Type(),
			type2:    parquet.Timestamp(parquet.Nanosecond).Type(),
			expected: true,
		},
		{
			name:     "different timestamp units - millis vs micros",
			type1:    parquet.Timestamp(parquet.Millisecond).Type(),
			type2:    parquet.Timestamp(parquet.Microsecond).Type(),
			expected: false,
		},
		{
			name:     "different timestamp units - micros vs nanos",
			type1:    parquet.Timestamp(parquet.Microsecond).Type(),
			type2:    parquet.Timestamp(parquet.Nanosecond).Type(),
			expected: false,
		},

		// Time logical types with different units
		{
			name:     "same time millis",
			type1:    parquet.Time(parquet.Millisecond).Type(),
			type2:    parquet.Time(parquet.Millisecond).Type(),
			expected: true,
		},
		{
			name:     "same time micros",
			type1:    parquet.Time(parquet.Microsecond).Type(),
			type2:    parquet.Time(parquet.Microsecond).Type(),
			expected: true,
		},
		{
			name:     "same time nanos",
			type1:    parquet.Time(parquet.Nanosecond).Type(),
			type2:    parquet.Time(parquet.Nanosecond).Type(),
			expected: true,
		},
		{
			name:     "different time units - millis vs micros",
			type1:    parquet.Time(parquet.Millisecond).Type(),
			type2:    parquet.Time(parquet.Microsecond).Type(),
			expected: false,
		},

		// Logical type vs physical type
		{
			name:     "string logical vs byte array physical",
			type1:    parquet.String().Type(),
			type2:    parquet.ByteArrayType,
			expected: false,
		},
		{
			name:     "int32 logical vs int32 with same logical type",
			type1:    parquet.Int(32).Type(),
			type2:    parquet.Int32Type,
			expected: true, // Both have the same Integer logical type
		},
		{
			name:     "date logical vs int32 physical",
			type1:    parquet.Date().Type(),
			type2:    parquet.Int32Type,
			expected: false,
		},

		// Decimal logical types with different precision/scale
		{
			name:     "same decimal(10,2)",
			type1:    parquet.Decimal(10, 2, parquet.Int32Type).Type(),
			type2:    parquet.Decimal(10, 2, parquet.Int32Type).Type(),
			expected: true,
		},
		{
			name:     "different decimal precision",
			type1:    parquet.Decimal(10, 2, parquet.Int32Type).Type(),
			type2:    parquet.Decimal(12, 2, parquet.Int32Type).Type(),
			expected: false,
		},
		{
			name:     "different decimal scale",
			type1:    parquet.Decimal(10, 2, parquet.Int32Type).Type(),
			type2:    parquet.Decimal(10, 3, parquet.Int32Type).Type(),
			expected: false,
		},
		{
			name:     "same decimal different physical type",
			type1:    parquet.Decimal(10, 2, parquet.Int32Type).Type(),
			type2:    parquet.Decimal(10, 2, parquet.Int64Type).Type(),
			expected: false,
		},
		// Geometry logical types
		{
			name:     "geometry types with same CRS",
			type1:    parquet.Geometry(format.GeometryDefaultCRS).Type(),
			type2:    parquet.Geometry(format.GeometryDefaultCRS).Type(),
			expected: true,
		},
		{
			name:     "geometry types with different CRS",
			type1:    parquet.Geometry(format.GeometryDefaultCRS).Type(),
			type2:    parquet.Geometry("OGC:CRS83").Type(),
			expected: false,
		},
		{
			name:     "geometry with empty CRS vs default CRS",
			type1:    parquet.Geometry("").Type(),
			type2:    parquet.Geometry(format.GeometryDefaultCRS).Type(),
			expected: true,
		},
		// Geography logical types
		{
			name:     "geography types with same CRS and algorithm",
			type1:    parquet.Geography(format.GeographyDefaultCRS, 0).Type(),
			type2:    parquet.Geography(format.GeographyDefaultCRS, 0).Type(),
			expected: true,
		},
		{
			name:     "geography types with different CRS",
			type1:    parquet.Geography(format.GeographyDefaultCRS, format.Karney).Type(),
			type2:    parquet.Geography("OGC:CRS83", format.Karney).Type(),
			expected: false,
		},
		{
			name:     "geography types with same CRS but different algorithm",
			type1:    parquet.Geography(format.GeographyDefaultCRS, format.Karney).Type(),
			type2:    parquet.Geography(format.GeographyDefaultCRS, format.Andoyer).Type(),
			expected: false,
		},
		{
			name:     "geography with empty CRS vs default CRS",
			type1:    parquet.Geography("", format.Spherical).Type(),
			type2:    parquet.Geography(format.GeographyDefaultCRS, format.Spherical).Type(),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parquet.EqualTypes(test.type1, test.type2)
			if result != test.expected {
				t.Errorf("EqualTypes(%v, %v) = %v, expected %v",
					test.type1, test.type2, result, test.expected)

				// Additional debugging info
				t.Logf("Type1: Kind=%v, Length=%v, LogicalType=%v",
					test.type1.Kind(), test.type1.Length(), test.type1.LogicalType())
				t.Logf("Type2: Kind=%v, Length=%v, LogicalType=%v",
					test.type2.Kind(), test.type2.Length(), test.type2.LogicalType())
			}
		})
	}
}

func TestOptionalTimeZeroValue(t *testing.T) {
	type Record struct {
		ID   int       `parquet:"id"`
		Time time.Time `parquet:"time,optional,timestamp(microsecond)"`
	}

	// Create records with zero and non-zero time.Time values
	records := []Record{
		{ID: 1, Time: time.Time{}},                                 // zero value - should be NULL
		{ID: 2, Time: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)}, // non-zero value
		{ID: 3, Time: time.Time{}},                                 // another zero value
	}

	// Write to parquet
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)

	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write records: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back using row API to check definition levels
	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	rows := make([]parquet.Row, len(records))
	n, err := reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read rows: %v", err)
	}

	if n != len(records) {
		t.Fatalf("expected %d rows, got %d", len(records), n)
	}

	// Check definition levels
	// For record 1 (zero time): should have definitionLevel=0 (NULL)
	// For record 2 (non-zero time): should have definitionLevel=1 (non-NULL)
	// For record 3 (zero time): should have definitionLevel=0 (NULL)

	expectedDefinitionLevels := []int{0, 1, 0} // ID column has 0 (required), Time column varies

	for i, row := range rows[:n] {
		if len(row) < 2 {
			t.Fatalf("row %d has less than 2 columns", i)
		}

		timeValue := row[1] // Second column is Time
		definitionLevel := int(timeValue.DefinitionLevel())

		if definitionLevel != expectedDefinitionLevels[i] {
			t.Errorf("row %d: expected definitionLevel=%d for Time column, got %d",
				i, expectedDefinitionLevels[i], definitionLevel)
		}

		// For NULL values, IsNull() should return true
		if expectedDefinitionLevels[i] == 0 && !timeValue.IsNull() {
			t.Errorf("row %d: expected IsNull()=true for zero time.Time, got false", i)
		}

		// For non-NULL values, check the actual value
		if expectedDefinitionLevels[i] == 1 {
			if timeValue.IsNull() {
				t.Errorf("row %d: expected IsNull()=false for non-zero time.Time, got true", i)
			}
		}
	}

	// Also read back using GenericReader to verify the values
	genReader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer genReader.Close()

	readRecords := make([]Record, len(records))
	n2, err := genReader.Read(readRecords)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read with GenericReader: %v", err)
	}

	readRecords = readRecords[:n2]

	// Verify that zero time.Time values are preserved as zero
	if !readRecords[0].Time.IsZero() {
		t.Errorf("record 0: expected zero time.Time, got %v", readRecords[0].Time)
	}

	if readRecords[1].Time.IsZero() {
		t.Errorf("record 1: expected non-zero time.Time, got zero")
	}

	if !readRecords[2].Time.IsZero() {
		t.Errorf("record 2: expected zero time.Time, got %v", readRecords[2].Time)
	}

	// Verify IDs are correct
	for i := range readRecords {
		if readRecords[i].ID != records[i].ID {
			t.Errorf("record %d: expected ID=%d, got %d", i, records[i].ID, readRecords[i].ID)
		}
	}
}

func TestOptionalTimeWithMillisecond(t *testing.T) {
	type Record struct {
		Time time.Time `parquet:"time,optional,timestamp(millisecond)"`
	}

	records := []Record{
		{Time: time.Time{}},
		{Time: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, 2)
	n, _ := reader.Read(readRecords)
	readRecords = readRecords[:n]

	if !readRecords[0].Time.IsZero() {
		t.Errorf("expected zero time, got %v", readRecords[0].Time)
	}

	if readRecords[1].Time.IsZero() {
		t.Errorf("expected non-zero time, got zero")
	}
}

func TestOptionalTimeWithNanosecond(t *testing.T) {
	type Record struct {
		Time time.Time `parquet:"time,optional,timestamp(nanosecond)"`
	}

	records := []Record{
		{Time: time.Time{}},
		{Time: time.Date(2024, 12, 25, 0, 0, 0, 123456789, time.UTC)},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, 2)
	n, _ := reader.Read(readRecords)
	readRecords = readRecords[:n]

	if !readRecords[0].Time.IsZero() {
		t.Errorf("expected zero time, got %v", readRecords[0].Time)
	}

	if readRecords[1].Time.IsZero() {
		t.Errorf("expected non-zero time, got zero")
	}
}

// TestIssue155 verifies the fix for https://github.com/parquet-go/parquet-go/issues/155
// The issue reported that empty time.Time{} values were being serialized as "1754-08-30"
// instead of being preserved as zero values (NULL) when using optional timestamp fields.
func TestIssue155(t *testing.T) {
	type TestStruct struct {
		TestDate time.Time `parquet:"test_date,optional,timestamp"`
		TestInt  int       `parquet:"test_int"`
	}

	// Create a record with an empty time.Time (zero value)
	original := TestStruct{
		TestDate: time.Time{}, // empty/zero time - should be preserved
		TestInt:  123,
	}

	// Write to parquet
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[TestStruct](buf)
	if _, err := writer.Write([]TestStruct{original}); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Read back
	reader := parquet.NewGenericReader[TestStruct](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	result := make([]TestStruct, 1)
	n, err := reader.Read(result)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 record, got %d", n)
	}

	// Verify the TestInt field is preserved
	if result[0].TestInt != 123 {
		t.Errorf("expected TestInt=123, got %d", result[0].TestInt)
	}

	// The critical assertion from issue #155:
	// An empty time.Time should remain zero after round-trip
	if !result[0].TestDate.IsZero() {
		t.Errorf("expected TestDate.IsZero()=true, got false with value: %v (was: %s)",
			result[0].TestDate, result[0].TestDate.Format("2006-01-02"))
	}

	// Also verify it's not the erroneous "1754-08-30" date that was reported
	if result[0].TestDate.Year() == 1754 {
		t.Errorf("got the erroneous 1754-08-30 date that was reported in issue #155")
	}
}

// TestIssue326 reproduces https://github.com/parquet-go/parquet-go/issues/326
// Issue #326: using *time.Time (pointer) with timestamp(millisecond) tag causes a panic.
func TestIssue326(t *testing.T) {
	type Person struct {
		ID        int32      `parquet:"id"`
		Name      string     `parquet:"name"`
		CreatedAt *time.Time `parquet:"created_at,timestamp(millisecond)"`
	}

	// This should not panic - pointer types should support timestamp tags
	schema := parquet.SchemaOf(Person{})
	if schema == nil {
		t.Fatal("schema should not be nil")
	}
	t.Logf("Schema: %s", schema)

	// Test writing and reading
	buffer := new(bytes.Buffer)
	now := time.Now()

	writer := parquet.NewGenericWriter[Person](buffer, schema)
	_, err := writer.Write([]Person{
		{ID: 1, Name: "Alice", CreatedAt: &now},
		{ID: 2, Name: "Bob", CreatedAt: nil}, // NULL value
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Buffer size: %d bytes", buffer.Len())

	// Read back
	reader := parquet.NewGenericReader[Person](bytes.NewReader(buffer.Bytes()))
	rows := make([]Person, 2)
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	t.Logf("Row 0: ID=%d, Name=%s, CreatedAt=%v", rows[0].ID, rows[0].Name, rows[0].CreatedAt)
	t.Logf("Row 1: ID=%d, Name=%s, CreatedAt=%v", rows[1].ID, rows[1].Name, rows[1].CreatedAt)

	// Verify first row has timestamp
	if rows[0].CreatedAt == nil {
		t.Error("expected first row to have non-nil CreatedAt")
	} else if !rows[0].CreatedAt.Equal(now.Truncate(time.Millisecond)) {
		t.Errorf("timestamp mismatch: got %v, want %v", rows[0].CreatedAt, now.Truncate(time.Millisecond))
	}

	// Verify second row has NULL
	if rows[1].CreatedAt != nil {
		t.Errorf("expected second row to have nil CreatedAt, got %v", rows[1].CreatedAt)
	}
}

func TestGeometry(t *testing.T) {
	type Record struct {
		Geometry  geom.T `parquet:"geometry,optional,geometry(OGC:CRS84)"`
		Geography geom.T `parquet:"geography,optional,geography(OGC:CRS84:Vincenty)"`
		WKBBytes  []byte `parquet:"wkb_bytes,optional,geometry"`
	}

	geomBytes, err := wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{0, 0}), wkb.NDR)
	if err != nil {
		t.Fatalf("failed to marshal geometry: %v", err)
	}

	records := []Record{
		{
			Geometry:  geom.NewPointFlat(geom.XY, []float64{0, 0}),
			Geography: geom.NewPointFlat(geom.XY, []float64{0, 0}),
			WKBBytes:  geomBytes,
		},
		{
			Geometry:  nil,
			Geography: nil,
			WKBBytes:  nil,
		}, // NULL geometries
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Record](buf)
	if _, err := writer.Write(records); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	reader := parquet.NewGenericReader[Record](bytes.NewReader(buf.Bytes()))
	defer reader.Close()

	readRecords := make([]Record, 2)
	n, _ := reader.Read(readRecords)
	readRecords = readRecords[:n]

	r := readRecords[0]
	if !reflect.DeepEqual(r.Geometry, records[0].Geometry) {
		t.Errorf("geometry mismatch: got %v, want %v", r.Geometry, records[0].Geometry)
	}
	if !reflect.DeepEqual(r.Geography, records[0].Geography) {
		t.Errorf("geography mismatch: got %v, want %v", r.Geography, records[0].Geography)
	}
	if !bytes.Equal(r.WKBBytes, records[0].WKBBytes) {
		t.Errorf("WKB bytes mismatch: got %v, want %v", r.WKBBytes, records[0].WKBBytes)
	}

	r = readRecords[1]
	if r.Geometry != nil {
		t.Errorf("expected nil geometry, got %v", r.Geometry)
	}
	if r.Geography != nil {
		t.Errorf("expected nil geography, got %v", r.Geography)
	}
	if r.WKBBytes != nil {
		t.Errorf("expected nil WKBBytes, got %v", r.WKBBytes)
	}
}

func TestShreddedVariant(t *testing.T) {
	// These are just error cases. Non-error cases are covered in TestSchemaRoundTrip.
	errTestCases := []parquet.Node{
		parquet.Variant(),
		parquet.Map(parquet.String(), parquet.Leaf(parquet.ByteArrayType)),
		parquet.Decimal(0, 39, parquet.FixedLenByteArrayType(16)),
		parquet.Uint(8),
		parquet.Uint(16),
		parquet.Uint(32),
		parquet.Uint(64),
		parquet.Repeated(parquet.Leaf(parquet.Int32Type)),
		parquet.Repeated(parquet.String()),
		parquet.Enum(),
		parquet.Geometry(format.GeometryDefaultCRS),
		parquet.Geography(format.GeographyDefaultCRS, format.Spherical),
		// Also test some logical types that we don't yet support or provide API to construct
		parquet.Leaf(logicalType{Type: parquet.ByteArrayType, lt: format.LogicalType{Unknown: &format.NullType{}}}),
		parquet.Leaf(logicalType{Type: parquet.ByteArrayType, lt: format.LogicalType{Float16: &format.Float16Type{}}}),
	}
	for _, testCase := range errTestCases {
		// Direct
		_, err := parquet.ShreddedVariant(testCase)
		if err == nil {
			t.Errorf("ShreddedVariant(%v) should have returned an error", testCase)
		} else if !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("error message should contain 'not allowed': %q", err.Error())
		}
		// Indirect (group that contains the offending type)
		group := parquet.Group{"a": parquet.String(), "b": testCase}
		_, err = parquet.ShreddedVariant(group)
		if err == nil {
			t.Errorf("ShreddedVariant(%v) should have returned an error", group)
		} else if !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("error message should contain 'not allowed': %q", err.Error())
		}
	}
}

type logicalType struct {
	parquet.Type
	lt format.LogicalType
}

func (l logicalType) LogicalType() *format.LogicalType {
	return &l.lt
}
