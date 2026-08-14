package parquet

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

func TestDecimalInt64PanicMessage(t *testing.T) {
	for _, precision := range []int{0, 19} {
		t.Run(fmt.Sprintf("precision=%d", precision), func(t *testing.T) {
			var p any
			func() {
				defer func() { p = recover() }()
				Decimal(0, precision, Int64Type)
			}()
			want := "DECIMAL annotated with Int64 must have precision >= 1 and <= 18, got " + fmt.Sprintf("%d", precision)
			if p == nil {
				t.Fatal("expected Decimal() to panic for out-of-range Int64 precision")
			}
			if got := fmt.Sprintf("%s", p); got != want {
				t.Errorf("wrong panic message:\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

func TestCompareDecimalByteArrays(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		// equal lengths
		{[]byte{0x00, 0x01}, []byte{0x00, 0x01}, 0},  // +1 == +1
		{[]byte{0xFF, 0xFF}, []byte{0x00, 0x01}, -1}, // -1 < +1
		{[]byte{0x00, 0x01}, []byte{0xFF, 0xFF}, +1}, // +1 > -1
		{[]byte{0xFF, 0xFF}, []byte{0xFF, 0x00}, +1}, // -1 > -256
		{[]byte{0x80, 0x00}, []byte{0xFF, 0xFF}, -1}, // -32768 < -1
		{[]byte{0x00, 0x01}, []byte{0x7F, 0xFF}, -1}, // +1 < +32767
		{[]byte{0x00, 0x00}, []byte{0xFF, 0xFF}, +1}, // 0 > -1
		{[]byte{0x00, 0x00}, []byte{0x00, 0x01}, -1}, // 0 < +1
		// different lengths (BYTE_ARRAY backed decimals)
		{[]byte{0x01}, []byte{0x00, 0x01}, 0},  // +1 == +1
		{[]byte{0xFF}, []byte{0xFF, 0xFF}, 0},  // -1 == -1
		{[]byte{0xFF}, []byte{0xFF, 0x00}, +1}, // -1 > -256
		{[]byte{0x01}, []byte{0x01, 0x00}, -1}, // +1 < +256
		{[]byte{0xFF}, []byte{0x01}, -1},       // -1 < +1
		{[]byte{0x00, 0x80}, []byte{0x7F}, +1}, // +128 > +127
		{[]byte{0xFF, 0x80}, []byte{0x80}, 0},  // -128 == -128
		{nil, nil, 0},                          // 0 == 0
		{nil, []byte{0x01}, -1},                // 0 < +1
		{nil, []byte{0xFF}, +1},                // 0 > -1
	}

	sign := func(k int) int {
		switch {
		case k < 0:
			return -1
		case k > 0:
			return +1
		default:
			return 0
		}
	}

	for _, tt := range tests {
		if got := sign(compareDecimalByteArrays(tt.a, tt.b)); got != tt.want {
			t.Errorf("compareDecimalByteArrays(%X, %X) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
		if got := sign(compareDecimalByteArrays(tt.b, tt.a)); got != -tt.want {
			t.Errorf("compareDecimalByteArrays(%X, %X) = %d, want %d", tt.b, tt.a, got, -tt.want)
		}
	}
}

// TestDecimalStatistics verifies that decimals backed by BYTE_ARRAY and
// FIXED_LEN_BYTE_ARRAY write column chunk statistics and column index bounds
// using the signed sort order defined by the parquet format, instead of the
// unsigned byte order of the physical type under which every negative value
// sorts above every positive value.
//
// https://github.com/parquet-go/parquet-go/issues/592
func TestDecimalStatistics(t *testing.T) {
	tests := []struct {
		name    string
		node    Node
		values  [][]byte
		wantMin []byte
		wantMax []byte
	}{
		{
			name: "fixed length byte array",
			node: Decimal(0, 4, FixedLenByteArrayType(2)),
			values: [][]byte{
				{0x00, 0x01}, // +1
				{0xFF, 0xFF}, // -1
				{0x01, 0x00}, // +256
				{0xFF, 0x00}, // -256
			},
			wantMin: []byte{0xFF, 0x00},
			wantMax: []byte{0x01, 0x00},
		},
		{
			name: "fixed length byte array dictionary",
			node: Encoded(Decimal(0, 4, FixedLenByteArrayType(2)), &RLEDictionary),
			values: [][]byte{
				{0x00, 0x01}, // +1
				{0xFF, 0xFF}, // -1
				{0x01, 0x00}, // +256
				{0xFF, 0x00}, // -256
			},
			wantMin: []byte{0xFF, 0x00},
			wantMax: []byte{0x01, 0x00},
		},
		{
			name: "fixed length byte array 16",
			node: Decimal(0, 38, FixedLenByteArrayType(16)),
			values: [][]byte{
				{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, // +1
				{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // -1
			},
			wantMin: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			wantMax: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
		},
		{
			name: "byte array",
			node: Decimal(0, 18, ByteArrayType),
			values: [][]byte{
				{0x01},       // +1
				{0xFF},       // -1
				{0x01, 0x00}, // +256
				{0xFF, 0x00}, // -256
			},
			wantMin: []byte{0xFF, 0x00},
			wantMax: []byte{0x01, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema("t", Group{"d": Optional(tt.node)})

			buf := new(bytes.Buffer)
			writer := NewGenericWriter[any](buf, schema)
			for _, v := range tt.values {
				if _, err := writer.Write([]any{map[string]any{"d": v}}); err != nil {
					t.Fatalf("failed to write: %v", err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			stats := f.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
			if !bytes.Equal(stats.MinValue, tt.wantMin) {
				t.Errorf("column chunk statistics min = %X, want %X", stats.MinValue, tt.wantMin)
			}
			if !bytes.Equal(stats.MaxValue, tt.wantMax) {
				t.Errorf("column chunk statistics max = %X, want %X", stats.MaxValue, tt.wantMax)
			}

			columnIndexes := f.ColumnIndexes()
			if len(columnIndexes) != 1 {
				t.Fatalf("expected 1 column index, got %d", len(columnIndexes))
			}
			if got := columnIndexes[0].MinValues[0]; !bytes.Equal(got, tt.wantMin) {
				t.Errorf("column index min = %X, want %X", got, tt.wantMin)
			}
			if got := columnIndexes[0].MaxValues[0]; !bytes.Equal(got, tt.wantMax) {
				t.Errorf("column index max = %X, want %X", got, tt.wantMax)
			}
		})
	}
}

// TestDecimalColumnIndexBoundaryOrder verifies that the boundary order of the
// column index of binary decimal columns is determined with the signed sort
// order: the pages below are descending under the signed order but would be
// (incorrectly) ascending under the unsigned byte order.
func TestDecimalColumnIndexBoundaryOrder(t *testing.T) {
	typ := Decimal(0, 4, FixedLenByteArrayType(2)).Type()
	indexer := typ.NewColumnIndexer(0)

	// page 1: [+1, +2], page 2: [-5, -3]
	indexer.IndexPage(2, 0,
		makeValueBytes(FixedLenByteArray, []byte{0x00, 0x01}),
		makeValueBytes(FixedLenByteArray, []byte{0x00, 0x02}),
	)
	indexer.IndexPage(2, 0,
		makeValueBytes(FixedLenByteArray, []byte{0xFF, 0xFB}),
		makeValueBytes(FixedLenByteArray, []byte{0xFF, 0xFD}),
	)

	columnIndex := indexer.ColumnIndex()
	if columnIndex.BoundaryOrder != format.Descending {
		t.Errorf("boundary order = %s, want %s", columnIndex.BoundaryOrder, format.Descending)
	}
}

// TestDecimalSortingOrder verifies that sorting rows by a binary decimal
// column applies the signed sort order.
func TestDecimalSortingOrder(t *testing.T) {
	schema := NewSchema("t", Group{"d": Decimal(0, 4, FixedLenByteArrayType(2))})
	buffer := NewGenericBuffer[any](schema,
		SortingRowGroupConfig(SortingColumns(Ascending("d"))),
	)

	values := [][]byte{
		{0x00, 0x01}, // +1
		{0xFF, 0x00}, // -256
		{0x01, 0x00}, // +256
		{0xFF, 0xFF}, // -1
	}
	for _, v := range values {
		if _, err := buffer.Write([]any{map[string]any{"d": v}}); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	sort.Sort(buffer)

	rows := make([]Row, len(values))
	reader := buffer.Rows()
	defer reader.Close()
	if n, err := reader.ReadRows(rows); n != len(values) {
		t.Fatalf("expected to read %d rows, got %d (%v)", len(values), n, err)
	}

	want := [][]byte{
		{0xFF, 0x00}, // -256
		{0xFF, 0xFF}, // -1
		{0x00, 0x01}, // +1
		{0x01, 0x00}, // +256
	}
	for i, row := range rows {
		if got := row[0].ByteArray(); !bytes.Equal(got, want[i]) {
			t.Errorf("row %d = %X, want %X", i, got, want[i])
		}
	}
}
