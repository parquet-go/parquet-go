package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

// putBigEndianInt64 writes n as a big-endian two's-complement integer,
// sign-extending into dst when dst is wider than 8 bytes.
func putBigEndianInt64(dst []byte, n int64) {
	if len(dst) < 8 {
		panic("dst shorter than 8 bytes")
	}
	pad := byte(0)
	if n < 0 {
		pad = 0xFF
	}
	for i := range len(dst) - 8 {
		dst[i] = pad
	}
	binary.BigEndian.PutUint64(dst[len(dst)-8:], uint64(n))
}

func TestFixedLenDecimalReadIntoInt64(t *testing.T) {
	type encodedAmount struct {
		Amount [8]byte `parquet:"amount,decimal(2:18)"`
	}
	type integerAmount struct {
		Amount int64 `parquet:"amount,decimal(2:18)"`
	}
	tests := []struct {
		name  string
		cents int64
	}{
		{name: "positive", cents: 100000},
		{name: "negative", cents: -100000},
		{name: "zero", cents: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], uint64(tt.cents))

			var file bytes.Buffer
			writer := NewGenericWriter[encodedAmount](&file)
			if _, err := writer.Write([]encodedAmount{{Amount: encoded}}); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			byteReader := NewReader(bytes.NewReader(file.Bytes()))
			var raw encodedAmount
			if err := byteReader.Read(&raw); err != nil {
				t.Fatal(err)
			}
			if err := byteReader.Close(); err != nil {
				t.Fatal(err)
			}
			if raw.Amount != encoded {
				t.Fatalf("stored bytes = %x, want %x", raw.Amount, encoded)
			}

			integerReader := NewReader(bytes.NewReader(file.Bytes()))
			var got integerAmount
			if err := integerReader.Read(&got); err != nil {
				t.Fatal(err)
			}
			if err := integerReader.Close(); err != nil {
				t.Fatal(err)
			}
			if got.Amount != tt.cents {
				t.Fatalf("unscaled amount = %d, want %d (stored bytes %x)", got.Amount, tt.cents, encoded)
			}
		})
	}

	// Precision above 18 is stored as FIXED_LEN_BYTE_ARRAY(16). Values that
	// still fit in int64 are sign-extended across the extra bytes.
	t.Run("16-byte", func(t *testing.T) {
		type encodedAmount struct {
			Amount [16]byte `parquet:"amount,decimal(0:38)"`
		}
		type integerAmount struct {
			Amount int64 `parquet:"amount,decimal(0:18)"`
		}
		tests := []struct {
			name  string
			cents int64
		}{
			{name: "positive", cents: 100000},
			{name: "negative", cents: -100000},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var encoded encodedAmount
				putBigEndianInt64(encoded.Amount[:], tt.cents)

				var file bytes.Buffer
				writer := NewGenericWriter[encodedAmount](&file)
				if _, err := writer.Write([]encodedAmount{encoded}); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}

				integerReader := NewReader(bytes.NewReader(file.Bytes()))
				var got integerAmount
				if err := integerReader.Read(&got); err != nil {
					t.Fatal(err)
				}
				if err := integerReader.Close(); err != nil {
					t.Fatal(err)
				}
				if got.Amount != tt.cents {
					t.Fatalf("unscaled amount = %d, want %d (stored bytes %x)", got.Amount, tt.cents, encoded.Amount)
				}
			})
		}
	})

	// A one-byte FIXED_LEN_BYTE_ARRAY is sign-extended: 0x80 is -128 and 0x7f is 127.
	t.Run("1-byte", func(t *testing.T) {
		type encodedAmount struct {
			Amount [1]byte `parquet:"amount,decimal(0:2)"`
		}
		type int64Amount struct {
			Amount int64 `parquet:"amount,decimal(0:18)"`
		}
		type int32Amount struct {
			Amount int32 `parquet:"amount,decimal(0:9)"`
		}
		tests := []struct {
			name string
			raw  byte
			want int64
		}{
			{name: "negative", raw: 0x80, want: -128},
			{name: "positive", raw: 0x7f, want: 127},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var file bytes.Buffer
				writer := NewGenericWriter[encodedAmount](&file)
				if _, err := writer.Write([]encodedAmount{{Amount: [1]byte{tt.raw}}}); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}

				int64Reader := NewReader(bytes.NewReader(file.Bytes()))
				var got64 int64Amount
				if err := int64Reader.Read(&got64); err != nil {
					t.Fatal(err)
				}
				if err := int64Reader.Close(); err != nil {
					t.Fatal(err)
				}
				if got64.Amount != tt.want {
					t.Fatalf("int64 amount = %d, want %d", got64.Amount, tt.want)
				}

				int32Reader := NewReader(bytes.NewReader(file.Bytes()))
				var got32 int32Amount
				if err := int32Reader.Read(&got32); err != nil {
					t.Fatal(err)
				}
				if err := int32Reader.Close(); err != nil {
					t.Fatal(err)
				}
				if int64(got32.Amount) != tt.want {
					t.Fatalf("int32 amount = %d, want %d", got32.Amount, tt.want)
				}
			})
		}
	})

	t.Run("int32", func(t *testing.T) {
		type encodedAmount struct {
			Amount [8]byte `parquet:"amount,decimal(0:18)"`
		}
		type int32Amount struct {
			Amount int32 `parquet:"amount,decimal(0:9)"`
		}
		write := func(t *testing.T, n int64) *bytes.Reader {
			t.Helper()
			var encoded encodedAmount
			putBigEndianInt64(encoded.Amount[:], n)
			var file bytes.Buffer
			writer := NewGenericWriter[encodedAmount](&file)
			if _, err := writer.Write([]encodedAmount{encoded}); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			return bytes.NewReader(file.Bytes())
		}
		for _, n := range []int64{100000, -100000} {
			t.Run(fmt.Sprintf("fits/%d", n), func(t *testing.T) {
				reader := NewReader(write(t, n))
				defer reader.Close()
				var got int32Amount
				if err := reader.Read(&got); err != nil {
					t.Fatal(err)
				}
				if int64(got.Amount) != n {
					t.Fatalf("amount = %d, want %d", got.Amount, n)
				}
			})
		}
		for _, n := range []int64{int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1} {
			t.Run(fmt.Sprintf("overflow/%d", n), func(t *testing.T) {
				reader := NewReader(write(t, n))
				defer reader.Close()
				var got int32Amount
				err := reader.Read(&got)
				if err == nil {
					t.Fatalf("read amount %d, want an error that the value does not fit in int32", got.Amount)
				}
				if !strings.Contains(err.Error(), "does not fit in int32") {
					t.Fatalf("read error = %v, want an int32 overflow", err)
				}
			})
		}
	})

	t.Run("null", func(t *testing.T) {
		type encodedAmount struct {
			Amount *[16]byte `parquet:"amount,decimal(0:38)"`
		}
		type integerAmount struct {
			Amount *int64 `parquet:"amount,decimal(0:18)"`
		}
		const present int64 = -100000
		var raw [16]byte
		putBigEndianInt64(raw[:], present)

		var file bytes.Buffer
		writer := NewGenericWriter[encodedAmount](&file)
		if _, err := writer.Write([]encodedAmount{{}, {Amount: &raw}}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		reader := NewReader(bytes.NewReader(file.Bytes()))
		defer reader.Close()

		var got integerAmount
		if err := reader.Read(&got); err != nil {
			t.Fatal(err)
		}
		if got.Amount != nil {
			t.Fatalf("amount = %d, want null", *got.Amount)
		}
		if err := reader.Read(&got); err != nil {
			t.Fatal(err)
		}
		if got.Amount == nil || *got.Amount != present {
			t.Fatalf("amount = %v, want %d", got.Amount, present)
		}
	})
}

// TestFixedLenDecimalWriteInt64 checks that an int64 written into a
// FIXED_LEN_BYTE_ARRAY(16) decimal is big-endian two's complement. A negative
// value has to be sign-extended: the bytes above the low 8 are 0xFF.
func TestFixedLenDecimalWriteInt64(t *testing.T) {
	schema := NewSchema("row", Group{
		"amount": Decimal(0, 38, FixedLenByteArrayType(16)),
	})
	type intAmount struct {
		Amount int64 `parquet:"amount"`
	}
	type rawAmount struct {
		Amount [16]byte `parquet:"amount,decimal(0:38)"`
	}
	tests := []struct {
		name  string
		cents int64
	}{
		{name: "positive", cents: 100000},
		{name: "negative", cents: -100000},
		{name: "min", cents: math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var file bytes.Buffer
			writer := NewGenericWriter[intAmount](&file, schema)
			if _, err := writer.Write([]intAmount{{Amount: tt.cents}}); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			reader := NewReader(bytes.NewReader(file.Bytes()))
			defer reader.Close()
			var got rawAmount
			if err := reader.Read(&got); err != nil {
				t.Fatal(err)
			}
			var want [16]byte
			putBigEndianInt64(want[:], tt.cents)
			if got.Amount != want {
				t.Fatalf("stored bytes = %x, want %x", got.Amount, want)
			}
		})
	}
}

func TestInt64FromDecimalBytes(t *testing.T) {
	cases := []struct {
		raw  []byte
		want int64
	}{
		{[]byte{0x80}, -128},
		{[]byte{0x7f}, 127},
		{[]byte{0x01, 0x00}, 256},
		{[]byte{0xff}, -1},
	}
	for _, tt := range cases {
		got, err := int64FromDecimalBytes(tt.raw)
		if err != nil {
			t.Fatalf("%x: %v", tt.raw, err)
		}
		if got != tt.want {
			t.Fatalf("%x = %d, want %d", tt.raw, got, tt.want)
		}
	}
	_, err := int64FromDecimalBytes(bytes.Repeat([]byte{0x01}, 9))
	if err == nil {
		t.Fatal("expected a value wider than int64 to be rejected")
	}
}

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
