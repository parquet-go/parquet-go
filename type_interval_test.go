package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

func TestIntervalNodeType(t *testing.T) {
	node := parquet.IntervalNode()
	typ := node.Type()

	if got, want := typ.Kind(), parquet.FixedLenByteArray; got != want {
		t.Errorf("Kind: got %v, want %v", got, want)
	}
	if got, want := typ.Length(), 12; got != want {
		t.Errorf("Length: got %v, want %v", got, want)
	}
	// INTERVAL has no formal LogicalType in the Parquet spec (field 9 is reserved);
	// it is expressed solely via ConvertedType = 21.
	if lt := typ.LogicalType(); lt != nil {
		t.Errorf("LogicalType: expected nil, got %v", lt)
	}
	ct := typ.ConvertedType()
	if ct == nil {
		t.Fatal("ConvertedType is nil")
	}
	if got, want := *ct, deprecated.Interval; got != want {
		t.Errorf("ConvertedType: got %v, want %v", got, want)
	}
}

func TestIntervalRoundTrip(t *testing.T) {
	type Row struct {
		V parquet.Interval `parquet:"v,interval"`
	}

	schema := parquet.SchemaOf(new(Row))

	rows := []Row{
		{V: parquet.Interval{Months: 0, Days: 0, Milliseconds: 0}},
		{V: parquet.Interval{Months: 1, Days: 2, Milliseconds: 3000}},
		{V: parquet.Interval{Months: 12, Days: 365, Milliseconds: 86400000}},
		{V: parquet.Interval{Months: ^uint32(0), Days: ^uint32(0), Milliseconds: ^uint32(0)}},
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := parquet.NewReader(bytes.NewReader(buf.Bytes()), schema)
	defer r.Close()

	got := make([]Row, 0, len(rows))
	for {
		var row Row
		err := r.Read(&row)
		if err != nil {
			break
		}
		got = append(got, row)
	}

	if len(got) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got), len(rows))
	}
	for i, want := range rows {
		if got[i] != want {
			t.Errorf("row[%d]: got %+v, want %+v", i, got[i], want)
		}
	}
}

func TestIntervalRoundTripByteArray(t *testing.T) {
	type Row struct {
		V [12]byte `parquet:"v,interval"`
	}

	schema := parquet.SchemaOf(new(Row))

	rows := []Row{
		{V: [12]byte{1, 0, 0, 0, 2, 0, 0, 0, 0xb8, 0x0b, 0, 0}}, // months=1, days=2, millis=3000
	}

	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, schema)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := parquet.NewReader(bytes.NewReader(buf.Bytes()), schema)
	defer r.Close()

	var row Row
	if err := r.Read(&row); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if row != rows[0] {
		t.Errorf("got %v, want %v", row, rows[0])
	}
}

func TestIntervalCompare(t *testing.T) {
	typ := parquet.IntervalNode().Type()

	// Same values
	a := parquet.FixedLenByteArrayValue([]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})
	b := parquet.FixedLenByteArrayValue([]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})
	if got := typ.Compare(a, b); got != 0 {
		t.Errorf("Compare equal: got %d, want 0", got)
	}

	// a < b
	c := parquet.FixedLenByteArrayValue([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if got := typ.Compare(a, c); got >= 0 {
		t.Errorf("Compare less: got %d, want < 0", got)
	}
}
