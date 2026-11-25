package parquet_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestConvertKeepsNulls(t *testing.T) {
	int32Node := parquet.Leaf(parquet.Int32Type)
	int64Node := parquet.Leaf(parquet.Int64Type)

	tests := []struct {
		scenario string
		from     parquet.Node
		to       parquet.Node
		in       parquet.Row
		want     parquet.Row
	}{
		{
			scenario: "int32 to int64 null",
			from:     parquet.Optional(int32Node),
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "int32 to int64 zero stays zero",
			from:     parquet.Optional(int32Node),
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.Int32Value(0).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(0).Level(0, 1, 0)},
		},
		{
			scenario: "int32 to int64 value",
			from:     parquet.Optional(int32Node),
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.Int32Value(5).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(5).Level(0, 1, 0)},
		},
		{
			scenario: "int64 to int32 null",
			from:     parquet.Optional(int64Node),
			to:       parquet.Optional(int32Node),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "float to double null",
			from:     parquet.Optional(parquet.Leaf(parquet.FloatType)),
			to:       parquet.Optional(parquet.Leaf(parquet.DoubleType)),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "float to double zero",
			from:     parquet.Optional(parquet.Leaf(parquet.FloatType)),
			to:       parquet.Optional(parquet.Leaf(parquet.DoubleType)),
			in:       parquet.Row{parquet.FloatValue(0).Level(0, 1, 0)},
			want:     parquet.Row{parquet.DoubleValue(0).Level(0, 1, 0)},
		},
		{
			scenario: "float to double value",
			from:     parquet.Optional(parquet.Leaf(parquet.FloatType)),
			to:       parquet.Optional(parquet.Leaf(parquet.DoubleType)),
			in:       parquet.Row{parquet.FloatValue(1.5).Level(0, 1, 0)},
			want:     parquet.Row{parquet.DoubleValue(1.5).Level(0, 1, 0)},
		},
		{
			scenario: "double to float null",
			from:     parquet.Optional(parquet.Leaf(parquet.DoubleType)),
			to:       parquet.Optional(parquet.Leaf(parquet.FloatType)),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "bool to int32 null",
			from:     parquet.Optional(parquet.Leaf(parquet.BooleanType)),
			to:       parquet.Optional(int32Node),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "bool false to int32 zero",
			from:     parquet.Optional(parquet.Leaf(parquet.BooleanType)),
			to:       parquet.Optional(int32Node),
			in:       parquet.Row{parquet.BooleanValue(false).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int32Value(0).Level(0, 1, 0)},
		},
		{
			scenario: "decimal int32 to int64 null",
			from:     parquet.Optional(parquet.Decimal(2, 9, parquet.Int32Type)),
			to:       parquet.Optional(parquet.Decimal(2, 18, parquet.Int64Type)),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "decimal int32 to int64 value",
			from:     parquet.Optional(parquet.Decimal(2, 9, parquet.Int32Type)),
			to:       parquet.Optional(parquet.Decimal(2, 18, parquet.Int64Type)),
			in:       parquet.Row{parquet.Int32Value(100).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(100).Level(0, 1, 0)},
		},
		{
			scenario: "decimal fixed to int64 null",
			from:     parquet.Optional(parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16))),
			to:       parquet.Optional(parquet.Decimal(0, 18, parquet.Int64Type)),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "string to int64 null",
			from:     parquet.Optional(parquet.String()),
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "string to int64 value",
			from:     parquet.Optional(parquet.String()),
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.ByteArrayValue([]byte("42")).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(42).Level(0, 1, 0)},
		},
		{
			scenario: "timestamp millis to micros null",
			from:     parquet.Optional(parquet.Timestamp(parquet.Millisecond)),
			to:       parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.NullValue().Level(0, 0, 0)},
		},
		{
			scenario: "timestamp millis to micros value",
			from:     parquet.Optional(parquet.Timestamp(parquet.Millisecond)),
			to:       parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
			in:       parquet.Row{parquet.Int64Value(1000).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(1_000_000).Level(0, 1, 0)},
		},
		{
			scenario: "repeated int32 to int64 nulls and zero",
			from:     parquet.Repeated(int32Node),
			to:       parquet.Repeated(int64Node),
			in: parquet.Row{
				parquet.NullValue().Level(0, 0, 0),
				parquet.Int32Value(5).Level(1, 1, 0),
				parquet.Int32Value(0).Level(1, 1, 0),
				parquet.NullValue().Level(1, 0, 0),
			},
			want: parquet.Row{
				parquet.NullValue().Level(0, 0, 0),
				parquet.Int64Value(5).Level(1, 1, 0),
				parquet.Int64Value(0).Level(1, 1, 0),
				parquet.NullValue().Level(1, 0, 0),
			},
		},
		{
			scenario: "optional int32 null to required int64",
			from:     parquet.Optional(int32Node),
			to:       int64Node,
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.Int64Value(0).Level(0, 0, 0)},
		},
		{
			scenario: "optional int32 value to required int64",
			from:     parquet.Optional(int32Node),
			to:       int64Node,
			in:       parquet.Row{parquet.Int32Value(5).Level(0, 1, 0)},
			want:     parquet.Row{parquet.Int64Value(5).Level(0, 0, 0)},
		},
		{
			scenario: "required int32 null to optional int64",
			from:     int32Node,
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.NullValue().Level(0, 0, 0)},
			want:     parquet.Row{parquet.Int64Value(0).Level(0, 1, 0)},
		},
		{
			scenario: "required int32 value to optional int64",
			from:     int32Node,
			to:       parquet.Optional(int64Node),
			in:       parquet.Row{parquet.Int32Value(5).Level(0, 0, 0)},
			want:     parquet.Row{parquet.Int64Value(5).Level(0, 1, 0)},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			got := convertColumn(t, test.from, test.to, test.in)
			if !rowsDeepEqual(got, []parquet.Row{test.want}) {
				t.Fatalf("converted row:\nwant %v\ngot  %v", formatRow(test.want), formatRow(got[0]))
			}
		})
	}
}

func TestConvertNullDoesNotHideInvalidValue(t *testing.T) {
	conv, err := parquet.Convert(
		parquet.NewSchema("s", parquet.Group{"a": parquet.Repeated(parquet.Leaf(parquet.Int64Type))}),
		parquet.NewSchema("s", parquet.Group{"a": parquet.Repeated(parquet.String())}),
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{{
		parquet.NullValue().Level(0, 0, 0),
		parquet.ByteArrayValue([]byte("nope")).Level(1, 1, 0),
	}}
	_, err = conv.Convert(rows)
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("error = %v, want the invalid value %q", err, "nope")
	}
}

func TestConvertKeepsNullsNestedOptional(t *testing.T) {
	from := parquet.NewSchema("s", parquet.Group{
		"g": parquet.Optional(parquet.Group{
			"a": parquet.Leaf(parquet.Int32Type),
		}),
	})
	to := parquet.NewSchema("s", parquet.Group{
		"g": parquet.Optional(parquet.Group{
			"a": parquet.Leaf(parquet.Int64Type),
		}),
	})
	conv, err := parquet.Convert(to, from)
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.Int32Value(7).Level(0, 1, 0)},
	}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	want := []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.Int64Value(7).Level(0, 1, 0)},
	}
	if !rowsDeepEqual(rows, want) {
		t.Fatalf("converted rows:\nwant %v\ngot  %v", want, rows)
	}
}

func TestConvertKeepsNullsTwoColumns(t *testing.T) {
	from := parquet.NewSchema("s", parquet.Group{
		"a": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
		"b": parquet.Optional(parquet.Leaf(parquet.FloatType)),
	})
	to := parquet.NewSchema("s", parquet.Group{
		"a": parquet.Optional(parquet.Leaf(parquet.Int64Type)),
		"b": parquet.Optional(parquet.Leaf(parquet.DoubleType)),
	})
	conv, err := parquet.Convert(to, from)
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{{
		parquet.NullValue().Level(0, 0, 0),
		parquet.FloatValue(1.5).Level(0, 1, 1),
	}}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	want := parquet.Row{
		parquet.NullValue().Level(0, 0, 0),
		parquet.DoubleValue(1.5).Level(0, 1, 1),
	}
	if !rowsDeepEqual(rows, []parquet.Row{want}) {
		t.Fatalf("converted row:\nwant %v\ngot  %v", formatRow(want), formatRow(rows[0]))
	}
}

func TestConvertKeepsNullsRowBuffer(t *testing.T) {
	from := parquet.NewSchema("s", parquet.Group{
		"a": parquet.Optional(parquet.Leaf(parquet.Int32Type)),
	})
	to := parquet.NewSchema("s", parquet.Group{
		"a": parquet.Optional(parquet.Leaf(parquet.Int64Type)),
	})
	conv, err := parquet.Convert(to, from)
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.Int32Value(0).Level(0, 1, 0)},
		{parquet.Int32Value(5).Level(0, 1, 0)},
	}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	buf := parquet.NewRowBuffer[any](to)
	if _, err := buf.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	page, err := buf.ColumnChunks()[0].Pages().ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	min, max, ok := page.Bounds()
	if page.NumNulls() != 1 || !ok || min.Int64() != 0 || max.Int64() != 5 || min.IsNull() || max.IsNull() {
		t.Fatalf("NumNulls = %d, bounds %v..%v ok=%v, want 1 null and 0..5", page.NumNulls(), min, max, ok)
	}
}

func TestConvertReadRowsKeepsNulls(t *testing.T) {
	type src struct {
		A *int32 `parquet:"a,optional"`
	}
	type dst struct {
		A *int64 `parquet:"a,optional"`
	}
	v := int32(5)
	var buf bytes.Buffer
	if err := parquet.Write(&buf, []src{{}, {A: &v}, {}}); err != nil {
		t.Fatal(err)
	}
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()), parquet.SchemaOf(new(dst)))
	defer r.Close()
	got := make([]parquet.Row, 3)
	n, err := r.ReadRows(got)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	got = got[:n]
	want := []parquet.Row{
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.Int64Value(5).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
	}
	if !rowsDeepEqual(got, want) {
		t.Fatalf("ReadRows:\nwant %v\ngot  %v", want, got)
	}
}

func convertColumn(t *testing.T, from, to parquet.Node, row parquet.Row) []parquet.Row {
	t.Helper()
	conv, err := parquet.Convert(
		parquet.NewSchema("s", parquet.Group{"a": to}),
		parquet.NewSchema("s", parquet.Group{"a": from}),
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := []parquet.Row{row}
	if _, err := conv.Convert(rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func rowsDeepEqual(got, want []parquet.Row) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			return false
		}
		for j := range want[i] {
			if !parquet.DeepEqual(want[i][j], got[i][j]) {
				return false
			}
		}
	}
	return true
}

func formatRow(row parquet.Row) string {
	var buf bytes.Buffer
	for i, v := range row {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(v.String())
		if v.IsNull() {
			buf.WriteString("(null)")
		}
	}
	return buf.String()
}
