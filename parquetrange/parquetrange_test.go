package parquetrange

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type contact struct {
	Name        string  `parquet:",plain"`
	PhoneNumber *string `parquet:",plain"`
}

func TestGenericRows(t *testing.T) {
	type testCase[T any] struct {
		name        string
		config      IterConfig
		shouldMatch bool
	}
	tests := []testCase[[]contact]{
		{"succeeds because it doesn't reuse rows", IterConfig{ReuseRows: false, ChunkSize: 1}, true},
		{"succeeds because ChunkSize is larger than input size", IterConfig{ReuseRows: true, ChunkSize: 10}, true},
		{"fails because johnPhoneNumber is overwritten with janePhoneNumber", IterConfig{ReuseRows: true, ChunkSize: 1}, false},
	}

	var johnPhoneNumber = "555-555-5555"
	var janePhoneNumber = "666-666-6666"

	contacts := []contact{
		{"John Doe", &johnPhoneNumber},
		{"Jane Doe", &janePhoneNumber},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testGenericReaderRows(t, contacts, tt.config, tt.shouldMatch)
		})
		t.Run(tt.name+"_flatten", func(t *testing.T) {
			testFlattenRows(t, contacts, tt.config, tt.shouldMatch)
		})
	}
}

func testGenericReaderRows[Row any](t *testing.T, rows []Row, config IterConfig, shouldMatch bool) {
	buffer := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buffer)
	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewGenericReader[Row](bytes.NewReader(buffer.Bytes()))

	result := make([]Row, 0, len(rows))
	for newRows, err := range GenericRows(reader, config) {
		if err != nil {
			t.Fatal(err)
		}

		result = append(result, newRows...)
	}

	if len(rows) != len(result) {
		t.Fatal(fmt.Errorf("incorrect number of values were read: want=%d got=%d", len(rows), len(result)))
	}

	matches := reflect.DeepEqual(rows, result)
	if !((matches && shouldMatch) || (!matches && !shouldMatch)) {
		t.Fatal(fmt.Errorf("rows mismatch:\nwant: %+v\ngot: %+v", rows, result))
	}
}

func testFlattenRows[Row any](t *testing.T, rows []Row, config IterConfig, shouldMatch bool) {
	buffer := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buffer)
	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewGenericReader[Row](bytes.NewReader(buffer.Bytes()))

	result := make([]Row, 0, len(rows))
	for newRow, err := range Flatten(GenericRows(reader, config)) {
		if err != nil {
			t.Fatal(err)
		}

		result = append(result, newRow)
	}

	if len(rows) != len(result) {
		t.Fatal(fmt.Errorf("incorrect number of values were read: want=%d got=%d", len(rows), len(result)))
	}

	matches := reflect.DeepEqual(rows, result)
	if !((matches && shouldMatch) || (!matches && !shouldMatch)) {
		t.Fatal(fmt.Errorf("rows mismatch:\nwant: %+v\ngot: %+v", rows, result))
	}
}
