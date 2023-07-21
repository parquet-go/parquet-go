package parquet_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestSortingWriter(t *testing.T) {
	type Row struct {
		Value int32 `parquet:"value"`
	}

	rows := make([]Row, 1000)
	for i := range rows {
		rows[i].Value = int32(i)
	}

	prng := rand.New(rand.NewSource(0))
	prng.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewSortingWriter[Row](buffer, 99,
		parquet.SortingWriterConfig(
			parquet.SortingColumns(
				parquet.Ascending("value"),
			),
		),
	)

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	read, err := parquet.Read[Row](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Value < rows[j].Value
	})

	assertRowsEqual(t, rows, read)
}

func TestSortingWriterDropDuplicatedRows(t *testing.T) {
	type Row struct {
		Value int32 `parquet:"value"`
	}

	rows := make([]Row, 1000)
	for i := range rows {
		rows[i].Value = int32(i / 2)
	}

	prng := rand.New(rand.NewSource(0))
	prng.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	buffer := bytes.NewBuffer(nil)
	writer := parquet.NewSortingWriter[Row](buffer, 99,
		parquet.SortingWriterConfig(
			parquet.SortingBuffers(
				parquet.NewFileBufferPool("", "buffers.*"),
			),
			parquet.SortingColumns(
				parquet.Ascending("value"),
			),
			parquet.DropDuplicatedRows(true),
		),
	)

	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	read, err := parquet.Read[Row](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Value < rows[j].Value
	})

	n := len(rows) / 2
	for i := range rows[:n] {
		rows[i] = rows[2*i]
	}

	assertRowsEqual(t, rows[:n], read)
}

func TestSortingWriterCorruptedString(t *testing.T) {
	type Row struct {
		Tag string `parquet:"tag"`
	}
	rowsWant := make([]Row, 107) // passes at 106, but fails at 107+
	for i := range rowsWant {
		rowsWant[i].Tag = randString(100)
	}

	buffer := bytes.NewBuffer(nil)

	writer := parquet.NewSortingWriter[Row](buffer, 2000,
		&parquet.WriterConfig{
			PageBufferSize: 2560,
			Sorting: parquet.SortingConfig{
				SortingColumns: []parquet.SortingColumn{
					parquet.Ascending("tag"),
				},
			},
		})

	_, err := writer.Write(rowsWant)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	rowsGot, err := parquet.Read[Row](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(rowsWant, func(i, j int) bool {
		return rowsWant[i].Tag < rowsWant[j].Tag
	})

	assertRowsEqualByRow(t, rowsGot, rowsWant)
}

func TestSortingWriterCorruptedFixedLenByteArray(t *testing.T) {
	type Row struct {
		ID [16]byte `parquet:"id,uuid"`
	}
	rowsWant := make([]Row, 700) // passes at 300, fails at 400+.
	for i := range rowsWant {
		rowsWant[i].ID = rand16bytes()
	}

	buffer := bytes.NewBuffer(nil)

	writer := parquet.NewSortingWriter[Row](buffer, 2000,
		&parquet.WriterConfig{
			PageBufferSize: 2560,
			Sorting: parquet.SortingConfig{
				SortingColumns: []parquet.SortingColumn{
					parquet.Ascending("id"),
				},
			},
		})

	_, err := writer.Write(rowsWant)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	rowsGot, err := parquet.Read[Row](bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(rowsWant, func(i, j int) bool {
		return idLess(rowsWant[i].ID, rowsWant[j].ID)
	})

	assertRowsEqualByRow(t, rowsGot, rowsWant)
}

const letterRunes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterRunes[rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(letterRunes))]
	}
	return string(b)
}

func rand16bytes() [16]byte {
	var b [16]byte
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return b
}

func idLess(ID1, ID2 [16]byte) bool {
	k1 := binary.BigEndian.Uint64(ID1[:8])
	k2 := binary.BigEndian.Uint64(ID2[:8])
	switch {
	case k1 < k2:
		return true
	case k1 > k2:
		return false
	}
	k1 = binary.BigEndian.Uint64(ID1[8:])
	k2 = binary.BigEndian.Uint64(ID2[8:])
	return k1 < k2
}

func assertRowsEqualByRow[T any](t *testing.T, rowsGot, rowsWant []T) {
	if len(rowsGot) != len(rowsWant) {
		t.Errorf("want rows length %d but got rows length %d", len(rowsWant), len(rowsGot))
	}
	count := 0
	for i := range rowsGot {
		if !reflect.DeepEqual(rowsGot[i], rowsWant[i]) {
			t.Error("rows mismatch at index", i, ":")
			t.Logf(" want: %#v\n", rowsWant[i])
			t.Logf("  got: %#v\n", rowsGot[i])

			// check if rowsGot[i] is even present in rowsWant
			found := false
			for j := range rowsWant {
				if reflect.DeepEqual(rowsWant[j], rowsGot[i]) {
					t.Log("  we found the row at index", j, "in want.")
					found = true
					break
				}
			}
			if !found {
				t.Log("  got row index", i, "isn't found in want rows, and is therefore corrupted data.")
			}
			count++
		}
	}
	if count > 0 {
		t.Error(count, "rows mismatched out of", len(rowsWant), "total")
	}
}
