package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func testRoundTrip[T comparable](t *testing.T, rows []T) {
	t.Helper()
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](buf)
	_, err := w.Write(rows)
	if err != nil {
		t.Fatal("write:", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("close:", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal("open:", err)
	}

	r := parquet.NewGenericReader[T](f)
	got := make([]T, len(rows))
	n, err := r.Read(got)
	if err != nil && err.Error() != "EOF" {
		t.Fatal("read:", err)
	}
	if n != len(rows) {
		t.Fatalf("read %d rows, want %d", n, len(rows))
	}

	for i := range rows {
		if got[i] != rows[i] {
			t.Errorf("row %d: got %v, want %v", i, got[i], rows[i])
		}
	}
}

func TestNativeIntWithSmallerBitWidth(t *testing.T) {
	t.Run("int with int(8)", func(t *testing.T) {
		type Row struct {
			Value int `parquet:"value,int(8)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: -1},
			{Value: 127},
			{Value: -128},
		})
	})

	t.Run("int with int(16)", func(t *testing.T) {
		type Row struct {
			Value int `parquet:"value,int(16)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: -1},
			{Value: 32767},
			{Value: -32768},
		})
	})

	t.Run("int with int(32)", func(t *testing.T) {
		type Row struct {
			Value int `parquet:"value,int(32)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: -1},
			{Value: 2147483647},
			{Value: -2147483648},
		})
	})

	t.Run("uint with uint(8)", func(t *testing.T) {
		type Row struct {
			Value uint `parquet:"value,uint(8)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 255},
		})
	})

	t.Run("uint with uint(16)", func(t *testing.T) {
		type Row struct {
			Value uint `parquet:"value,uint(16)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 65535},
		})
	})

	t.Run("uint with uint(32)", func(t *testing.T) {
		type Row struct {
			Value uint `parquet:"value,uint(32)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 4294967295},
		})
	})

	t.Run("int with int(64)", func(t *testing.T) {
		type Row struct {
			Value int `parquet:"value,int(64)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: -1},
			{Value: 9223372036854775807},
		})
	})

	t.Run("uint with uint(64)", func(t *testing.T) {
		type Row struct {
			Value uint `parquet:"value,uint(64)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 18446744073709551615},
		})
	})

	t.Run("int32 with uint(16) override", func(t *testing.T) {
		type Row struct {
			Value int32 `parquet:"value,uint(16)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 255},
		})
	})

	t.Run("uint64 with int(32) override", func(t *testing.T) {
		type Row struct {
			Value uint64 `parquet:"value,int(32)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 42},
		})
	})

	t.Run("int32 with int(64) widening", func(t *testing.T) {
		type Row struct {
			Value int32 `parquet:"value,int(64)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: -1},
			{Value: 2147483647},
			{Value: -2147483648},
		})
	})

	t.Run("uint32 with uint(64) widening", func(t *testing.T) {
		type Row struct {
			Value uint32 `parquet:"value,uint(64)"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 4294967295},
		})
	})

	t.Run("int with int(32) and delta encoding", func(t *testing.T) {
		type Row struct {
			Value int `parquet:"value,int(32),delta"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 100},
			{Value: 200},
		})
	})

	t.Run("uint with uint(32) and delta encoding", func(t *testing.T) {
		type Row struct {
			Value uint `parquet:"value,uint(32),delta"`
		}
		testRoundTrip(t, []Row{
			{Value: 0},
			{Value: 1},
			{Value: 100},
			{Value: 200},
		})
	})
}
