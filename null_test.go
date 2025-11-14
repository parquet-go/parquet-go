package parquet

import (
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/internal/quick"
)

func TestNullIndex(t *testing.T) {
	testNullIndex[bool](t)
	testNullIndex[int](t)
	testNullIndex[int32](t)
	testNullIndex[int64](t)
	testNullIndex[uint](t)
	testNullIndex[uint32](t)
	testNullIndex[uint64](t)
	testNullIndex[float32](t)
	testNullIndex[float64](t)
	testNullIndex[[10]byte](t)
	testNullIndex[[16]byte](t)
	testNullIndex[deprecated.Int96](t)
	testNullIndex[string](t)
	testNullIndex[*struct{}](t)
	testNullIndexTime(t)
}

func testNullIndex[T comparable](t *testing.T) {
	var zero T
	t.Helper()
	t.Run(reflect.TypeOf(zero).String(), func(t *testing.T) {
		err := quick.Check(func(data []T) bool {
			if len(data) == 0 {
				return true
			}

			want := make([]uint64, (len(data)+63)/64)
			got := make([]uint64, (len(data)+63)/64)

			for i := range data {
				if (i % 2) == 0 {
					data[i] = zero
				}
			}

			array := makeArrayFromSlice(data)
			nullIndex[T](want, array)
			nullIndexFuncOf(reflect.TypeOf(zero))(got, array)

			if !reflect.DeepEqual(want, got) {
				t.Errorf("unexpected null index\nwant = %064b\ngot  = %064b", want, got)
				return false
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})
}

func BenchmarkNullIndex(b *testing.B) {
	benchmarkNullIndex[bool](b)
	benchmarkNullIndex[int](b)
	benchmarkNullIndex[int32](b)
	benchmarkNullIndex[int64](b)
	benchmarkNullIndex[uint](b)
	benchmarkNullIndex[uint32](b)
	benchmarkNullIndex[uint64](b)
	benchmarkNullIndex[float32](b)
	benchmarkNullIndex[float64](b)
	benchmarkNullIndex[[10]byte](b)
	benchmarkNullIndex[[16]byte](b)
	benchmarkNullIndex[deprecated.Int96](b)
	benchmarkNullIndex[string](b)
	benchmarkNullIndex[[]struct{}](b)
	benchmarkNullIndex[*struct{}](b)
}

func benchmarkNullIndex[T any](b *testing.B) {
	const N = 1000

	var zero T
	typ := reflect.TypeOf(zero)
	null := nullIndexFuncOf(typ)
	data := makeArrayFromSlice(make([]T, N))
	bits := make([]uint64, (N+63)/64)

	b.Run(typ.String(), func(b *testing.B) {
		for b.Loop() {
			null(bits, data)
		}
		b.SetBytes(int64(typ.Size() * N))
	})
}

// testNullIndexTime is a special test for time.Time because it's not comparable with ==
// but has an IsZero() method to detect zero values
func testNullIndexTime(t *testing.T) {
	t.Helper()
	t.Run("time.Time", func(t *testing.T) {
		data := []time.Time{
			time.Time{}, // zero
			time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), // non-zero
			time.Time{}, // zero
			time.Date(2024, 12, 25, 12, 30, 0, 0, time.UTC), // non-zero
		}

		array := makeArrayFromSlice(data)
		bits := make([]uint64, 1)
		nullIndexFuncOf(reflect.TypeOf(time.Time{}))(bits, array)

		// bits should be 0b1010 = 10 (bit 1 and 3 are set for non-zero values)
		expected := uint64(0b1010)
		if bits[0] != expected {
			t.Errorf("unexpected null index\nwant = %064b\ngot  = %064b", expected, bits[0])
		}
	})
}
