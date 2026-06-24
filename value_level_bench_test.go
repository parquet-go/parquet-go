package parquet

import "testing"

func makeLevelValues() []Value {
	values := make([]Value, 1024)
	for i := range values {
		values[i] = ValueOf(int64(i)).Level(0, 0, 0)
	}
	return values
}

// BenchmarkValueLevel measures the cost of mutating the levels of a Value in a
// hot loop, as reported in https://github.com/parquet-go/parquet-go/issues/545.
func BenchmarkValueLevel(b *testing.B) {
	values := makeLevelValues()
	b.ResetTimer()
	for b.Loop() {
		for i := range values {
			v := values[i]
			values[i] = v.Level(v.RepetitionLevel(), 1, v.Column())
		}
	}
}

// BenchmarkValueSetDefinitionLevel measures the single-field in-place setter.
// It inlines and (because the argument is in range) the bounds check is
// eliminated entirely, and mutating the slice element in place avoids copying
// the whole Value struct in and out on every iteration.
func BenchmarkValueSetDefinitionLevel(b *testing.B) {
	values := makeLevelValues()
	b.ResetTimer()
	for b.Loop() {
		for i := range values {
			values[i].SetDefinitionLevel(1)
		}
	}
}
