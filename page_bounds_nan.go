package parquet

import "math"

// boundsFloat32ExcludeNaN computes the min and max of data, skipping NaN values.
// Returns ok=false if all values are NaN or data is empty.
//
// NaN values are excluded from min/max statistics to comply with the Parquet
// format specification and to match the behavior of other implementations:
//   - Apache parquet-mr (Java): PARQUET-1246 ignores NaN in float/double statistics.
//   - Apache Arrow (Rust): Excludes NaN from Parquet statistics.
//   - Apache Iceberg: Spec states that lower/upper bounds must be compared against
//     all non-null, non-NaN values only (Iceberg spec, "upper_bounds" / "lower_bounds").
//
// Excluding NaN from statistics allows query engines (e.g. Spark, Trino, DuckDB)
// to rely on min/max values for predicate pushdown and row-group/page skipping.
// If NaN were included, the statistics would be meaningless for filtering because
// NaN is unordered with respect to all other float values in IEEE 754.
func boundsFloat32ExcludeNaN(data []float32) (min, max float32, ok bool) {
	// Find the first non-NaN value to initialize min and max.
	i := 0
	for i < len(data) && isNaN32(data[i]) {
		i++
	}
	if i >= len(data) {
		return 0, 0, false
	}

	min = data[i]
	max = data[i]
	for _, v := range data[i+1:] {
		if isNaN32(v) {
			continue
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max, true
}

// boundsFloat64ExcludeNaN computes the min and max of data, skipping NaN values.
// Returns ok=false if all values are NaN or data is empty.
//
// See boundsFloat32ExcludeNaN for details on why NaN values are excluded.
func boundsFloat64ExcludeNaN(data []float64) (min, max float64, ok bool) {
	// Find the first non-NaN value to initialize min and max.
	i := 0
	for i < len(data) && math.IsNaN(data[i]) {
		i++
	}
	if i >= len(data) {
		return 0, 0, false
	}

	min = data[i]
	max = data[i]
	for _, v := range data[i+1:] {
		if math.IsNaN(v) {
			continue
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max, true
}

// valueIsNaN reports whether v is a floating-point NaN value.
// Returns false for non-float types.
func valueIsNaN(v Value) bool {
	switch v.Kind() {
	case Float:
		return isNaN32(v.Float())
	case Double:
		return math.IsNaN(v.Double())
	default:
		return false
	}
}

// isNaN32 reports whether f is an IEEE 754 "not-a-number" value.
func isNaN32(f float32) bool {
	return f != f
}
