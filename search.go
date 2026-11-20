package parquet

// Search is like Find, but uses the default ordering of the given type. Search
// and Find are scoped to a given ColumnChunk and find the pages within a
// ColumnChunk which might contain the result.  See Find for more details.
func Search(index ColumnIndex, value Value, typ Type) int {
	return Find(index, value, CompareNullsLast(typ.Compare))
}

// Find uses the ColumnIndex passed as argument to find the page in a column
// chunk (determined by the given ColumnIndex) that the given value is expected
// to be found in.
//
// The function returns the index of the first page that might contain the
// value. If the function determines that the value does not exist in the
// index, NumPages is returned.
//
// If you want to search the entire parquet file, you must iterate over the
// RowGroups and search each one individually, if there are multiple in the
// file. If you call writer.Flush before closing the file, then you will have
// multiple RowGroups to iterate over, otherwise Flush is called once on Close.
//
// The comparison function passed as last argument determines the relative order
// of non-null values (generally the Compare method of the column type) and must
// match the ordering used for the index's min and max values, regardless of
// whether pages are ascending, descending, or unordered.
//
// When value is null, the comparison function is not used: Find returns the
// first page that may contain nulls (an all-null page, a page with a non-zero
// NullCount, or a page whose optional NullCounts metadata is absent).
func Find(index ColumnIndex, value Value, cmp func(Value, Value) int) int {
	switch {
	case index.IsAscending() && !value.IsNull():
		return binarySearch(index, value, cmp)
	default:
		return linearSearch(index, value, cmp)
	}
}

func binarySearch(index ColumnIndex, value Value, cmp func(Value, Value) int) int {
	numPages := index.NumPages()
	topIndex := numPages
	currentIndex := 0
	found := numPages

	for currentIndex < topIndex {
		nextIndex := currentIndex + (topIndex-currentIndex)/2

		// All-null pages are not ordered relative to non-null pages in the
		// column index; scan left for the nearest non-null page in range.
		probe := nextIndex
		for probe >= currentIndex && index.NullPage(probe) {
			probe--
		}
		if probe < currentIndex {
			currentIndex = nextIndex + 1
			continue
		}

		switch {
		case cmp(value, index.MinValue(probe)) < 0:
			topIndex = probe
		case cmp(value, index.MaxValue(probe)) > 0:
			currentIndex = nextIndex + 1
		default:
			found = probe
			topIndex = probe
		}
	}

	return found
}

func linearSearch(index ColumnIndex, value Value, cmp func(Value, Value) int) int {
	n := index.NumPages()

	// Page bounds exclude nulls, so a null value cannot be matched by them.
	if value.IsNull() {
		nc, hasCheck := index.(columnIndexNullCounts)
		for i := range n {
			if index.NullPage(i) || (hasCheck && !nc.hasNullCount(i)) || index.NullCount(i) > 0 {
				return i
			}
		}
		return n
	}

	for i := range n {
		if index.NullPage(i) {
			continue
		}
		min := index.MinValue(i)
		max := index.MaxValue(i)

		if cmp(min, value) <= 0 && cmp(value, max) <= 0 {
			return i
		}
	}

	return n
}
