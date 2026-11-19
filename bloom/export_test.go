package bloom

// Fold exposes fold to the tests of the bloom_test package.
func (f SplitBlockFilter) Fold() SplitBlockFilter { return f.fold() }
