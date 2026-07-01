package variant

import (
	"sort"
	"testing"
)

func TestEncoderLayoutInvariants(t *testing.T) {
	// Enable layout verification hooks to assert encoder state
	testHookVerifyArrayLayout = func(e *encoder, positions []elementPos) {
		if len(positions) == 0 {
			return
		}
		firstStart := positions[0].start
		if firstStart > len(e.scratch) {
			t.Errorf("invariant violation: firstStart %d > len(scratch) %d", firstStart, len(e.scratch))
		}
		for i := 1; i < len(positions); i++ {
			if positions[i].start != positions[i-1].end {
				t.Errorf("invariant violation: non-contiguous siblings at index %d: start %d != prior end %d", i, positions[i].start, positions[i-1].end)
			}
		}
	}
	testHookVerifyObjectLayout = func(e *encoder, entries []encodedField) {
		if len(entries) == 0 {
			return
		}
		// Copy and sort entries by start position to verify their contiguous allocation layout in scratch
		sorted := make([]encodedField, len(entries))
		copy(sorted, entries)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].start < sorted[j].start
		})
		firstStart := sorted[0].start
		if firstStart > len(e.scratch) {
			t.Errorf("invariant violation: firstStart %d > len(scratch) %d", firstStart, len(e.scratch))
		}
		for i := 1; i < len(sorted); i++ {
			if sorted[i].start != sorted[i-1].end {
				t.Errorf("invariant violation: non-contiguous siblings at index %d: start %d != prior end %d", i, sorted[i].start, sorted[i-1].end)
			}
		}
	}
	defer func() {
		testHookVerifyArrayLayout = nil
		testHookVerifyObjectLayout = nil
	}()

	// Run various complex structures to trigger layout validation
	testCases := []any{
		[]any{1, "hello", []any{2, 3, map[string]any{"x": 100, "y": 200}}},
		map[string]any{
			"nested": map[string]any{
				"array": []any{true, false, nil},
			},
		},
	}

	for _, tc := range testCases {
		_, _, err := Marshal(tc)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
	}
}
