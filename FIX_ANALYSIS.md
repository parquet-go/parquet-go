# Analysis: Does fix-merge-column-ordering Fix Issue #187?

## Conclusion

**YES, the `fix-merge-column-ordering` branch likely DOES fix issue #187.**

## The Connection

### Issue #187 Symptoms
- Data from binary columns (base64-encoded) appears in other columns
- Data gets truncated
- Intermittent corruption
- Specifically affects `SortingWriter`

### Root Cause Identified

When `SortingWriter.Flush()` is called:

1. It writes sorted batches to temporary row groups in a buffer
2. It calls `MergeRowGroups(f.RowGroups(), ...)` to merge them
3. **During merge, even though schemas are identical, the merge process can reorder fields**
4. Before fix-merge-column-ordering: column reordering caused `Value.columnIndex` to be wrong
5. Values with wrong column indexes get written to incorrect columns → **corruption!**

### Evidence

From `merge.go:46-48` (added in fix-merge-column-ordering):
```go
// Always apply conversion when merging multiple row groups to ensure
// column indices match the merged schema layout. The merge process can
// reorder fields even when schemas are otherwise identical.
```

This comment explicitly states the problem exists even with identical schemas, and the fix applies `ConvertRowGroup()` which corrects column indexes via the wrapper types.

### Why It Was Intermittent

- The corruption only occurs when the merge process reorders fields
- Field ordering during `MergeNodes()` may depend on:
  - Internal map iteration order
  - Field insertion order
  - Schema equality checks
- This explains why "only some files" show corruption

### Why It Affects Binary Data Most Visibly

- Binary/base64 data is large and distinctive
- When a 500-byte base64 string appears in a timestamp field, it's obvious
- Small values (numbers, short strings) might go unnoticed or look like plausible data

## Testing Required

To confirm this fix:

1. Run the reproduction test in `issue_187_test.go` **BEFORE** the fix-merge-column-ordering merge:
   ```bash
   git checkout <commit before merge>
   go test -run TestIssue187SortingWriterCorruption
   ```
   Expected: Should fail (show corruption)

2. Run the same test **AFTER** the fix-merge-column-ordering merge:
   ```bash
   git checkout claude/session-011CUYMh93Civ3peHHPu1g8Q
   go test -run TestIssue187SortingWriterCorruption
   ```
   Expected: Should pass (no corruption)

## Recommendation

1. **Test the reproduction case** to confirm the fix works
2. **Ask the original reporter** to test with latest code from fix-merge-column-ordering branch
3. If confirmed working, **close issue #187** with reference to the fix-merge-column-ordering changes
4. Consider adding a **regression test** similar to `issue_187_test.go` to the test suite

## Summary

The fix-merge-column-ordering branch appears to solve issue #187 by ensuring column indexes remain correct even when `MergeRowGroups()` (used by `SortingWriter`) reorders fields during the merge process. The wrapper types (`convertedValueReader` etc.) intercept values and correct their `columnIndex` field, preventing data from being written to wrong columns.
