# Investigation of Issue #187: SortingWriter Data Corruption

## Summary

**Status**: Issue #187 is still **OPEN** and **UNRESOLVED** as of January 2025.

**Finding**: The `fix-merge-column-ordering` branch does **NOT** address issue #187. That branch fixes different issues related to column index handling during schema conversion, but does not solve the data corruption problem reported in #187.

## Issue #187 Details

**Link**: https://github.com/parquet-go/parquet-go/issues/187
**Opened**: November 21, 2024
**Status**: Open

### Problem Description

Users report intermittent data corruption when writing Parquet files using `SortingWriter`:

- **Symptom**: Column data "leaks" into other columns and gets truncated
- **Affected Data**: Particularly affects base64-encoded binary data in `header` and `event` columns
- **Pattern**: Intermittent - only some files show corruption, corruption appears in sections
- **Configuration**:
  - Using `SortingWriter` with 10,000 row buffer
  - Sorting by "schema" and "position" columns
  - Writing MySQL binlog data through `io.Pipe` to S3

### Example from Issue

```
Expected: "2024-11-21 08:07:52"
Got:      "BgAAAAYAAAAAAAE=" (base64 data from another column)
```

## Related Issues and Fixes

### Issue #140 (Closed - May 2024)

**Link**: https://github.com/parquet-go/parquet-go/pull/140
**Problem**: Similar data corruption in `mergedRowReader`
**Root Cause**: Buffer reuse while caller still held references to rows

**Fix Applied**: Modified `mergedRowReader` to avoid reading new data into buffers while the caller still has references to existing rows. Key changes in `merge.go`:

```go
// In bufferedRowReader.next():
if !hasNext {
    // We need to read more rows, however it is unsafe to do so here because we haven't
    // returned the current rows to the caller yet which may cause buffer corruption.
    r.off = 0
    r.end = 0
}
return hasNext  // Returns false instead of trying to read more
```

This fix is **still present** in the current codebase.

### PR #220 (Closed - January 2025)

**Link**: https://github.com/parquet-go/parquet-go/pull/220
**Problem**: Reader issues (#206, #204) - reading incorrect data
**Note**: Mentioned #187 as "related" but did **NOT** fix it

## What fix-merge-column-ordering Actually Fixes

The `fix-merge-column-ordering` branch addresses:

1. **Column Index Handling in ConvertRowGroup** (`convert.go`):
   - When columns are reordered during schema conversion, `Value.columnIndex` was not updated
   - Introduced wrapper types (`convertedColumnChunk`, `convertedValueReader`, etc.) to fix column indexes
   - This prevents values from being written to wrong columns during schema conversion

2. **Map to Group Schema Support** (`column_buffer.go`):
   - Support for writing `map[string]T` to GROUP schemas (not MAP schemas)
   - Handling of `map[string]interface{}` with runtime type resolution

3. **Optional Byte Slice Handling** (`column_buffer.go`):
   - Fixed handling of `[]byte` in optional contexts

**These fixes are important** but address different problems than #187.

## Current Investigation Status

### What We Know

1. Issue #187 is about **writing** corruption (not reading)
2. It specifically affects `SortingWriter`
3. The #140 fix for `mergedRowReader` is still in place
4. The issue was reported 6 months after #140 was fixed, suggesting:
   - It's a different bug, OR
   - The #140 fix didn't completely solve the problem, OR
   - There's a regression

### Test Created

Created `issue_187_test.go` to attempt reproduction:
- 15,000 records with large base64 strings (500-1000 bytes)
- SortingWriter with 10k buffer (matching reported config)
- Sorts by schema and position
- Verifies all field data matches after write/read cycle

## Potential Root Causes to Investigate

Based on the symptoms and code analysis:

1. **Memory Reuse in SortingWriter Path**:
   - `SortingWriter` uses temporary row groups written to a buffer
   - On flush, it merges these row groups
   - Possible unsafe memory reuse during this process

2. **Column Buffer Handling**:
   - Binary/string data may be using underlying byte slices that get reused
   - Similar to #140 but in a different code path

3. **Sorting-Specific Issue**:
   - The sorting/merging process may be creating unsafe references
   - Large binary data may trigger specific edge cases

## Next Steps

1. **Run the reproduction test** (`issue_187_test.go`) in an environment with network access
2. **If test passes**: Try variations to trigger the intermittent corruption:
   - Different buffer sizes
   - Different data patterns
   - Different compression settings
   - Concurrent operations

3. **If test fails**: Use debugger/tracing to identify exactly where data corruption occurs:
   - Add instrumentation to track Value/Row memory addresses
   - Monitor buffer reuse patterns
   - Check for unsafe pointer operations

4. **Examine SortingWriter-specific code paths**:
   - Focus on `sorting.go`, the `sortAndWriteBufferedRows()` method
   - Check how temporary buffers are managed
   - Look for any Clone() or copy operations that might be missing

5. **Consider asking issue reporter for**:
   - Minimal reproduction case
   - Specific Go version and parquet-go version
   - Whether it happens with certain data patterns more than others

## Conclusion

The `fix-merge-column-ordering` branch improves column handling during schema conversion but **does not fix issue #187**. The SortingWriter corruption issue remains open and requires further investigation. A reproduction test has been created to aid in diagnosing the actual root cause.
