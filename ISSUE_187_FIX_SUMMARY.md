# Fix for Issue #187: Column Data Corruption in SortingWriter

## Issue Summary

**Issue:** https://github.com/parquet-go/parquet-go/issues/187

The issue reported intermittent data corruption when writing Parquet files using `SortingWriter`. Specifically:
- Data from certain columns (particularly binary/base64 data) was appearing in unrelated columns
- The corruption was intermittent and non-persistent within files
- Data appeared truncated and displaced across columns

## Root Cause

The root cause was in the column index handling during the `MergeRowGroups` operation used by `SortingWriter`.

### How SortingWriter Works

1. Accumulates rows in memory buffer (up to `sortRowCount`)
2. Sorts the buffer and writes to temporary row groups
3. When flushing, calls `MergeRowGroups()` to merge temporary row groups into final output
4. During merge, `ConvertRowGroup()` is called to ensure column ordering matches the merged schema

### The Bug

In `merge.go`, the merge process applies schema conversion:

```go
for i, rowGroup := range mergedRowGroups {
    rowGroupSchema := rowGroup.Schema()
    conv, err := Convert(schema, rowGroupSchema)
    if err != nil {
        return nil, fmt.Errorf("cannot merge row groups: %w", err)
    }
    mergedRowGroups[i] = ConvertRowGroup(rowGroup, conv)
}
```

The problem: When `ConvertRowGroup()` reordered columns, the internal `Value.columnIndex` field was not updated. This field indicates which column a value belongs to. When values had stale column indexes, they were written to the wrong columns, causing the reported data corruption.

## The Fix

The fix introduces wrapper types in `convert.go` that intercept and correct column indexes at multiple levels:

### New Types Added

1. **`convertedColumnChunk`**: Wraps `ColumnChunk` to return correct column index
   - Overrides `Column()` to return target position
   - Wraps `Pages()` to return `convertedPages`

2. **`convertedPages`**: Wraps `Pages` to return `convertedPage` instances
   - Propagates the target column index to pages

3. **`convertedPage`**: Wraps `Page` to return `convertedValueReader`
   - Overrides `Column()` to return correct position
   - Wraps `Values()` to return `convertedValueReader`

4. **`convertedValueReader`**: Wraps `ValueReader` to fix value column indexes
   - **Critical fix**: In `ReadValues()`, rewrites `Value.columnIndex` for each value:
     ```go
     func (r *convertedValueReader) ReadValues(values []Value) (int, error) {
         n, err := r.reader.ReadValues(values)
         // Rewrite columnIndex for all values to match target column position
         for i := range n {
             values[i].columnIndex = r.targetColumnIndex
         }
         return n, err
     }
     ```

### Modified Code in convert.go

The `ConvertRowGroup()` function now creates wrapped column chunks when reordering occurs:

```go
forEachLeafColumnOf(target, func(leaf leafColumn) {
    // ... find matching source column ...

    if i == int16(j) {
        // No reordering - use original chunk
        columns[i] = rowGroupColumns[j]
    } else {
        // Reordering - wrap to fix column index
        columns[i] = &convertedColumnChunk{
            chunk:             rowGroupColumns[j],
            targetColumnIndex: ^int16(i),
        }
    }
})
```

## Tests Added

1. **`TestConvertRowGroupColumnIndexes`**: Verifies `ColumnChunk.Column()` returns correct index after reordering
2. **`TestConvertRowGroupValueColumnIndexes`**: Verifies individual `Value.Column()` returns correct index (key test)
3. **`TestConvertRowGroupWithMissingColumns`**: Tests schema evolution with missing columns
4. **`writer_null_only_test.go`**: Tests writing/reading null-only optional columns

## Impact

This fix resolves the data corruption issue in:
- `SortingWriter` when merging row groups with reordered columns
- Any use of `ConvertRowGroup()` where column order differs between schemas
- Schema evolution scenarios where column positions change

## Additional Changes in fix-merge-column-ordering Branch

Beyond the core column index fix, the branch also includes:

1. **Map to Group schema support** (`column_buffer.go`):
   - Support for writing `map[string]T` to GROUP schemas (not MAP schemas)
   - Handling of `map[string]interface{}` with runtime type resolution
   - Extensive test coverage in `column_buffer_test.go`

2. **Optional byte slice handling** (`column_buffer.go`):
   - Fixed handling of `[]byte` in optional contexts
   - Treats all slices uniformly (removed special-casing of `[]byte`)

These changes improve the flexibility and correctness of Parquet schema conversions.
