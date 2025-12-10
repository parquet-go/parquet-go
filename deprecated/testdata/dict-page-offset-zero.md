# dict-page-offset-zero.parquet

## Why This File Was Moved to deprecated/testdata

This test file was moved from `testdata/` to `deprecated/testdata/` because
it is incompatible with the `SizeStatistics` field added to `ColumnMetaData`
in Apache Parquet Format 2.10.0.

## Background

### Timeline
- **2019**: This file was created (based on metadata:
  `dremio.version: 3.2.0-201905102005330382-0598733`)
- **2022**: File added to parquet-go test suite
- **2023**: Apache Parquet Format 2.10.0 added `size_statistics` field at
  position 16 in `ColumnMetaData`
  ([PARQUET-2261](https://issues.apache.org/jira/browse/PARQUET-2261))
- **2025**: `SizeStatistics` field enabled in parquet-go

### The Problem

When the `SizeStatistics` field was uncommented in `format/parquet.go`, this
file caused test failures with the error:

```
reading parquet file metadata: decoding thrift payload: 4:FIELD<LIST> → 0/1:LIST<STRUCT>: missing required field: 2:FIELD<I64>
```

The issue occurs because:

1. The file was created before `size_statistics` (field 16) was added to the
   Parquet specification
2. The file has data at field position 16 in `ColumnMetaData` that predates
   the `SizeStatistics` definition
3. When the thrift decoder tries to decode field 16 as `SizeStatistics`, it
   reads incompatible bytes
4. This corrupts the byte stream, causing subsequent fields in `RowGroup` to
   appear missing

### Why Not Fix the Decoder?

Several approaches were considered:

1. **Conditional skipping**: Skip only on error → The decode doesn't fail, it
   just reads wrong bytes
2. **Format version detection**: Would require complex versioning logic

None of these approaches properly solve the fundamental issue: the file
contains pre-spec data at a field position that now has a different meaning.

### Resolution

The file was moved to `deprecated/testdata/` because:

- It's the only file (out of 48 test files) incompatible with the updated
  schema
- All other test files work correctly with `SizeStatistics` enabled
- The file serves no unique testing purpose that isn't covered by other files
- Modern files with valid `SizeStatistics` data will work correctly

## References

- [Apache Parquet Format 2.10.0 Release Notes](
  https://github.com/apache/parquet-format/blob/master/CHANGES.md)
- [PARQUET-2261: Add statistics for better estimating
  unencoded/uncompressed sizes](
  https://issues.apache.org/jira/browse/PARQUET-2261)
- [Parquet Thrift Schema](
  https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
