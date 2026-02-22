# Review of PR #405: Reproduction of Issue #403

## Summary

PR #405 is a **partial but imprecise reproduction** of the bug reported in issue #403. While it does include test cases that exercise the key conditions (schema override with `Int(8)` + optional group + boolean field), it has gaps that weaken it as a definitive reproduction.

## What Issue #403 Reports

The reporter describes this exact scenario:

- **Struct**: `Frame` with `*TrackedExtra` (optional) and `bool TrackedExtraExists`
- **Schema override**: `GetExtraSchema()` specifying `parquet.Int(8)` for the nested `id` field
- **Writer**: `parquet.NewGenericWriter[Frame](fOut, config)` with schema applied via config
- **Reader**: `parquet.NewReader(fIn, readerConf)` (non-generic) with same schema
- **Symptom**: `TrackedExtraExists` always reads as `false` despite being written as `true`

## Test Case Analysis

### Tests 1-3: No schema override (DON'T reproduce the bug)

These are identical to PR #404 (by maintainer achille-roussel) and use only the struct-inferred schema. Without the `Int(8)` override, the `ID int` field correctly maps to INT64, and there is no type mismatch. These tests pass and add no new information beyond what PR #404 already established.

### Test 4: Schema on writer only

Uses `NewGenericWriter[Frame]` with `GetExtraSchema()` but reads with `NewGenericReader[Frame]` without schema override. This is an **asymmetric configuration** not described in the original issue (which applies the schema to both writer and reader).

### Test 5: Schema on both writer and reader (generic)

Uses `NewGenericWriter[Frame]` + `NewGenericReader[Frame]` with the schema on both sides. This is close but uses the **generic reader** instead of the non-generic `NewReader` used in the original issue. The generic and non-generic readers have different schema resolution and type conversion paths (writer.go:97-150 vs reader.go:31-70).

### Test 6: Schema on both + non-generic reader (closest match)

Uses `NewGenericWriter[FramePtr]` + `NewReader` with schema on both. This is the **closest to the original issue** but differs in two ways:
1. Uses `FramePtr` (with `*bool`) instead of `Frame` (with `bool`)
2. The `*bool` vs `bool` distinction can affect deserialization behavior since pointer types interact differently with parquet's definition levels

## The Missing Test Case

No test case in PR #405 exactly replicates the original issue's pattern:

```
NewGenericWriter[Frame] + schema  -->  NewReader + schema  -->  Frame (bool, not *bool)
```

This specific combination is what the reporter described but it is not present in the PR.

## Root Cause Context

The `Int(8)` schema override is central to triggering the bug. `parquet.Int(8)` maps to physical type INT32 (type_int_logical.go:78, `baseType()` returns `int32Type{}` for BitWidth < 64), while `ID int` in Go is 64-bit (INT64). This forces the writer into the slower schema-conversion code path (writer.go:137-151) instead of the optimized direct-mapping path (writer.go:131-136), because `EqualNodes()` detects the mismatch.

However, the actual boolean parsing failure appears to be a separate library-level regression in v0.26.0+:
- **PR #408** (merged): Fixed `booleanDictionary.Lookup` using `offsetOfU64` instead of `offsetOfBool` in dictionary_boolean.go
- **PR #410** (open): Fixes stale data in pooled `valuesSliceBuffer.reserve()` where reused buffers retain values from previous `Reconstruct` calls

## Verdict

**PR #405 is a partial reproduction that does not precisely match the original issue.**

- Tests 1-3 are redundant with PR #404 (don't exercise the bug condition)
- Tests 4-6 get progressively closer but none exactly match the reported scenario
- The PR conflates the schema type mismatch (`Int(8)` vs `int`) with the boolean parsing regression
- The actual root cause appears to be the boolean dictionary offset bug (PR #408) and/or stale buffer reuse (PR #410), which are general boolean reading regressions not specific to the optional-group-before-bool pattern
