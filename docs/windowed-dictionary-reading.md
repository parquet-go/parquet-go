# Windowed dictionary reading

```go
file, err := parquet.OpenFile(source, size, parquet.WindowedDictionaryReading(true))
```

The option defaults to false and is also available as
`FileConfig.WindowedDictionaryReading`.

## Supported pages

Flat, required BYTE_ARRAY columns whose data pages use RLE_DICTIONARY (or the
legacy PLAIN_DICTIONARY) encoding, with either data-page version and any
compression. All other pages keep eager decoding.

## Design

An eligible page keeps its encoded buffer and dictionary instead of decoding
every index into an `[]int32`. A resumable cursor walks the RLE/bit-packed
runs and pauses mid-run between reads:

- RLE runs, and the zero-extended tail of a clean short stream, do one
  dictionary lookup and fill the output. They use no index storage.
- Bit-packed IDs are unpacked into a window of at most 1024 `int32` (4 KiB),
  sized to the caller's batch and allocated on the first bit-packed run.
  A column reader reuses one window across pages and seeks.
- Byte-array values reference dictionary storage, never the window, so value
  lifetimes match the eager path.

This follows Arrow Rust (`DictIndexDecoder`), Arrow C++
(`RleBitPackedDecoder::GetBatchWithDict`) and Polars (`HybridRleDecoder`). It
does not stream the dictionary itself.

## Validation

Before a page is returned, the whole stream is structurally validated and
every index is checked against the dictionary, so malformed input fails from
ReadPage as it does today. RLE runs are checked without expansion. Bit-packed
IDs are skipped when the dictionary covers every ID at that bit width.
Surplus values after the logical count are parsed but ignored, and a clean
short stream is zero-extended, both matching eager decoding.

## Limits

- `Page.Data()` materializes the full index array once per page, for page
  encoders and other callers that need it. That page loses the bound.
- Starting a slice replays the preceding run headers. There are no cursor
  checkpoints, so many increasing-offset slices of one page cost quadratic
  time in the number of runs.
- Encoded pages, dictionaries, other columns and returned values are separate
  memory costs. The option bounds intermediate indexes, not total heap or RSS.
- Exported `Column.DecodeDataPageV1/V2` copy eligible input, so a caller
  reusing its buffer cannot change a lazy page.
- When the dictionary does not cover the bit width, validation unpacks
  bit-packed IDs a second time. Fully bit-packed pages with such dictionaries
  are within a few percent of eager.
