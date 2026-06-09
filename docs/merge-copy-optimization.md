# Merge & Copy Optimization Design

This document audits the optimizations that already exist in the row/page/value
copy and merge paths, and specifies the design for two new optimizations:

1. **Skip decompression / re-encoding** when pages can be copied from source to
   destination (priority 1).
2. **Multi-CPU parallel processing of columns** (priority 2).

## Governing invariant

> The copy optimization must be **semantically transparent**: the output must
> honor every aspect of the writer configuration (encoding, codec, data page
> version, statistics, bloom filters, encryption). Any property where a verbatim
> copy would diverge from configured behavior forces demotion to the layer that
> can satisfy it — down to full decode. Only config-unspecified internals (exact
> page boundaries / sizing) may differ.

Correctness dominates. The fast path is a pure accelerator that never changes
results. The output must be byte-identical regardless of worker count (the
parallel path buffers and assembles columns in deterministic column order, never
in completion order).

## Existing optimizations (audit)

### `CopyRows` (row.go:305)

- Schema conversion is applied **only** when `targetSchema != sourceSchema`
  (`EqualNodes`), explicitly trading away the `RowWriterTo` fast path for safety
  in the rare evolving-schema case.
- Two interface fast paths: `src.(RowWriterTo).WriteRowsTo(dst)` and
  `dst.(RowReaderFrom).ReadRowsFrom(src)`.
- Otherwise a batched copy through a reusable `[]Row` buffer
  (`defaultRowBufferSize`), with `clearRows` recycling.

### `CopyValues` (value.go:102)

- Same shape: `ValueWriterTo` / `ValueReaderFrom` fast paths, else a batched
  buffer copy.

### `CopyPages` (page.go:340)

- Plain `ReadPage` → `WritePage` loop with `Release(p)` recycling. No fast-path
  interface.

### `MergeRowGroups` (merge.go:25)

- Empty / schema auto-merge handling via `MergeNodes`.
- No effective sorting columns → `multiRowGroup` **concatenation**
  (deterministic, no heap).
- **Non-overlapping segment detection** (`overlappingRowGroups`, merge.go:127):
  uses the column-index min/max of the sorting columns to partition row groups
  into segments. Single-row-group segments are concatenated directly; only
  overlapping segments go through the heap merge. *This is already a "direct copy
  of non-overlapping ranges", but at row-group granularity.*
- Dedup wrapping only when configured.

### `MergeRowReaders` (merge.go:472)

- Specialized by arity: 0 → empty, 1 → passthrough, 2 → `mergedRowReader2`
  (no heap, bulk-drains the survivor when one side ends), N → heap
  `mergedRowReader`. Both buffer 24 rows via `bufferedRowReader`.

### The gap

`Writer.WriteRowGroup` (writer.go:536) always does `rowGroup.Rows()` →
`CopyRows` → `WriteRows` → column buffers → `writeDataPage` (writer.go:2240),
which **re-encodes and re-compresses every value** (`buf.compress`,
writer.go:2269). Nothing in the merge/copy path ever moves compressed page bytes
through intact.

The read seam already exists: `FilePages.readPage` (file.go:1371) reads the
**compressed** page bytes into a buffer; decompression + decode only happen later
in `decodeDataPageV1/V2`. So `(header, compressed-bytes)` is available before any
codec work. There is no "decompressed-but-not-decoded" page object today — L2
needs a new intermediate construct.

## The copy cascade (priority 1)

The copy unit is the **column chunk**; layer selection is per column chunk (and
within a chunk, uniform across its pages, since pages of one chunk share encoding
and codec). Four layers, each firing in strictly more situations than the one
above:

| Layer | What it skips | Fires when |
|-------|---------------|------------|
| **L0** — chunk copy | decompress + decode + encode + compress | `enc_src == enc_dst && codec_src == codec_dst && type matches` and no disqualifier |
| **L1** — page copy, skip decompression | decompress + decode + encode + compress (per page) | same as L0 but applied page-by-page (mainly relevant to coalescing, deprioritized) |
| **L2** — page copy, decompress but skip decoding | decode + encode | `enc_src == enc_dst && codec_src != codec_dst` |
| **L3** — page copy, decode but skip row assembly | per-row deconstruct/reconstruct | `enc_src != enc_dst`, or any demotion below |

Since each output row group derives from exactly **one** source row group (1:1,
see use case below), the output column chunk is built entirely from one source
chunk — including its dictionary page — so even per-page copies stay
dictionary-safe. No cross-source dictionary remapping is needed.

### Codec/encoding reconciliation

Strict: the output **honors the writer's configuration**. Optimizations fire only
when source and destination configs match. The per-chunk predicate is therefore
deterministic (see table). This mirrors the existing `CopyRows` philosophy
(convert only when schemas differ; optimize the matched-config common case).

### Statistics, indexes, bloom filters

For verbatim-copied (L0/L1) chunks:

- Copy each page's `Statistics` from the source page header.
- Copy the source `ColumnIndex` min/max/null/nan arrays verbatim; **regenerate
  the `OffsetIndex`** from the new file positions (every page lands at a new
  offset, so `OffsetIndex` and the chunk's dictionary/data offsets in
  `ColumnMetaData` must be rewritten even though the bytes are verbatim).
- Copy the source bloom filter bytes if present **and** the writer is configured
  to want one for that column.

Demotions required to preserve the invariant:

- **Incompatible bloom-filter configuration** (writer wants a filter the source
  lacks) → demote to **L3** (decode and rebuild). Most columns have no filter, so
  the gains hold.
- **Missing source statistics** + writer configured to write stats → demote to
  **L3** (decode and compute). Conservative: respect the writer config rather
  than silently dropping stats.

### Hard disqualifiers (force L3 / full decode)

- **Encryption** on either side — per-page AAD keys on row-group/column/page
  ordinals change in the output; re-encryption needs the plaintext.
- **Data page version mismatch** (V1 vs V2) — byte layout differs even at equal
  encoding; transcoding needs decoded levels/values.

CRC stays valid for L0/L1 (bytes unchanged); L2 (recompress) must recompute it.

## Use case: rewrite large row groups 1:1

The primary use case is **rewriting / merging row groups that are already at or
near target size**, not consolidating many tiny row groups into fewer large ones.

Consequence: a verbatim L0 chunk copy is inherently **1:1** (source row group →
output row group). Cross-source **L1 coalescing** (concatenating pages from
multiple source chunks to hit a target row-group size) is **out of scope** for
the initial design; it is a partial win anyway because dict-encoded columns would
fall to L3.

## Dispatch (transparent)

The optimization is transparent through `Writer.WriteRowGroup` — no new public
API the caller must opt into. A copyable source advertises its capability via a
**fast-path interface** (a raw-page capability, analogous to `RowReaderFrom` /
`RowWriterTo`). `WriteRowGroup` detects it and routes per column chunk to the
copy path, falling back to the existing re-encode path for any non-matching
column.

The capability must be **forwarded through wrappers**: `Convert` returns
`identity` for equal nodes (convert.go:341) and `ConvertRowGroup` returns the row
group unwrapped when `EqualNodes` holds (convert.go:541), but reindexed columns
get a `convertedColumnChunk`, and merge segments get `multiColumnChunk`. These
wrappers must delegate the raw-page interface to the underlying chunk for the
transparent path to survive a merge.

Write side: a sibling to `writeDataPage`, e.g. `writeRawPage(header,
compressedBytes, copiedStats)`, writes verbatim bytes to the column's buffer via
`writePageTo` and records the copied stats / column-index entries (instead of
computing them). Columns fill independent buffers; `writeRowGroup` assembles them
in column order — which is also the parallelism boundary.

## Parallel column processing (priority 2)

- **Unit:** columns within a row group. Each column chunk's bytes are produced
  concurrently into independent buffers, then written sequentially in column
  order. (Row-group-level parallelism already exists via `BeginRowGroup` /
  `ConcurrentRowGroupWriter`.)
- **Bounding:** a bounded worker pool (default `GOMAXPROCS`, configurable via a
  `WriterConfig` knob), processing columns in waves so peak memory is ≈ *degree*
  column chunks, not all of them. Critical for wide schemas (thousands of leaf
  columns).
- **Determinism:** completed-but-not-yet-writable columns buffer until their turn
  in column order. A slow column can stall writing of later columns it finished
  first; that is accepted to preserve byte-identical output.
- **Opt-in:** default off (degree 1). Concurrent reads multiply memory and load
  on the source `ReaderAt`; not every caller wants that tradeoff.
- **Source restriction:** random-access (`io.ReaderAt`) sources only. Each
  `ColumnChunk.Pages()` creates its own `FilePages` reading its byte range via
  the file's `ReaderAt`, so concurrent column readers are naturally isolated.
- **Path coverage:** only the single-source row-group path (plain rewrite +
  non-overlapping segments). The overlapping-segment **heap merge**
  (`mergedRowReader`) is row-oriented and stays sequential — partitioning a
  sorted output range across workers is a separate, harder problem not justified
  by the primary use case.

## Delivery sequence

1. **Increment 1 — single-source L0 copy through `WriteRowGroup`.** Detect a
   copyable file-backed row group, copy whole column chunks verbatim (dict + data
   pages), offset-translate the column/offset index, copy stats; demote any
   non-matching column to the existing re-encode path. Delivers the headline win
   for plain file rewrites; does not touch `MergeRowGroups`.
2. **Increment 2 — column parallelism.** `WriterConfig` knob, bounded pool, over
   the single-source path.
3. **Increment 3 — merge integration.** Teach `MergeRowGroups` to surface
   non-overlapping single-source segments through the same fast-path interface.
4. **Increment 4 (optional) — L2/L3 partial layers** for codec/encoding-mismatched
   columns, and only-if-needed L1 coalescing.

Each increment is independently shippable. The transparency invariant means each
is a pure accelerator that can be merged without behavior change.

## Implementation status

### Increment 1 — single-source L0 copy (DONE)

`writer_copy.go` + hooks in `writer.go`. `Writer.WriteRowGroup` detects when an
input row group's column chunks can be copied verbatim and splices the
contiguous source byte range (dictionary page + data pages) into the output,
copying statistics, column index, and offset index (offset-translated). The
existing re-encode path is untouched (it now lives in the `else` branch of the
column loop in `writeRowGroup`). All-or-nothing per row group: if any column is
not copyable the whole row group falls back.

Predicate (`columnChunkIsCopyable`): matching physical type, codec, encoding, and
data page version; source not encrypted and writer not encrypting; no
writer-requested bloom filter; source column index and offset index present.
Anything else demotes to the re-encode path (honoring the transparency
invariant).

The copy path streams each source byte range (dictionary + data pages) straight
into the output via `offsetTrackingWriter.ReadFrom` → `bufio.ReadFrom`, with no
intermediate buffer and no per-chunk allocation. An earlier version staged the
bytes in a `make([]byte, TotalCompressedSize)` per chunk; profiling showed that
allocation was ~96% of all allocations and the resulting GC/scheduler churn
dominated CPU (the throughput was alloc/GC-bound, *not* memory-bandwidth-bound).
Removing it cut time/op ~3× and B/op ~27×.

Benchmark (`BenchmarkWriteRowGroupCopy` vs `…Reencode`, Apple M4 Max, 200k rows,
6 columns, Snappy, 4 row groups, in-memory source → `io.Discard`):

| | ns/op | throughput | B/op | allocs/op |
|---|---|---|---|---|
| Copy (L0) | ~86,000 | ~30 GB/s | 104 KB | 408 |
| Re-encode | ~30,300,000 | ~87 MB/s | 23.2 MB | 2,579 |
| Ratio | **~350×** | ~350× | ~220× | ~6× |

Single-threaded (`GOMAXPROCS=1`) the copy path sustains ~28 GB/s; the remaining
cost is the actual byte copy (`bufio.ReadFrom`) plus footer thrift-encoding of
the copied page indexes, both largely irreducible. This is CPU/allocation
savings; with real disk/network I/O the wall-clock multiple compresses (I/O
becomes a larger share) but the CPU/alloc wins hold.

### Increment 2 — column parallelism (EVALUATED, REMOVED)

A `WriteColumnConcurrency` `WriterConfig` option with a bounded worker pool was
implemented and verified deterministic/race-free, then **removed**. Rationale:

The streaming rewrite (see Increment 1) made `loadCopiedChunk` cheap — it reads
only the small page index and records byte ranges; the actual data copy happens
sequentially in `writeRowGroup`. So the concurrency knob parallelized only the
*cheap* index reads while the *expensive* data copy stayed serial, yielding no
measurable benefit on any source (neutral-to-negative in every benchmark).
Shipping a public API option that can't be justified by a benchmark is a
liability (removal would be breaking), so it was dropped.

Parallelism should be reintroduced only where the per-column work is actually
expensive — i.e. alongside the L2/L3 re-encode layers (below), or as parallel
*data* prefetch into pooled buffers for high-latency sources — and designed for
that case rather than retrofitted onto the now-trivial copy load.

### Increment 3 — merge integration (DONE)

Two parts:

1. `fileColumnChunkOf` unwraps `convertedColumnChunk` (positional column remap,
   e.g. column reordering from `MergeNodes`) to reach the underlying file-backed
   chunk, so copy fires through that wrapper. A type difference is still caught
   and demoted by the predicate's type check.
2. Segment splitting: `WriteRowGroup` recognizes a row group that is the in-order
   concatenation of independently writable segments
   (`orderedRowGroupSegments`) and, when at least one segment is copyable, writes
   each segment as its own output row group — so a merge of non-overlapping,
   sorted inputs copies its segments verbatim while re-encoding only the
   overlapping ones.

Decision on the transparency invariant: row-group partitioning *below*
`MaxRowsPerRowGroup` is treated as an unspecified internal (the same class as
page boundaries/sizing, which the invariant already permits to differ). Emitting
one output row group per copyable segment preserves all rows, their order, and
correct per-row-group statistics/indexes; only the number of row groups (each
still ≤ `MaxRowsPerRowGroup`) may differ from the re-encode path.

Guards that keep the change scoped and correct:
- `mergedRowGroup` (overlapping heap merge) returns nil segments — it is never
  split, because its order depends on cross-row-group interleaving.
- `sortedSegmentRowGroup` returns nil segments when `dropDuplicatedRows` is set —
  deduplication spans segment boundaries and would be lost by independent writes.
- Splitting only happens when at least one segment is copy-eligible, so pure
  re-encode merges keep their existing output structure.

### Increment 4 — L2/L3 partial layers (DEFERRED, optional)

Deliberately not implemented. The config-mismatch case (different codec or
encoding) already produces correct output via the row-oriented re-encode
fallback; L2/L3 would only make it faster. The plan marked this increment
optional, and L2's raw-page recompression carries file-corruption risk
(notably the DataPageV2 layout, below) that is disproportionate to a marginal
speed gain on a non-primary case. Documented mechanism for a future, deliberate
implementation:

- **L2 (decompress, recompress, skip decode)** for encoding-matches /
  codec-differs (e.g. recompress Snappy→Zstd): iterate raw pages, decompress the
  body, recompress with the destination codec, rewrite the page header
  (`CompressedPageSize`, CRC), copy statistics, rebuild the offset index.
  Caveat: DataPageV2 stores repetition/definition levels *uncompressed* ahead of
  the values, so only the values portion may be recompressed
  (`{Repetition,Definition}LevelsByteLength`); the simplest safe first cut would
  handle DataPageV1 only and demote V2.
- **L3 (decode, skip row assembly)** mainly buys per-column parallelism over the
  re-encode path (the payoff for `WriteColumnConcurrency` on mismatched configs);
  lower priority than L2.
