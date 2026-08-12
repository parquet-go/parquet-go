# DELTA_BYTE_ARRAY prefix computation: fixed-length prefixes vs word-parallel exact LCP

Research notes on an idea to speed up `DELTA_BYTE_ARRAY` by quantizing common
prefix lengths to fixed sizes (8, 16, 32, 64 bytes, ...) so that matching,
encoding, and decoding can rely on fixed-width operations.

**Conclusion: the encode-side speedup attributed to fixed-length prefixes is
almost entirely achievable with an *exact* longest-common-prefix (LCP)
computed word-at-a-time (XOR + trailing-zero-count), without giving up any
compression ratio. Quantization measured 0% faster than exact word-parallel
LCP while degrading the encoded size by ~3.4% on prefix-heavy data. The
word-parallel exact LCP is prototyped on this branch: +61% encode throughput
at identical output.**

## 1. The idea and its spec-legality

`DELTA_BYTE_ARRAY` stores, per value, the length of a prefix shared with the
previous value (`DELTA_BINARY_PACKED`) plus the remaining suffix
(`DELTA_LENGTH_BYTE_ARRAY`). The parquet-format spec
([Encodings.md](https://github.com/apache/parquet-format/blob/master/Encodings.md))
says only "store the prefix length of the previous entry plus the suffix" —
it never requires the *longest* prefix. The PARQUET-246 history (readers must
honor a nonzero prefix length on the first value of a page) confirms readers
treat prefix lengths as arbitrary valid data. So a writer quantizing prefix
lengths is spec-legal and every reader stays correct.

## 2. Prior art

### Computer science literature

- **Front coding / compressed string dictionaries** (Brisaboa, Martínez-Prieto,
  Navarro et al., SEA 2011 / Inf. Systems 2016 / CIKM 2019): Plain Front
  Coding always stores *exact* LCPs; the tunable knob in this literature is
  the restart-bucket size, never the prefix-length grid. Quantized prefix
  lengths do not appear as a named technique anywhere we could find.
- **FSST** (Boncz, Neumann, Leis, VLDB 2020): strongest precedent for the
  "fixed-width copies beat variable-width" principle — symbols capped at 8
  bytes precisely so decode is a branch-free 8-byte word write per code.
  Applied to substring symbols, not prefix lengths.
- **German strings / Umbra, Arrow StringView** (in DuckDB, Velox, Polars):
  fixed 4-byte inline prefix; ~95% of equality checks short-circuit on it.
  Quantizes the *comparison-relevant* prefix, not the stored encoding.
- **LevelDB/RocksDB restart points, Druid incremental encoding, Lucene
  BlockTree**: quantize *restart positions* (shared length forced to 0 every
  N entries) for random access — a different axis than quantizing lengths.
- **The LZ match-length idiom** (`ZSTD_count`, LZ4, zlib-rs `compare256`):
  exact LCP computed 8–32 bytes at a time via wide loads + XOR/compare +
  trailing-zero-count. This is the well-established fast path for exact LCP,
  and the reason quantization gains ~nothing at encode time: the exact
  version is the same loop plus one `TZCNT` at the mismatch.

### Parquet / Arrow / columnar ecosystems

- **arrow-rs PR [#10549](https://github.com/apache/arrow-rs/pull/10549)**
  (merged 2026-08): closest prior art. Replaced the byte-at-a-time LCP in the
  DELTA_BYTE_ARRAY encoder with 32-byte block compares (`chunks_exact(32)`)
  plus a byte-wise tail — ~15x faster LCP on long prefixes, DBA encode became
  cheaper than PLAIN. **Kept the prefix exact; quantization was not
  discussed.** (Note: 64-byte blocks regressed to an out-of-line `bcmp` call
  in Rust; 32 was the sweet spot.)
- **parquet-java** `DeltaByteArrayWriter` uses `Arrays.mismatch`, which the
  JVM intrinsifies to SIMD — exact LCP, SIMD for free.
- **Arrow C++** encoder still uses a scalar byte loop; decoder does two
  variable-length memcpys per value (open issue
  [arrow#37873](https://github.com/apache/arrow/issues/37873) about skipping
  zero-length copies).
- **DuckDB** doesn't write DELTA_BYTE_ARRAY at all (prefers
  DELTA_LENGTH_BYTE_ARRAY / DICT_FSST); its reader transcodes DBA pages to
  PLAIN layout up front.
- **Velox** decoder avoids the prefix copy entirely by decoding in place over
  the previous value (only suffix bytes are copied); their 2026 blog post on
  DELTA decoding attacked the decoder around the format, keeping exact
  prefixes.
- **Trino PR [#15923](https://github.com/trinodb/trino/pull/15923)**: batched
  DBA decode, 3.5–9x; even optimized, DBA decodes ~2x PLAIN vs DLBA's ~5x —
  the chained variable-length prefix copy is the structural decode
  bottleneck.
- **cuDF** computes LCP byte-per-thread on GPU; their in-source comment notes
  the string copy dominates, not the LCP.
- **ORC, Lance, Vortex, BtrBlocks, FastLanes**: none use front coding for
  strings at all — they reach for FSST/dictionary instead of making prefixes
  fixed-width.
- **No Parquet Jira, mailing-list thread, or format issue** proposes capping,
  rounding, or quantizing prefix lengths. The idea appears untried and
  undiscussed.

## 3. Measurements (Apple M4 Max, arm64, Go 1.26)

Data: 10k values resembling sorted URLs/paths (5 hot prefixes of 24–53 bytes
+ 8-digit suffix), the workload DELTA_BYTE_ARRAY exists for. The generic
benchmarks in `encoding_test.go` use *random* byte arrays with ~no shared
prefixes, so they do not exercise the prefix search at all.

Isolated prefix-search strategies (higher is better):

| strategy                          | throughput | suffix bytes kept |
|-----------------------------------|-----------:|------------------:|
| byte-by-byte (current)            |   4.7 GB/s |             68.7% |
| exact LCP, 8B words + XOR/TZCNT   |  13.3 GB/s |             68.7% |
| quantized to multiples of 8       |  13.6 GB/s |             71.2% |
| quantized to powers of two (8..64)|  10.7 GB/s |             76.6% |

End-to-end `ByteArrayEncoding.EncodeByteArray` on the same data
(`BenchmarkEncodeByteArrayPrefixHeavy`):

| encoder LCP                | encode      | encoded/raw ratio |
|----------------------------|------------:|------------------:|
| byte-by-byte (baseline)    |  2.56 GB/s  |            0.7266 |
| word-exact (prototype)     |  4.11 GB/s  |            0.7266 |
| quantized multiples of 8   |  4.10 GB/s  |            0.7512 |

Decode was unchanged in all cases (~6.7 GB/s on the arm64 pure-Go path):
quantized prefixes don't help decode here because the cost is per-value
`memmove` dispatch, not copy alignment — and the amd64 AVX2 decoder already
copies 32-byte blocks unconditionally regardless of the stored length.

Also of note: the prefix-length stream did not get cheaper under
quantization. Lengths that are multiples of 8 have deltas that are multiples
of 8, which need *more* bits in DELTA_BINARY_PACKED than exact-LCP deltas of
similar magnitude (the format offers no way to store `length/8`).

## 4. Why quantization loses to exact word-parallel LCP

The quantized search and the exact search run the *same* 8-byte compare loop;
they differ only at the mismatched word, where the exact version spends one
`XOR` + `TZCNT` to recover the last 0–7 matching bytes. That single
instruction pair buys back all of the compression ratio. Rounding down a
grid of k costs up to k−1 suffix bytes per value ((k−1)/2 expected), moves
them into the suffix stream, and slightly worsens the prefix-length stream.
Page compression (zstd/snappy) would claw some of that back, but there is no
speed left to pay for it with.

The one place a fixed grid could still pay off is decode-side: prefix lengths
guaranteed ≤N and word-aligned would allow a branchless fixed-width prefix
copy. But (a) a reader can never assume it (other writers produce exact
LCPs), so it needs a per-page "all lengths grid-aligned" check plus fallback,
and (b) the existing AVX2 decoder already gets the same effect by copying 32
bytes unconditionally with a rarely-taken branch for longer prefixes.
Velox-style in-place decode (retain the prefix in the output buffer, copy
only suffix bytes) is the stronger decode-side idea and needs no format
tricks.

## 5. Prototype on this branch

`encoding/delta/byte_array.go` now uses `wordSearchPrefixLength` (8-byte
words + `bits.TrailingZeros64`) for both `EncodeByteArray` and
`EncodeFixedLenByteArray`, replacing the byte loop and the
`binarySearchPrefixLength` path (which compared O(n log n) bytes via
`sort.Search` + `bytes.Equal`). `byte_array_prefix_bench_test.go` adds the
prefix-heavy benchmarks. Output is byte-identical to the previous encoder;
all tests pass with `-race`.

Bounds checks: the naive `Uint64(base[i:])` form carries 4 checks per
iteration. Reslicing both inputs to `n` and using the closed range
`base[i:i+8]` eliminates 3 of them, but one `IsSliceInBounds` on `base`
survives — prove tracks the `min` relation for one operand only. A fully
check-free shape that advances both slices by 8 each iteration measured ~10%
*slower* (two pointer+length updates per iteration cost more than the
residual predicted-not-taken check).

The final implementation casts both inputs to `[]uint64` with
`bitpack/unsafecast.Slice` (header reinterpretation, which also dodges
`checkptr` under `-race`; misaligned 8-byte loads are fine on amd64/arm64)
and indexes words: the hot loop is fully check-free, with only two per-call
`[:nw]` reslice checks left. Verified with `-gcflags='-d=ssa/check_bce'`.
Isolated cost vs the byte-indexed form: ~equal at ≤100B (values shorter than
8 bytes go straight to the byte tail and pay ~0.3 ns for the unused casts),
−19% at 1000B; end-to-end encode is unchanged to slightly better. Mismatch
location uses `TrailingZeros64` on little-endian and `LeadingZeros64` under
`cpu.IsBigEndian`, since the cast reads native-endian words.

Two more per-iteration checks were eliminated in the same pass: the
`_ = length.values[:len(prefix.values)]`-style hints in the encoder's copy
loop and the purego decoders did not actually work — for the encoder because
`values` is a heap-object field the compiler reloads after every `copy` call,
fixed by hoisting the slices into locals; for the decoders because prove
tracks the discarded-reslice fact for one operand only, fixed by assigning
the reslice back (`prefix = prefix[:len(suffix)]`). Decode gained ~3% on the
arm64 purego path. The remaining checks are load-bearing: the per-value
`src[baseOffset:endOffset]` slices and copy bounds are the only validation of
caller-provided offsets and of prefix/suffix lengths read from the page, and
`lastValue = dst[j:]` after appends is unprovable but negligible.

Possible follow-ups:

- Widen the compare to 16/32-byte blocks (`bytes.Equal` on chunks, or
  NEON/AVX2, or the Go 1.26 `simd` experiment) with a word-wise tail, as
  arrow-rs did — helps values with very long shared prefixes.
- Velox-style in-place decode to eliminate the prefix copy in
  `decodeByteArray`.
- Skip the copy when `p == 0` / `n == 0` in decoders (arrow#37873's
  observation).
