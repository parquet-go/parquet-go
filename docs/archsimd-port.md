# Porting hand-written assembly to simd/archsimd

Inventory of every algorithm implemented in amd64 assembly in this repository,
categorized by feasibility of porting to Go 1.26's experimental `simd/archsimd`
package, and ordered from simplest to hardest.

## What archsimd gives us (Go 1.26, `GOEXPERIMENT=simd`)

- 128/256/512-bit vector types (`Int32x8`, `Uint8x64`, ...), mask types with
  `ToBits`/`FromBits`, masked `LoadMasked*`/`StoreMasked`, `LoadSlicePart`/
  `StoreSlicePart` for tails.
- Feature detection via `archsimd.X86.AVX2()`, `.AVX512()`, `.AVX512VBMI()`,
  `.AVX512GFNI()`, `.AVX512VAES()`, etc.
- Everything our SIMD kernels use for arithmetic/compare/permute is present:
  min/max (signed+unsigned, incl. 64-bit), 64-bit `Mul` (VPMULLQ, AVX512),
  rotates (VPROLQ), variable shifts (VPSLLVD/Q), two-source permutes
  (`ConcatPermute` ≈ VPERMI2D/Q), byte shuffles, blends (`Merge`),
  `Compress`/`Expand`, `OnesCount`, `LeadingZeros`, and AES rounds at all
  three widths (`AESEncryptOneRound`, AVXAES/VAES/AVX512VAES).
- **Missing:** gather (`VPGATHERDD/QQ`), scatter (`VPSCATTERDD/QQ`), conflict
  detection (`VPCONFLICT`), and scalar BMI2 bit-deposit/extract (`PDEP`/`PEXT`
  — also not in `math/bits`). Kernels built on these cannot be ported 1:1.

Caveats:

- archsimd is **amd64-only** in Go 1.26 and its docs explicitly say it is not
  portable. The immediate win is eliminating `.s` files, explicit (correct)
  feature gating, and code the compiler can inline/instrument; arm64 would
  come with a future Go release or the planned portable `simd` package.
- `GOEXPERIMENT=simd` must be set by the **final consumer's build**, which a
  library cannot require. Ported files need `//go:build goexperiment.simd`
  and the purego fallbacks stay. This branch is the exploration vehicle.
- Local testing on darwin/arm64 requires an amd64 machine or CI runner
  (Rosetta 2 does not expose AVX2/AVX-512 to macOS binaries).

## Tier 1 — trivial ports (start here)

Small, straight-line kernels; every instruction maps directly.

| Kernel | File | Today | archsimd mapping |
|---|---|---|---|
| `bytealg.Broadcast` | internal/bytealg/broadcast_amd64.s | AVX2 byte memset | `BroadcastUint8x32` + `StoreSlice`/`StoreSlicePart` (or drop asm: plain Go loop is memset-recognized) |
| `broadcastRangeInt32` | column_buffer_amd64.s | AVX2 iota ramp | broadcast + `Add`; **port fixes a real bug** (scalar tail computes `base*(i+1)` instead of `base+i`, masked by tests that only use base=1) |
| `memsetValues` | value_amd64.s | AVX2 24-byte pattern fill | 3× `VPERMQ` trick → `PermuteScalars`, or plain Go doubling copy |
| `encodeInt32IndexEqual8Contiguous{AVX2,SSE}` | encoding/rle/rle_amd64.s | broadcast+compare+movemask | `Broadcast` + `Equal` + `Mask.ToBits` |
| `bytealg.Count` | internal/bytealg/count_amd64.s | AVX2 + AVX-512BW byte count | `Equal` → `ToBits` → `bits.OnesCount64`; cleanest possible demo of the mask API |
| `blockInsert` / `blockCheck` | bloom/block_amd64.s | AVX2 split-block bloom | `BroadcastUint32x8`, `Mul` (salts), `ShiftRight`, `ShiftLeft` (VPSLLVD), `Or`; check = `And`+`Equal` all-lanes (replaces VPTEST) |

## Tier 2 — straightforward, mostly volume

Loop/dispatch logic is bigger than the SIMD math; all ops available.

| Kernel | File | Notes |
|---|---|---|
| `filterInsert` / `filterCheck` / `filterInsertBulk` | bloom/filter_amd64.s | Same mask math as Tier 1 bloom; bulk variant's "manual scatter" (VEXTRACTI128+VPEXTRQ) becomes `GetElem` + scalar RMW, same structure |
| `min*`/`max*`/`combinedBounds*` (numeric, 19 fns) | page_min/page_max/page_bounds_amd64.s | `Min`/`Max` + `GetHi`/`GetLo` reduction. One Go generic over 6 types replaces ~1500 lines of asm; biggest deletion win in the repo. Watch float NaN semantics (VMINPS propagates 2nd operand; page_bounds_nan_test.go covers it) |
| `orderOf{Int,Uint,Float}{32,64}` (6 fns) | order_amd64.s | asm uses VPERMI2 to build the shifted-by-one vector; in Go just load `data[i:]` and `data[i+1:]` — two unaligned loads, no permute. Compare → `Mask.ToBits` all-true. Also fixes ungated AVX512DQ use (KXORB/KORTESTB) in the 64-bit variants |
| `multiProbe32AVX2` / `multiProbe64AVX2` / `multiProbe128SSE2` | hashprobe/hashprobe_amd64.s | broadcast+compare+`ToBits`, then `bits.TrailingZeros`/`OnesCount` (makes today's implicit POPCNT/BMI1 assumption explicit) |
| `encodeByteArrayLengths` / `decodeByteArrayLengths` | encoding/delta/length_byte_array_amd64.s | adjacent diff = `Sub` of two offset loads; prefix sum = shift-and-add ladder (`ConcatShiftBytesRight` or permutes) |
| `Hash32/64/128`, `MultiHash*` | hashprobe/aeshash/aeshash_amd64.s | `AESEncryptOneRound` is a direct fit (gate on `X86.AVXAES()`); stretch: widen to 2–4 hashes/iter with VAES. Note purego fallback currently panics — port removes that gap |

## Tier 3 — real SIMD work

Intricate lane choreography, but every instruction has an archsimd equivalent.

| Kernel | File | Notes |
|---|---|---|
| `blockDelta/blockMin/blockSub/blockBitWidths` + `decodeBlock` (int32+int64) | encoding/delta/binary_packed_amd64.s | rotate-by-one via `Permute`; prefix-sum ladders; AVX2's hand-rolled vpminsq/vpmaxsq macros collapse to native 64-bit `Min`/`Max`; scalar LZCNT → `bits.LeadingZeros` |
| `MultiSum64Uint{8,16,32,64,128}` | bloom/xxhash/sum64uint_amd64.s | needs 64-bit `Mul` (AVX512), `RotateAllLeft`, `ExtendLo*` widening loads; the 128-bit variant's VPERMI2Q deinterleave → `ConcatPermute`. Gate becomes an honest `X86.AVX512()` (today's gate checks CD but uses DQ's VPMULLQ) |
| `encodeMiniBlockInt32x3to16bitsAVX2` + `encodeInt32Bitpack1to16bitsAVX2` | delta + rle | the general bit-packers: variable shifts (`ShiftLeft/Right` per-lane), `PermuteScalars`, blends, cross-boundary realign. Hardest arithmetic in the portable set. The x1bit specializations become one-liners (`ToBits` IS the 1-bit pack) |
| `validatePrefixAndSuffixLengthValuesAVX2` + `decodeByteArray*` | encoding/delta/byte_array_amd64.s | validation is portable (rotate+compare+movemask); the decode over-copy tricks are mostly `copy` logic — may end up plain Go |
| `minBE128` / `maxBE128` | page_min/page_max_amd64.s | lexicographic u128 min/max with index tracking: byte-swap `Permute`, paired `VPCMPUQ` → mask bit-fixup → `Merge`. Hardest portable kernel; also fixes ungated AVX512BW use (VPSHUFB on ZMM) |

## Tier 4 — blocked: needs instructions archsimd doesn't expose

| Kernel | File | Blocker | Possible redesign |
|---|---|---|---|
| `gatherBits`, `gather32`, `gather64` | sparse/gather_amd64.s | VPGATHERDD/QQ over strided memory | none faithful; scalar strided loads + `SetElem` insert loses the point. Keep asm or wait for gather in archsimd |
| `nullIndex32` / `nullIndex64` | null_amd64.s | VPGATHERDD/QQ (strided sparse.Array) | same as above; when stride == elem size a contiguous compare+`ToBits` version is portable and covers the common dense case |
| `dictionaryBounds{Int32,Int64,Uint32,Uint64,Float32,Float64}` | dictionary_amd64.s | AVX-512 k-masked gathers | none without gather |
| `dictionaryLookup32` / `dictionaryLookup64` | dictionary_amd64.s | gather **and** the tree's only scatters (VPSCATTERDD/DQ) | none without gather+scatter |
| `encodeFloat/encodeDouble/decodeFloat/decodeDouble` | encoding/bytestreamsplit | gather+scatter used to transpose | redesignable: BYTE_STREAM_SPLIT is an N×4/N×8 byte transpose, expressible as load + `Permute`/`Interleave` network with no memory gather. Blocked as-written, portable with redesign |
| `encodeBytesBitpackBMI2` / `decodeBytesBitpackBMI2` | encoding/rle/rle_amd64.s | scalar PDEP/PEXT (no Go intrinsic) | bitWidth ≤ 8: plain Go shift loop is decent; SIMD alternative would want VPMULTISHIFTQB/GFNI, not exposed |
| `encodeMiniBlockInt32x2bitsAVX2` / `Int64x2bitsAVX2` | encoding/delta/binary_packed_amd64.s | PDEPQ bit-plane interleave | two `ToBits` planes + scalar interleave, or fold into the general 3-16-bit path |

## Tier 5 — scalar assembly: replace with plain Go, not archsimd

No SIMD content; the Go compiler generates comparable code. Deleting these is
pure maintenance win, independent of archsimd.

- `wyhash.MultiHash*` (MULQ pairs → `bits.Mul64`; purego version already exists)
- `xxhash.Sum64` (vendored cespare scalar asm; purego version exists)
- `nullIndex8`, `nullIndex128` (also fixes ungated SSE4.1 PCMPEQQ), `writePointersBE128`
- `dictionaryBoundsBE128` (BSWAPQ loop)
- `decodeByteArrayOffsets`, `encodeMiniBlock*Default`, `decodeBlock*Default`
- `gather128`, `gatherBitsDefault`

## Dead code found during the audit (delete regardless)

- `dictionaryLookupByteArrayString` / `dictionaryLookupFixedLenByteArray{String,Pointer}`
  — calls commented out since segmentio/parquet-go#368 (GC race).
- Five `//go:noescape` declarations in binary_packed_amd64.go with **no assembly
  body** (`decodeMiniBlockInt32Default`, `...x1to16bitsAVX2`, `...x17to26bitsAVX2`,
  `...x27to31bitsAVX2`, `decodeMiniBlockInt64Default`) — callers are dead; the
  live path uses the external `bitpack` module.
- `combinedBoundsBool` / `combinedBoundsBE128` declared in page_bounds_amd64.go
  with no body and no callers.

## Incidental bugs / gate mismatches surfaced

1. **`broadcastRangeInt32AVX2` scalar tail is wrong** (`base*(i+1)` instead of
   `base+i`); only correct for base==1, which is all the test covers.
   `column_buffer_optional.go:247` can hit it with other bases.
2. Ungated feature use (safe on real CPUs, but worth making explicit — an
   archsimd port does so automatically): LZCNT + PDEP under AVX2-only gates
   (delta), POPCNT/TZCNT under AVX2 gate (hashprobe, bytealg), AVX512DQ
   KXORB/KORTESTB under F+VL gate (orderOf*64), AVX512BW VPSHUFB-on-ZMM under
   F+DQ gate (min/maxBE128), VPMULLQ (DQ) under F+VL+VBMI gate
   (bytestreamsplit) and under an F+CD gate (xxhash).
3. `combinedBoundsInt64AVX512` has no in-assembly length guard and over-reads
   if called with len < 32 (Go-side gate is the only protection).

## Status

Strategy: purely additive. New `*_simd_amd64.go` files are gated on
`//go:build !purego && goexperiment.simd`; the existing asm-backed files got
`&& !goexperiment.simd` added to their build tags so nothing is deleted and
the default build is bit-for-bit unchanged.

Done (Tier 1, 2026-08-10):

- `internal/bytealg`: `Count` (AVX2 + AVX-512 mask paths), `Broadcast` —
  bytealg_simd_amd64.go
- `bloom`: `Block.Insert`/`Check`, `filterInsert`/`filterCheck`/
  `filterInsertBulk` — bloom_simd_amd64.go (bulk uses a scalar-hash loop for
  now; the asm's 4-way vectorized fasthash is a follow-up)
- root: `broadcastRangeInt32` (fixes the scalar-tail bug), `memsetValues`,
  plus `broadcastValueInt32`/`writePointersBE128` carried over —
  column_buffer_simd_amd64.go, value_simd_amd64.go, tests in
  simd_amd64_test.go
- `encoding/rle`: run detector `encodeInt32IndexEqual8ContiguousSIMD`,
  injected through the existing function-pointer dispatch by a later-ordered
  init — rle_simd_amd64.go (no tag changes needed; the blocked BMI2 kernels
  keep their assembly)

Validation: both GOARCH=amd64 builds (with and without GOEXPERIMENT=simd)
compile; affected package tests pass in both modes, run under Rosetta 2 which
exposes AVX2 (not AVX-512), so the AVX2 paths execute for real on the arm64
dev machine. First benchmark signal (Rosetta, indicative only): archsimd
bloom block ops ~0.6x asm throughput — needs profiling on native x86.

## Suggested execution order

1. Tier 1 kernels one PR at a time, benchmarked against asm (`bytealg.Count`
   or bloom block first — smallest surface, existing benchmarks).
2. Tier 5 deletions + dead-code removal in parallel (no archsimd needed).
3. Tier 2, leading with page min/max/bounds (one generic replaces 19 functions).
4. Tier 3 as appetite allows.
5. Revisit Tier 4 when archsimd grows gather/scatter (tracked upstream in the
   simd proposal), or attempt the bytestreamsplit transpose redesign.
