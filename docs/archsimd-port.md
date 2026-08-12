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
- **memsetValues is deliberately NOT SIMD, and its assembly was removed
  entirely** (the one deliberate exception to the no-deletion rule, decided
  after review): Value contains a pointer, and the assembly stored it with
  raw vector writes that bypass GC write barriers — a latent use-after-free
  hazard if the pointee's only other reference goes away during a concurrent
  mark. All builds now share a single portable doubling-`copy` implementation
  in value.go (typedslicecopy = one bulk barrier pass + memmove, ~65GB/s);
  value_amd64.{go,s}, value_simd_amd64.go and values_purego.go are gone.
  The same GC-safety audit should be applied to any other kernel writing
  pointer-bearing memory from assembly.
- `encoding/rle`: run detector `encodeInt32IndexEqual8ContiguousSIMD`,
  injected through the existing function-pointer dispatch by a later-ordered
  init — rle_simd_amd64.go (no tag changes needed; the blocked BMI2 kernels
  keep their assembly)

Validation: both GOARCH=amd64 builds (with and without GOEXPERIMENT=simd)
compile; affected package tests pass in both modes, run under Rosetta 2 which
exposes AVX2 (not AVX-512), so the AVX2 paths execute for real on the arm64
dev machine. All tests also pass on native x86 with full AVX-512
(GCP c4-standard-8, Xeon Platinum 8581C Emerald Rapids), which exercises the
AVX-512 Count path that Rosetta cannot.

### Benchmarks: assembly vs archsimd (c4-standard-8, Emerald Rapids, n=10)

| Benchmark | asm | archsimd | delta |
|---|---|---|---|
| BlockInsert | 1.96ns | 2.10ns | +7% |
| BlockCheck | 1.80ns | 1.97ns | +9% |
| FilterInsert | 2.40ns | 2.72ns | +14% |
| FilterCheck | 2.24ns | 2.56ns | +14% |
| FilterInsertBulk | 16.6ns | 19.6ns | +19% |
| Broadcast/100B | 2.22ns | 2.39ns | +8% |
| Broadcast/10KB | 55.6ns | 106.5ns | +92% |
| Count/256KiB | 1.60µs | 2.99µs | +87% |
| rle run detector | 429ns | 927ns | +116% |

### GOAMD64 matters as much as the code

The table above was measured at the default GOAMD64=v1. At that level,
`math/bits.OnesCount*` is not a bare POPCNT: the compiler emits a load of
`runtime.x86HasPOPCNT`, a test-and-branch, and keeps a fallback CALL to
`math/bits.OnesCount64` in the loop body — which forces register
spills/reloads around every popcount. Rebuilding both modes with
**GOAMD64=v4** (Emerald Rapids, n=10):

| Benchmark | asm v4 | archsimd v4 | delta | (delta at v1) |
|---|---|---|---|---|
| BlockInsert | 1.80ns | 1.81ns | +0.7% | +7% |
| BlockCheck | 1.65ns | 1.40ns | **-15.5%** | +9% |
| FilterInsert | 2.22ns | 2.45ns | +10.5% | +14% |
| FilterCheck | 2.08ns | 2.20ns | +5.8% | +14% |
| FilterInsertBulk | 15.9ns | 16.8ns | +5.9% | +19% |
| Count/256KiB | 1.68µs | 1.98µs | +17.8% | +87% |
| Broadcast/10KB | 47.8ns | 84.9ns | +77% | +92% |
| RLE detector | 381ns | 779ns | +104% | +116% |

Bloom reaches parity (geomean +1%, BlockCheck faster than asm) and Count's
gap collapses to +18%. Broadcast and the RLE detector don't use popcount and
keep their gap: it comes from per-iteration bounds checks and slice-header
updates around loops whose body is 1-2 vector instructions. Conclusion:
benchmark and ship the GOEXPERIMENT=simd path with GOAMD64=v3/v4.

### Bounds-check elimination and amortization round

Two composable techniques closed most of the remaining gap (numbers at
GOAMD64=v4, same boot as their asm baseline — note the VM lands on different
physical hosts across stop/start cycles, so only same-boot ratios are valid):

- **Array-pointer chunking**: `c := (*[256]uint8)(d)` carries one provable
  check per chunk; constant-bounds subslices (`c[64:128]`) then compile with
  zero checks (verified with `-d=ssa/check_bce/debug=1`). This also turns
  indexed addressing into constant offsets. Needed because the prove pass
  does NOT derive `words[n+3]` safety from `n+4 <= len(words)` — the naive
  unroll kept 4 bounds checks + 4 shift/add address computations per
  iteration.
- **Wider iterations**: Broadcast 256B/iter, Count 256B (AVX-512) and 128B
  (AVX2) per iter, rle detector 4 groups/iter with a single-branch
  "any uniform" test (`(e+1)&0x100` carry trick — note the naive
  `e0&e1&e2&e3` test is WRONG, it loses uniform groups).
- **Rotate-compare** in the rle detector: uniform ⇔ `w == w.Permute(rot1)`,
  replacing the memory→GPR→vector broadcast of the first element.

| Benchmark | asm v4 | archsimd v4 | delta |
|---|---|---|---|
| Broadcast/1KB | 12.1ns | 7.9ns | **-35%** |
| Broadcast/10KB | 69.8ns | 53.8ns | **-23%** |
| Broadcast/100B | 2.44ns | 2.78ns | +14% |
| Count/2MiB | 20.2µs | 19.5µs | **-4%** |
| Count/4KiB | 25.5ns | 29.4ns | +15% |
| Count/256KiB | 1.81µs | 2.45µs | +36% |
| rle detector | 476ns | 654ns | +37% |

Broadcast now beats the assembly at large sizes and Count matches it in the
memory-bound regime. The asm's 8-byte splat trick for small sizes ports to
pure Go directly (`0x0101010101010101 * uint64(src)` + `PutUint64` stores
with an overlapping tail): Broadcast/size=10 went from 7.0ns to 2.6ns, vs
1.9ns for asm — the remaining ~0.7ns is dispatch/prologue overhead.

Count deep-dive: `d = d[256:]` loop control costs ~8 scalar ops per
iteration (dual len/cap updates plus a branchless clamp of the pointer
advance — Go refuses to materialize a past-the-end pointer for an empty
result). Ranging over `unsafecast.Slice[[256]uint8](d)` reduces control to
inc/cmp/branch and took 256KiB from +36% to +21%.

Count round 2 — vector accumulation. Replacing the kmov+popcnt+add scalar
chain with byte accumulators flushed through SumAbsDiff (VPSADBW) beat the
popcount version by 10-20%, after two lowering traps were profiled out:

1. `ones.Masked(mask)` lowers to a masked VPBROADCASTB — a shuffle-port uop
   that pairs with every compare and serializes the loop on port 5. The
   `a.Add(ones).Merge(a, mask)` form lowers to a single merge-masked VPADDB
   on port 0 instead.
2. Storing the vector total to a stack array for the final reduction emits a
   legacy (non-VEX) MOVUPS — the third sighting of the AVX/SSE transition
   penalty, costing ~155ns per call (4KiB was +600% until fixed). Reduce in
   registers with GetHi/GetLo/GetElem instead.

Final same-boot standings vs asm at v4: +13% (4KiB), +14% (256KiB), +41%
(2MB — only visible on hosts with very high memory bandwidth; the earlier
"parity at 2MB" was a slow-host artifact, the Go loop runs ~6.4 cycles per
256B chunk vs the asm's ~4.3). The residual is the addressing-mode-folding
compiler gap: four lea/mov per iteration to materialize load addresses.
Report upstream with the ShiftAll bug.

### Constant CPU feature checks (custom toolchain experiment, 2026-08-11)

Tested the `constant-amd64-checks` branch of github.com/achille-roussel/go
(go1.28-devel), which makes `archsimd.X86.AVX2()`/`AVX512()` return
build-tag constants under GOAMD64=v3/v4 so guards fold and fallback paths
DCE. Verified in the binary: Count went from 3 `cmpb (%rip)` feature guards
to 0, and the entire AVX2 case was eliminated at v4. Same-boot results
(GOAMD64=v4, GOEXPERIMENT=simd; asm-under-both-toolchains as drift control,
which measured ±0.2%):

- bloom geomean **-12%** (BlockCheck -35%, FilterCheck -11%); bloom vs asm
  reaches **overall parity (+0.5% geomean)**
- rle detector -8.7% (gap vs asm narrows to +25%)
- Broadcast dispatch-dominated sizes -22%
- Count ±0.5% (its single per-call guard was already negligible; the
  addressing-fold gap keeps it at +13-14% vs asm)

Caveat: master's archsimd renamed APIs since 1.26.5 — `Load*Slice`→`Load*`,
`StoreSlice`→`Store`, `SumAbsDiff`→`SumOf8AbsDiff` (returns Uint64x8
directly) — the branch will need those renames when it targets a newer Go.

### AVX-512 tiers for the delta length kernels

The assembly stayed SSE2 ("keeps the code simple... already yields most of
the performance"); in Go a wider tier is the same code at different type
widths, so both kernels gained AVX-512 tiers (16 lanes vs the asm's 4) —
and the 512-bit tier is simpler than the AVX2 one because Mask32x16FromBits
is legal under the AVX-512 gate. Results vs the SSE2 asm (Emerald Rapids,
v4): encode **-27%**, decode **+7%**. Two lessons:

- Chunk views also solve *shifted* streams: the encode loads are offset by
  one element, so one chunk view over `offsets` and one over `offsets[1:]`
  make both loads constant-length chunk accesses. Views must be clamped to
  a common length (`[:n]`) for prove to elide cross-view indexing checks.
- **Prefix-sum carries must stay in vector registers**: extracting the
  running total to a GPR and re-broadcasting per chunk serialized the loop
  (decode was +31% before the fix); a vector-resident carry (all lanes
  equal, updated by one add from a last-lane permute — the asm's PSHUFD
  trick) leaves a single add on the critical path.

### Optimistic float bounds (NaN witness)

The +36% float bounds gap at cache-resident sizes was the cost of NaN-safe
compare-and-merge (2 uops per update vs VMINPS's 1), forced because
archsimd's float Min/Max NaN semantics are undocumented and the compiler
canonicalizes commutative operands (so the assembly's operand-order trick is
inexpressible). The fix: scan optimistically with native Min/Max while
accumulating a sum of every loaded vector — addition propagates NaN
unconditionally, unlike VMINPS which can erase one, so a NaN-free sum proves
the fast result exact; NaN (or a spurious +Inf/-Inf sum) triggers a rescan
with the compare-and-merge fallback. Results: 4KiB +36% → +24%, 256KiB -19%,
2MB -30..35% vs asm. The remaining 4KiB delta is structural (6 vector ops
per chunk vs the asm's 4 — the two NaN-witness adds); only upstream changes
(documented Min/Max NaN semantics, or not canonicalizing FP min/max
operands, which is semantics-changing under NaN) can close it.

Two more #80835 manifestations found here: BroadcastFloat32x16(0)
materializes the float constant with a legacy XORPS (fixed by building the
zero via an integer broadcast), and the template's single-accumulator
Min chains serialized on latency (restored dual accumulators).

Reference point — the standard library: `bytes.Count` (what the purego build
uses, backed by the stdlib's AVX2 assembly) is far slower than both
(same boot, GOAMD64=v4, 256KiB: repo asm 1.63µs < archsimd 2.06µs <
bytes.Count 3.04µs; archsimd beats the stdlib by 30-50% at every size). So
even compiler-limited, the archsimd Count is a strong upgrade over falling
back to the standard library.

Still open: rle detector's last +37% (movemask-to-GPR transfers per group
are the suspect).

Lessons from the tuning rounds (all confirmed by pprof + objdump on the VM):

1. **Avoid `ShiftAll*` with a scalar count.** Go 1.26 materializes the count
   with a legacy (non-VEX) `MOVQ`, and mixing legacy SSE into 256-bit VEX
   code triggers an AVX/SSE state-transition penalty on every call — the
   bloom kernels were **65x slower** until switched to the per-lane
   `ShiftLeft`/`ShiftRight` with a constant vector loaded from memory.
   Worth reporting upstream to the Go project.
2. **Iterate with shrinking slices** (`d = d[256:]` with constant-offset
   loads) instead of `Load...Slice(data[i:])` — re-sliced loads rebuild
   slice headers and bounds logic per iteration (~2x on Count).
3. **Don't round-trip vector lanes through stack arrays** in hot loops;
   store-to-load forwarding stalls made a "vectorized" 4-way hash slower
   than the scalar one.
4. Remaining gaps (Count ~1.9x, Broadcast/10KB ~1.9x, rle detector ~2.2x)
   are scalar loop-overhead the compiler doesn't yet eliminate around the
   intrinsics — acceptable for an experiment, revisit as the toolchain
   matures.

Benchmark infra: GCP instance `parquet-archsimd-bench` (c4-standard-8,
us-central1-b, project achille-demo-test), currently **stopped** — restart
with `gcloud compute instances start parquet-archsimd-bench
--project=achille-demo-test --zone=us-central1-b`; it has Go 1.26.5 in
/usr/local/go and the repo cloned at ~/parquet-go.

### Tier 2 progress (branch archsimd-tier2)

- **page min/max/bounds (19 kernels)**: done — one generated Go file
  (page_minmax_simd_amd64.go) replaces ~1800 lines of assembly across three
  .s files. New capability: AVX2 tiers (the assembly was AVX-512-only, so
  AVX2 CPUs ran scalar). Two lowering rules learned:
  1. float Min/Max must use compare-and-merge — archsimd NaN semantics are
     undocumented and the compiler canonicalizes commutative operands, so
     the assembly's operand-order trick is not expressible;
  2. 64-bit integer Min/Max at the AVX2 tier must use compare-and-merge —
     Int64x4.Min lowers to VPMINSQ (AVX-512-only) and SIGILLs on AVX2 CPUs;
     VPCMPGTQ+blend instead, with a 1<<63 bias for unsigned.
  The lowercase page kernels ignore interior NaN on amd64 (asm and simd)
  but propagate NaN in purego (slices.Min) — pre-existing divergence,
  documented in page_bounds_vector_nan_test.go.
- **orderOf* (6 kernels)**: done — shifted-pair vector compares with AVX2
  tiers (new vs the AVX-512-only asm); floats report undefined order on NaN
  (matches asm; purego generic differs).
- **delta length_byte_array (2 kernels)**: done — AVX2 (asm was SSE2-only);
  the decode prefix sum uses a Permute shift-and-add ladder with
  compare-built lane masks: **Mask32x8FromBits lowers to KMOVD
  (AVX-512-only) and faults on AVX2 CPUs** — second sighting of the
  compiles-at-AVX2-width-but-needs-AVX-512 trap after Int64x4.Min.
- **hashprobe multiProbe (3 kernels)**: done — broadcast + group compare +
  occupancy-filtered mask, mirroring the asm structure.
- **aeshash (6 functions)**: done — bit-identical to the assembly (golden
  tests pass unchanged); gate is AVXAES (VEX encoding needs AVX), and the
  purego+simd build gains a working AES hash where the stub panicked.

Tier 2 benchmarked on Emerald Rapids (GOAMD64=v4, same-boot vs asm, n=8).
The first run exposed four issues, all diagnosed with pprof + objdump and
fixed; final standings:

| Kernel group | vs assembly |
|---|---|
| bounds int/uint (all sizes) | -28% .. +6% (mostly faster than asm) |
| bounds float 256KiB/2MB | -9% .. -34% (faster than asm) |
| bounds float 4KiB | +24% (optimistic NaN-witness path; structural floor, see below) |
| orderOf* | +109% .. +217% (known gap, see below) |
| hash tables 32/64 | +12% .. +83% (hash-bound) |
| hash table 128 | +8% .. +70% |
| MultiHash64 | +48% |

New lessons (beyond tier 1):

1. **Call ClearAVXUpperBits() before returning from vector code.** The asm
   ends with VZEROUPPER; without it the CALLER's scalar float code pays the
   AVX-SSE transition penalty on every call (float bounds 4KiB was +2091%
   from this + scalar reductions). The compiler does not insert it.
2. **Never round-trip vector values through stack arrays or [N]byte copies**:
   two narrow stores followed by a wide load defeat store forwarding and
   serialize loops (aeshash was 16x slower). Build vectors with SetElem
   (register-only) or load directly from source memory (UnsafeArray for
   sparse); note BroadcastUint64x2 is VPBROADCASTQ = AVX2, SetElem from a
   zero value is plain AVX.
3. **Scalar float compares (UCOMISS) and vector spills (MOVUPS) are legacy
   SSE encodings** — more manifestations of golang/go#80835. Float
   reductions must be in-register shuffle ladders; if the compiler spills a
   vector held across a loop (multiProbe128), consider not using a vector at
   all (two GPR compares beat a spilled vector compare).
4. Min/max overlap-tail trick: reload full vectors overlapping processed
   elements instead of scalar tails (idempotent ops), eliminating both tail
   scalar float compares and tail loops.

orderOf* follow-up: the two-overlapping-loads scan was ~2-2.5x slower than
the asm; rewriting the AVX-512 tier as a rolling single-load scan with
ConcatPermute (the asm's VPERMI2D trick, trailing pairs covered by one or
two overlapping window compares) brought it to **-13.5%..+21%** — Int64 and
Float64 beat the asm at most sizes. Correctness pinned by an exhaustive
violation-at-every-position test (order_simd_test.go) run on AVX-512
hardware. MultiHash64 follow-up: a VAES fast path (gated on X86.VAES, dense input
detected via pointer stride) hashes 4 values per iteration — 256-bit AES
rounds encrypt two blocks per instruction, with interleaves packing
[seed, value] block pairs and reassembling the hashes for a single store.
Result: **+21% throughput over the assembly** (11.9 -> 14.5 GiB/s), hash
values bit-identical (golden tests on VAES hardware). The same treatment
would fit MultiHash32/MultiHash128, and an AVX512VAES variant could do 4
blocks per instruction. The probe gap was then closed
in three steps: pre-slicing values to hoist its bounds check, testing
group fullness with >= so prove can elide the group insert bounds checks
(it cannot see through OnesCount32 + ==), and reading densely packed keys
through a plain slice instead of the strided Index (whose per-key multiply
and register pressure spilled slice headers into the probe loop). With
VAES MultiHash32 added (the 32-bit state is the 64-bit one with a
VPMOVZXDQ-widened value), the hash table benchmarks landed at **+7..+34%
vs assembly** (from +65..+83%), with the remaining delta in the
insert-heavy portion of the benchmark.

## Suggested execution order

1. Tier 1 kernels one PR at a time, benchmarked against asm (`bytealg.Count`
   or bloom block first — smallest surface, existing benchmarks).
2. Tier 5 deletions + dead-code removal in parallel (no archsimd needed).
3. Tier 2, leading with page min/max/bounds (one generic replaces 19 functions).
4. Tier 3 as appetite allows.
5. Revisit Tier 4 when archsimd grows gather/scatter (tracked upstream in the
   simd proposal), or attempt the bytestreamsplit transpose redesign.
