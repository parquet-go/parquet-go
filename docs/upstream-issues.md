# Draft upstream Go issues — archsimd codegen findings

Drafts for golang/go issues, from porting parquet-go's amd64 assembly kernels
to `simd/archsimd` (parquet-go/parquet-go#584, details in
docs/archsimd-port.md). All repros verified against go1.26.5 with
`GOEXPERIMENT=simd`, measurements on GCP c4-standard-8 (Intel Xeon Platinum
8581C, Emerald Rapids). NOT SUBMITTED — for review.

---

## Issue 1: cmd/compile: legacy SSE encodings emitted inside VEX/EVEX simd code cause AVX-SSE transition penalties

**Go version**: go1.26.5, `GOEXPERIMENT=simd`, GOARCH=amd64

### What did you do?

Compiled functions using `simd/archsimd` vector operations.

```go
package repro

import "simd/archsimd"

func ShiftImm(x archsimd.Uint32x8) archsimd.Uint32x8 {
	return x.ShiftAllRight(27)
}
```

### What did you see?

The scalar shift count is materialized into an XMM register with a
legacy-encoded (non-VEX) `MOVQ`:

```
MOVL $0x1b, AX
MOVQ AX, X1        ; 66 48 0f 6e c8   <- legacy SSE encoding, no VEX prefix
VPSRLD X1, Y0, Y0  ; c5 fd d2 c1      <- VEX
```

Executing a legacy SSE instruction while the upper halves of the YMM/ZMM
registers are dirty triggers an AVX-SSE state transition penalty on Intel
CPUs (Intel SDM / Optimization Reference Manual §"Mixing AVX and SSE code").
Because almost any surrounding archsimd code leaves uppers dirty, every call
through such a function pays the penalty.

Measured impact: a parquet bloom filter block insert using
`ShiftAllRight(27)` between 256-bit operations ran at 132ns/op instead of
2ns/op — **~65x slower** — with ~96% of samples on the two instructions
following the legacy `MOVQ`. Replacing the scalar-count shift with a
per-lane `ShiftRight` fed by a constant vector loaded from memory restored
the expected performance.

A second manifestation: storing a vector accumulator to a stack array for a
final reduction (`total.StoreSlice(totals[:])` with `var totals [8]uint64`)
emitted a legacy `MOVUPS X14, 0(R8)`. The per-call transition cost was
~155ns, observed in profiles as time attributed to the *first EVEX
instruction of the next call*. Rewriting the reduction with
`GetHi`/`GetLo`/`GetElem` (register-only) removed it.

### What did you expect to see?

Functions containing VEX/EVEX instructions should use VEX encodings for all
XMM/YMM operations the compiler itself emits (scalar-to-vector moves,
spills, stack copies, zeroing), or the compiler should manage the state with
VZEROUPPER at the boundaries. GCC/Clang emit `vmovq`/`vmovups` in AVX
functions for this reason.

---

## Issue 2: cmd/compile: simd intrinsic loads do not fold constant offsets into addressing modes

**Go version**: go1.26.5, `GOEXPERIMENT=simd`, GOARCH=amd64

### What did you do?

A loop loading four consecutive 64-byte vectors per iteration from a chunk:

```go
//go:noinline
func countLoop(chunks [][256]uint8, v archsimd.Uint8x64) int {
	c0, c1, c2, c3 := 0, 0, 0, 0
	for i := range chunks {
		c := &chunks[i]
		c0 += int(archsimd.LoadUint8x64Slice(c[0:64]).Equal(v).ToBits())
		c1 += int(archsimd.LoadUint8x64Slice(c[64:128]).Equal(v).ToBits())
		c2 += int(archsimd.LoadUint8x64Slice(c[128:192]).Equal(v).ToBits())
		c3 += int(archsimd.LoadUint8x64Slice(c[192:256]).Equal(v).ToBits())
	}
	return c0 + c1 + c2 + c3
}
```

### What did you see?

Seven `LEA`s per iteration materialize four separate base registers — the
common base `(AX)(R9*1)` is even recomputed four times — and every load uses
a zero displacement:

```
LEAQ (AX)(R9*1), R10
LEAQ (AX)(R9*1), R11
LEAQ 0x40(R11), R11
LEAQ (AX)(R9*1), R12
LEAQ 0x80(R12), R12
LEAQ (AX)(R9*1), R9
LEAQ 0xc0(R9), R9
VMOVDQU64 (R10), Z1
...
VMOVDQU64 (R11), Z1
...
VMOVDQU64 (R12), Z1
...
VMOVDQU64 (R9), Z1
```

### What did you expect to see?

One base register with folded displacements (EVEX disp8*N compression makes
them 1-byte):

```
VMOVDQU64 (R10), Z1
VMOVDQU64 0x40(R10), Z2
VMOVDQU64 0x80(R10), Z3
VMOVDQU64 0xc0(R10), Z4
```

Ordinary (non-intrinsic) Go memory operations get this folding. In an
issue-width-limited loop whose vector body is ~12 uops, the 7 extra LEAs
account for a measured ~15-20% throughput loss versus equivalent
hand-written assembly.

---

## Issue 3: cmd/compile: prove does not eliminate bounds checks for constant-offset indexes guarded by `i+4 <= len(s)`

**Go version**: go1.26.5 (plain Go, no GOEXPERIMENT needed)

### What did you do?

```go
func Sum4(words [][8]int32) (n int32) {
	for i := 0; i+4 <= len(words); i += 4 {
		n += words[i][0] + words[i+1][0] + words[i+2][0] + words[i+3][0]
	}
	return n
}
```

`go build -gcflags='-d=ssa/check_bce/debug=1'`

### What did you see?

All four index expressions keep their bounds checks:

```
./repro.go:22:13: Found IsInBounds
./repro.go:22:27: Found IsInBounds
./repro.go:22:43: Found IsInBounds
./repro.go:22:59: Found IsInBounds
```

In the compiled loop this is four `CMPQ`/`JBE` pairs plus the associated
address recomputation per iteration.

### What did you expect to see?

`i+4 <= len(words)` (with `i >= 0` from the induction variable) implies
`i+k < len(words)` for k in 0..3, so prove should eliminate all four checks.
This is the natural shape of any manually unrolled loop; the workaround we
found (convert to an array pointer per group: `c := (*[4][8]int32)(s)` and
index `c[0]..c[3]`) recovers the performance but is non-obvious.

Measured impact on an unrolled scan kernel: eliminating these checks via the
array-pointer workaround improved throughput ~26% (927ns → 654ns together
with the addressing effect above).

---

## Issue 4: cmd/compile: missed fusion — `x.Add(y.Masked(m))` should lower like `x.Add(y).Merge(x, m)` (merge-masked add)

**Go version**: go1.26.5, `GOEXPERIMENT=simd`, GOARCH=amd64

### What did you do?

The natural way to write "add 1 to lanes selected by a mask" (a byte
histogram/population count accumulator):

```go
acc = acc.Add(ones.Masked(mask))
```

### What did you see?

The zeroing `Masked` is lowered as a zero-masked broadcast, producing two
instructions with both compare and broadcast competing for the shuffle port
(port 5):

```
VPCMPEQB Z0, Z8, K1
VPBROADCASTB.Z X2, K1, Z8   // port 5
VPADDB Z7, Z8, Z7
```

In a loop with four such chains per iteration, port 5 serializes the loop:
measured 2.98µs for a 256KiB byte-count kernel.

Rewriting as the equivalent merge form:

```go
acc = acc.Add(ones).Merge(acc, mask)
```

lowers to a single merge-masked add on port 0:

```
VPCMPEQB Z0, Z8, K1
VPADDB Z4, Z7, K1, Z7
```

and the same kernel runs at 2.26µs (-24%).

### What did you expect to see?

Both forms compute the same value; the compiler already advertises pattern
fusion for masked operations (`archsimd` doc: "an Add operation followed by
Masked may be optimized to a masked add instruction"). The
`x.Add(y.Masked(m))` shape — arguably the more intuitive one — should
canonicalize to the merge-masked instruction as well.

---

## Issue 5 (lower confidence — possibly working as intended): cmd/compile: `s = s[N:]` loop advancement generates ~8 scalar ops

**Go version**: go1.26.5 (plain Go)

### What did you do?

The idiomatic vector-processing loop shape:

```go
for len(d) >= 256 {
	// ... process d[0:256] ...
	d = d[256:]
}
```

### What did you see?

Per iteration, the reslice compiles to dual len/cap updates plus a
branchless clamp of the pointer advance (the compiler avoids materializing a
past-the-end pointer when the resulting slice is empty):

```
ADDQ $-0x100, CX        // cap
MOVQ CX, R10
SARQ $0x3f, R10         // sign mask
ANDL $0x100, R10
ADDQ R10, AX            // clamped pointer advance
ADDQ $-0x100, BX        // len
CMPQ BX, $0x100
```

versus 3 instructions (`add`/`cmp`/`jcc`) for the equivalent assembly loop.
Measured ~15% on a byte-count kernel. Workaround: reinterpret as a slice of
chunk arrays and `range` over it, which compiles to a pointer increment.

### What did you expect to see?

The clamp exists for GC safety (no past-the-end pointers), which may make
this working-as-intended; filing to ask whether the loop optimizer could
strength-reduce this common shape — e.g. recognize that inside the loop body
`len(d) >= 256` holds, so the advanced pointer is always interior, and hoist
the clamp out of the loop or drop cap tracking when the cap result is only
used by the same pattern.
