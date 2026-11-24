//go:build !purego && (!goexperiment.simd || go1.27)

#include "textflag.h"

#define errnoIndexOutOfBounds 1

// func dictionaryBoundsInt32(dict []int32, indexes []int32) (min, max int32, err errno)
TEXT ·dictionaryBoundsInt32(SB), NOSPLIT, $0-64
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    XORQ R10, R10 // min
    XORQ R11, R11 // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), R10
    MOVL R10, R11

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2  // [len(dict)...]
    VPBROADCASTD R10, Y3 // [min...]
    VMOVDQU32 Y3, Y4     // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDD (AX)(Y0*4), K1, Y1
    VPMINSD Y1, Y3, Y3
    VPMAXSD Y1, Y4, Y4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERM2I128 $1, Y3, Y3, Y0
    VPERM2I128 $1, Y4, Y4, Y1
    VPMINSD Y0, Y3, Y3
    VPMAXSD Y1, Y4, Y4

    VPSHUFD $0b1110, Y3, Y0
    VPSHUFD $0b1110, Y4, Y1
    VPMINSD Y0, Y3, Y3
    VPMAXSD Y1, Y4, Y4

    VPSHUFD $1, Y3, Y0
    VPSHUFD $1, Y4, Y1
    VPMINSD Y0, Y3, Y3
    VPMAXSD Y1, Y4, Y4

    MOVQ X3, R10
    MOVQ X4, R11
    ANDQ $0xFFFFFFFF, R10
    ANDQ $0xFFFFFFFF, R11

    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    CMPL DI, R10
    CMOVLLT DI, R10
    CMPL DI, R11
    CMOVLGT DI, R11
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVL R10, min+48(FP)
    MOVL R11, max+52(FP)
    MOVQ R12, err+56(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return

// func dictionaryBoundsInt64(dict []int64, indexes []int32) (min, max int64, err errno)
TEXT ·dictionaryBoundsInt64(SB), NOSPLIT, $0-72
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    XORQ R10, R10 // min
    XORQ R11, R11 // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), R10
    MOVQ R10, R11

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2  // [len(dict)...]
    VPBROADCASTQ R10, Z3 // [min...]
    VMOVDQU64 Z3, Z4     // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDQ (AX)(Y0*8), K1, Z1
    VPMINSQ Z1, Z3, Z3
    VPMAXSQ Z1, Z4, Z4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERMQ $0b1110, Z3, Z0
    VPERMQ $0b1110, Z4, Z1
    VPMINSQ Z0, Z3, Z3
    VPMAXSQ Z1, Z4, Z4

    VPERMQ $1, Z3, Z0
    VPERMQ $1, Z4, Z1
    VPMINSQ Z0, Z3, Z3
    VPMAXSQ Z1, Z4, Z4

    VSHUFF64X2 $2, Z3, Z3, Z0
    VSHUFF64X2 $2, Z4, Z4, Z1
    VPMINSQ Z0, Z3, Z3
    VPMAXSQ Z1, Z4, Z4

    MOVQ X3, R10
    MOVQ X4, R11

    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), DI
    CMPQ DI, R10
    CMOVQLT DI, R10
    CMPQ DI, R11
    CMOVQGT DI, R11
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVQ R10, min+48(FP)
    MOVQ R11, max+56(FP)
    MOVQ R12, err+64(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return

// func dictionaryBoundsFloat32(dict []float32, indexes []int32) (min, max float32, err errno)
TEXT ·dictionaryBoundsFloat32(SB), NOSPLIT, $0-64
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    PXOR X3, X3   // min
    PXOR X4, X4   // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSS (AX)(DI*4), X3
    MOVAPS X3, X4

    UCOMISS X3, X3
    JP findNonNaN

    CMPQ DX, $8
    JB initSSE2

    CMPB ·hasAVX512VL(SB), $0
    JE initSSE2

    MOVQ DX, DI
    ANDQ $-8, DI
initAVX512:
    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2 // [len(dict)...]
    VPBROADCASTD X3, Y3 // [min...]
    VMOVDQU32 Y3, Y4    // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDD (AX)(Y0*4), K1, Y1
    // Keep non-NaN accumulators in SRC2 so NaN lanes preserve Y3 and Y4.
    VMINPS Y3, Y1, Y3
    VMAXPS Y4, Y1, Y4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERM2I128 $1, Y3, Y3, Y0
    VPERM2I128 $1, Y4, Y4, Y1
    VMINPS Y0, Y3, Y3
    VMAXPS Y1, Y4, Y4

    VPSHUFD $0b1110, Y3, Y0
    VPSHUFD $0b1110, Y4, Y1
    VMINPS Y0, Y3, Y3
    VMAXPS Y1, Y4, Y4

    VPSHUFD $1, Y3, Y0
    VPSHUFD $1, Y4, Y1
    VMINPS Y0, Y3, Y3
    VMAXPS Y1, Y4, Y4

    VZEROUPPER
    JMP test
    PCALIGN $32
initSSE2:
    INCQ SI
    MOVQ DX, R11
    SUBQ SI, R11
    CMPQ R11, $4
    JB test
    ANDQ $-4, R11
    ADDQ SI, R11
    SHUFPS $0, X3, X3
    MOVAPS X3, X4
    PCALIGN $16
loopSSE2:
    MOVL (CX)(SI*4), DI
    MOVL 4(CX)(SI*4), R8
    MOVL 8(CX)(SI*4), R9
    MOVL 12(CX)(SI*4), R10
    CMPL DI, BX
    JAE indexOutOfBounds
    CMPL R8, BX
    JAE indexOutOfBounds
    CMPL R9, BX
    JAE indexOutOfBounds
    CMPL R10, BX
    JAE indexOutOfBounds
    MOVSS (AX)(DI*4), X1
    MOVSS (AX)(R8*4), X2
    UNPCKLPS X2, X1
    MOVSS (AX)(R9*4), X5
    MOVSS (AX)(R10*4), X6
    UNPCKLPS X6, X5
    MOVLHPS X5, X1
    MOVAPS X1, X2
    // Keep non-NaN accumulators in SRC2 so NaN lanes preserve X3 and X4.
    MINPS X3, X1
    MAXPS X4, X2
    MOVAPS X1, X3
    MOVAPS X2, X4
    ADDQ $4, SI
    CMPQ SI, R11
    JNE loopSSE2

    MOVHLPS X3, X1
    MOVHLPS X4, X2
    MINPS X1, X3
    MAXPS X2, X4
    PSHUFD $1, X3, X1
    PSHUFD $1, X4, X2
    MINSS X1, X3
    MAXSS X2, X4
    JMP test
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSS (AX)(DI*4), X1
    MOVAPS X1, X2
    MINSS X3, X1
    MAXSS X4, X2
    MOVAPS X1, X3
    MOVAPS X2, X4
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVSS X3, min+48(FP)
    MOVSS X4, max+52(FP)
    MOVQ R12, err+56(FP)
    RET
findNonNaN:
    INCQ SI
    CMPQ SI, DX
    JE return
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSS (AX)(DI*4), X1
    UCOMISS X1, X1
    JP findNonNaN
    MOVAPS X1, X3
    MOVAPS X1, X4
    CMPB ·hasAVX512VL(SB), $0
    JE initSSE2
    MOVQ DX, DI
    ANDQ $-8, DI
    CMPQ SI, DI
    JAE initSSE2
    ANDQ $-8, SI
    JMP initAVX512

// func dictionaryBoundsFloat64(dict []float64, indexes []int32) (min, max float64, err errno)
TEXT ·dictionaryBoundsFloat64(SB), NOSPLIT, $0-72
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    PXOR X3, X3   // min
    PXOR X4, X4   // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSD (AX)(DI*8), X3
    MOVAPD X3, X4

    UCOMISD X3, X3
    JP findNonNaN

    CMPQ DX, $8
    JB initSSE2

    CMPB ·hasAVX512VL(SB), $0
    JE initSSE2

    MOVQ DX, DI
    ANDQ $-8, DI
initAVX512:
    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2 // [len(dict)...]
    VPBROADCASTQ X3, Z3 // [min...]
    VMOVDQU64 Z3, Z4    // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDQ (AX)(Y0*8), K1, Z1
    // Keep non-NaN accumulators in SRC2 so NaN lanes preserve Z3 and Z4.
    VMINPD Z3, Z1, Z3
    VMAXPD Z4, Z1, Z4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERMQ $0b1110, Z3, Z0
    VPERMQ $0b1110, Z4, Z1
    VMINPD Z0, Z3, Z3
    VMAXPD Z1, Z4, Z4

    VPERMQ $1, Z3, Z0
    VPERMQ $1, Z4, Z1
    VMINPD Z0, Z3, Z3
    VMAXPD Z1, Z4, Z4

    VSHUFF64X2 $2, Z3, Z3, Z0
    VSHUFF64X2 $2, Z4, Z4, Z1
    VMINPD Z0, Z3, Z3
    VMAXPD Z1, Z4, Z4

    VZEROUPPER
    JMP test
    PCALIGN $32
initSSE2:
    INCQ SI
    MOVQ DX, R11
    SUBQ SI, R11
    CMPQ R11, $4
    JB test
    ANDQ $-4, R11
    ADDQ SI, R11
    UNPCKLPD X3, X3
    MOVAPD X3, X4
    MOVAPD X3, X5
    MOVAPD X3, X6
    PCALIGN $16
loopSSE2:
    MOVL (CX)(SI*4), DI
    MOVL 4(CX)(SI*4), R8
    MOVL 8(CX)(SI*4), R9
    MOVL 12(CX)(SI*4), R10
    CMPL DI, BX
    JAE indexOutOfBounds
    CMPL R8, BX
    JAE indexOutOfBounds
    CMPL R9, BX
    JAE indexOutOfBounds
    CMPL R10, BX
    JAE indexOutOfBounds
    MOVSD (AX)(DI*8), X1
    MOVHPD (AX)(R8*8), X1
    MOVSD (AX)(R9*8), X7
    MOVHPD (AX)(R10*8), X7
    MOVAPD X1, X2
    MOVAPD X7, X0
    // Keep non-NaN accumulators in SRC2 so NaN lanes preserve X3..X6.
    MINPD X3, X1
    MAXPD X4, X2
    MINPD X5, X7
    MAXPD X6, X0
    MOVAPD X1, X3
    MOVAPD X2, X4
    MOVAPD X7, X5
    MOVAPD X0, X6
    ADDQ $4, SI
    CMPQ SI, R11
    JNE loopSSE2

    MINPD X5, X3
    MAXPD X6, X4
    MOVHLPS X3, X1
    MOVHLPS X4, X2
    MINSD X1, X3
    MAXSD X2, X4
    JMP test
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSD (AX)(DI*8), X1
    MOVAPD X1, X2
    MINSD X3, X1
    MAXSD X4, X2
    MOVAPD X1, X3
    MOVAPD X2, X4
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVSD X3, min+48(FP)
    MOVSD X4, max+56(FP)
    MOVQ R12, err+64(FP)
    RET
findNonNaN:
    INCQ SI
    CMPQ SI, DX
    JE return
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVSD (AX)(DI*8), X1
    UCOMISD X1, X1
    JP findNonNaN
    MOVAPD X1, X3
    MOVAPD X1, X4
    CMPB ·hasAVX512VL(SB), $0
    JE initSSE2
    MOVQ DX, DI
    ANDQ $-8, DI
    CMPQ SI, DI
    JAE initSSE2
    ANDQ $-8, SI
    JMP initAVX512

// func dictionaryBoundsUint32(dict []uint32, indexes []int32) (min, max uint32, err errno)
TEXT ·dictionaryBoundsUint32(SB), NOSPLIT, $0-64
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    XORQ R10, R10 // min
    XORQ R11, R11 // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), R10
    MOVL R10, R11

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2  // [len(dict)...]
    VPBROADCASTD R10, Y3 // [min...]
    VMOVDQU32 Y3, Y4     // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDD (AX)(Y0*4), K1, Y1
    VPMINUD Y1, Y3, Y3
    VPMAXUD Y1, Y4, Y4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERM2I128 $1, Y3, Y3, Y0
    VPERM2I128 $1, Y4, Y4, Y1
    VPMINUD Y0, Y3, Y3
    VPMAXUD Y1, Y4, Y4

    VPSHUFD $0b1110, Y3, Y0
    VPSHUFD $0b1110, Y4, Y1
    VPMINUD Y0, Y3, Y3
    VPMAXUD Y1, Y4, Y4

    VPSHUFD $1, Y3, Y0
    VPSHUFD $1, Y4, Y1
    VPMINUD Y0, Y3, Y3
    VPMAXUD Y1, Y4, Y4

    MOVQ X3, R10
    MOVQ X4, R11
    ANDQ $0xFFFFFFFF, R10
    ANDQ $0xFFFFFFFF, R11

    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    CMPL DI, R10
    CMOVLCS DI, R10
    CMPL DI, R11
    CMOVLHI DI, R11
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVL R10, min+48(FP)
    MOVL R11, max+52(FP)
    MOVQ R12, err+56(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return

// func dictionaryBoundsUint64(dict []uint64, indexes []int32) (min, max uint64, err errno)
TEXT ·dictionaryBoundsUint64(SB), NOSPLIT, $0-72
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    XORQ R10, R10 // min
    XORQ R11, R11 // max
    XORQ R12, R12 // err
    XORQ SI, SI

    CMPQ DX, $0
    JE return

    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), R10
    MOVQ R10, R11

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ $0xFFFF, R8
    KMOVW R8, K1

    VPBROADCASTD BX, Y2  // [len(dict)...]
    VPBROADCASTQ R10, Z3 // [min...]
    VMOVDQU64 Z3, Z4     // [max...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y2, Y0, K2
    KMOVW K2, R9
    CMPB R9, $0xFF
    JNE indexOutOfBounds
    VPGATHERDQ (AX)(Y0*8), K1, Z1
    VPMINUQ Z1, Z3, Z3
    VPMAXUQ Z1, Z4, Z4
    KMOVW R8, K1
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512

    VPERMQ $0b1110, Z3, Z0
    VPERMQ $0b1110, Z4, Z1
    VPMINUQ Z0, Z3, Z3
    VPMAXUQ Z1, Z4, Z4

    VPERMQ $1, Z3, Z0
    VPERMQ $1, Z4, Z1
    VPMINUQ Z0, Z3, Z3
    VPMAXUQ Z1, Z4, Z4

    VSHUFF64X2 $2, Z3, Z3, Z0
    VSHUFF64X2 $2, Z4, Z4, Z1
    VPMINUQ Z0, Z3, Z3
    VPMAXUQ Z1, Z4, Z4

    MOVQ X3, R10
    MOVQ X4, R11

    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), DI
    CMPQ DI, R10
    CMOVQCS DI, R10
    CMPQ DI, R11
    CMOVQHI DI, R11
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVQ R10, min+48(FP)
    MOVQ R11, max+56(FP)
    MOVQ R12, err+64(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return

// func dictionaryBoundsBE128(dict [][16]byte, indexes []int32) (min, max *[16]byte, err errno)
TEXT ·dictionaryLookup32(SB), NOSPLIT, $0-80
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    MOVQ rows_array_ptr+48(FP), R8
    MOVQ rows_array_off+64(FP), R9

    XORQ SI, SI

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ R9, R10
    SHLQ $3, R10 // 8 * size

    MOVW $0xFFFF, R11
    KMOVW R11, K1
    KMOVW R11, K2

    VPBROADCASTD R9, Y2           // [size...]
    VPMULLD ·range0n8(SB), Y2, Y2 // [0*size,1*size,...]
    VPBROADCASTD BX, Y3           // [len(dict)...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y3, Y0, K3
    KMOVW K3, R11
    CMPB R11, $0xFF
    JNE indexOutOfBounds
    VPGATHERDD (AX)(Y0*4), K1, Y1
    VPSCATTERDD Y1, K2, (R8)(Y2*1)
    KMOVW R11, K1
    KMOVW R11, K2
    ADDQ R10, R8
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512
    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    MOVL DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
return:
    MOVQ AX, ret+72(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    JMP return

// func dictionaryLookup64(dict []uint64, indexes []int32, rows sparse.Array) errno
TEXT ·dictionaryLookup64(SB), NOSPLIT, $0-80
    MOVQ dict_base+0(FP), AX
    MOVQ dict_len+8(FP), BX

    MOVQ indexes_base+24(FP), CX
    MOVQ indexes_len+32(FP), DX

    MOVQ rows_array_ptr+48(FP), R8
    MOVQ rows_array_off+64(FP), R9

    XORQ SI, SI

    CMPQ DX, $8
    JB test

    CMPB ·hasAVX512VL(SB), $0
    JE test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ R9, R10
    SHLQ $3, R10 // 8 * size

    MOVW $0xFFFF, R11
    KMOVW R11, K1
    KMOVW R11, K2

    VPBROADCASTD R9, Y2           // [size...]
    VPMULLD ·range0n8(SB), Y2, Y2 // [0*size,1*size,...]
    VPBROADCASTD BX, Y3           // [len(dict)...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y3, Y0, K3
    KMOVW K3, R11
    CMPB R11, $0xFF
    JNE indexOutOfBounds
    VPGATHERDQ (AX)(Y0*8), K1, Z1
    VPSCATTERDQ Z1, K2, (R8)(Y2*1)
    KMOVW R11, K1
    KMOVW R11, K2
    ADDQ R10, R8
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512
    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), DI
    MOVQ DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
return:
    MOVQ AX, ret+72(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    JMP return
GLOBL ·range0n8(SB), RODATA|NOPTR, $40
DATA ·range0n8+0(SB)/4, $0
DATA ·range0n8+4(SB)/4, $1
DATA ·range0n8+8(SB)/4, $2
DATA ·range0n8+12(SB)/4, $3
DATA ·range0n8+16(SB)/4, $4
DATA ·range0n8+20(SB)/4, $5
DATA ·range0n8+24(SB)/4, $6
DATA ·range0n8+28(SB)/4, $7
DATA ·range0n8+32(SB)/4, $8
