//go:build !purego && goexperiment.simd && go1.27

#include "textflag.h"

// decodeByteArray stays on this kernel. The archsimd copy loop was slower
// on the prefix-heavy page the benchmark reconstructs.

// func decodeByteArrayAVX2(dst, src []byte, prefix, suffix []int32) int
TEXT ·decodeByteArrayAVX2(SB), NOSPLIT, $0-104
    MOVQ dst_base+0(FP), AX
    MOVQ src_base+24(FP), BX
    MOVQ prefix_base+48(FP), CX
    MOVQ suffix_base+72(FP), DX
    MOVQ suffix_len+80(FP), DI

    XORQ SI, SI
    XORQ R8, R8
    XORQ R9, R9
    MOVQ AX, R10 // last value

    JMP test
loop:
    MOVLQZX (CX)(SI*4), R8 // prefix length
    MOVLQZX (DX)(SI*4), R9 // suffix length
prefix:
    VMOVDQU (R10), Y0
    VMOVDQU Y0, (AX)
    CMPQ R8, $32
    JA copyPrefix
suffix:
    VMOVDQU (BX), Y1
    VMOVDQU Y1, (AX)(R8*1)
    CMPQ R9, $32
    JA copySuffix
next:
    MOVQ AX, R10
    ADDQ R9, R8
    LEAQ (AX)(R8*1), AX
    LEAQ (BX)(R9*1), BX
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ dst_base+0(FP), BX
    SUBQ BX, AX
    MOVQ AX, ret+96(FP)
    VZEROUPPER
    RET
copyPrefix:
    MOVQ $32, R12
copyPrefixLoop:
    VMOVDQU (R10)(R12*1), Y0
    VMOVDQU Y0, (AX)(R12*1)
    ADDQ $32, R12
    CMPQ R12, R8
    JB copyPrefixLoop
    JMP suffix
copySuffix:
    MOVQ $32, R12
    LEAQ (AX)(R8*1), R13
copySuffixLoop:
    VMOVDQU (BX)(R12*1), Y1
    VMOVDQU Y1, (R13)(R12*1)
    ADDQ $32, R12
    CMPQ R12, R9
    JB copySuffixLoop
    JMP next

